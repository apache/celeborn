/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.common.rpc.netty

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEndpoint, RpcMetricsTracker, ThreadSafeRpcEndpoint}

sealed private[celeborn] trait InboxMessage extends RpcMetricUtil

private[celeborn] trait RpcMetricUtil {
  var enqueueTime: Long = 0
  var dequeueTime: Long = 0
  var endProcessTime: Long = 0
}

private[celeborn] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any)
  extends InboxMessage

private[celeborn] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext)
  extends InboxMessage

private[celeborn] case object OnStart
  extends InboxMessage

private[celeborn] case object OnStop
  extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[celeborn] case class RemoteProcessConnected(remoteAddress: RpcAddress)
  extends InboxMessage with RpcMetricUtil

/** A message to tell all endpoints that a remote process has disconnected. */
private[celeborn] case class RemoteProcessDisconnected(
    remoteAddress: RpcAddress)
  extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[celeborn] case class RemoteProcessConnectionError(
    cause: Throwable,
    remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 */
private[celeborn] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint,
    val conf: CelebornConf,
    val metrics: RpcMetricsTracker) extends Logging {

  inbox => // Give this an alias so we can use it more clearly in closures.

  private[netty] val capacity = conf.get(CelebornConf.RPC_INBOX_CAPACITY)

  private[netty] val inboxLock = new ReentrantLock()
  private[netty] val isFull = inboxLock.newCondition()

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  private val messageCount = new AtomicLong(0)

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  try {
    inboxLock.lockInterruptibly()
    messages.add(OnStart)
    RpcMetricUtil.updateTime(OnStart, RpcMetricUtil.Enqueue)
    metrics.incQueueLength()
    messageCount.incrementAndGet()
  } finally {
    inboxLock.unlock()
  }

  def addMessage(message: InboxMessage): Unit = {
    messages.add(message)
    messageCount.incrementAndGet()
    signalNotFull()
    logDebug(s"queue length of ${messageCount.get()} ")
  }

  private def processInternal(dispatcher: Dispatcher, message: InboxMessage): Unit = {
    message match {
      case RpcMessage(_sender, content, context) =>
        try {
          endpoint.receiveAndReply(context).applyOrElse[Any, Unit](
            content,
            { msg =>
              throw new CelebornException(s"Unsupported message $message from ${_sender}")
            })
        } catch {
          case e: Throwable =>
            context.sendFailure(e)
            // Throw the exception -- this exception will be caught by the safelyCall function.
            // The endpoint's onError function will be called.
            throw e
        }

      case OneWayMessage(_sender, content) =>
        endpoint.receive.applyOrElse[Any, Unit](
          content,
          { msg =>
            throw new CelebornException(s"Unsupported message $message from ${_sender}")
          })

      case OnStart =>
        endpoint.onStart()
        if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
          try {
            inboxLock.lockInterruptibly()
            if (!stopped) {
              enableConcurrent = true
            }
          } finally {
            inboxLock.unlock()
          }
        }

      case OnStop =>
        val activeThreads =
          try {
            inboxLock.lockInterruptibly()
            inbox.numActiveThreads
          } finally {
            inboxLock.unlock()
          }
        assert(
          activeThreads == 1,
          s"There should be only a single active thread but found $activeThreads threads.")
        dispatcher.removeRpcEndpointRef(endpoint)
        endpoint.onStop()
        assert(isEmpty, "OnStop should be the last message")

      case RemoteProcessConnected(remoteAddress) =>
        endpoint.onConnected(remoteAddress)

      case RemoteProcessDisconnected(remoteAddress) =>
        endpoint.onDisconnected(remoteAddress)

      case RemoteProcessConnectionError(cause, remoteAddress) =>
        endpoint.onNetworkError(cause, remoteAddress)
    }
  }

  private[netty] def waitOnFull(): Unit = {
    if (capacity > 0 && !stopped) {
      try {
        inboxLock.lockInterruptibly()
        while (messageCount.get() >= capacity) {
          isFull.await()
        }
      } finally {
        inboxLock.unlock()
      }
    }
  }

  private def signalNotFull(): Unit = {
    // when this is called we assume putLock already being called
    require(inboxLock.isHeldByCurrentThread, "cannot call signalNotFull without holding lock")
    if (capacity > 0 && messageCount.get() < capacity) {
      isFull.signal()
    }
  }

  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    try {
      inboxLock.lockInterruptibly()
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
        RpcMetricUtil.updateTime(message, RpcMetricUtil.Dequeue)
        metrics.decQueueLength()
        messageCount.decrementAndGet()
        signalNotFull()
      } else {
        return
      }
    } finally {
      inboxLock.unlock()
    }

    while (true) {
      safelyCall(endpoint, endpointRef.name, message) {
        processInternal(dispatcher, message)
      }
      try {
        inboxLock.lockInterruptibly()
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        } else {
          RpcMetricUtil.updateTime(message, RpcMetricUtil.Dequeue)
          metrics.decQueueLength()
          messageCount.decrementAndGet()
          signalNotFull()
        }
      } finally {
        inboxLock.unlock()
      }
    }
  }

  def post(message: InboxMessage): Unit = {
    try {
      inboxLock.lockInterruptibly()
      if (stopped) {
        // We already put "OnStop" into "messages", so we should drop further messages
        onDrop(message)
      } else {
        addMessage(message)
        RpcMetricUtil.updateTime(message, RpcMetricUtil.Enqueue)
        metrics.incQueueLength()
      }
    } finally {
      inboxLock.unlock()
    }
  }

  def stop(): Unit = {
    try {
      inboxLock.lockInterruptibly()
      // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
      // message
      if (!stopped) {
        // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
        // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
        // safely.
        enableConcurrent = false
        stopped = true
        addMessage(OnStop)
        metrics.dump()
        // Note: The concurrent events in messages will be processed one by one.
      }
    } finally {
      inboxLock.unlock()
    }
  }

  def isEmpty: Boolean = {
    try {
      inboxLock.lockInterruptibly()
      messages.isEmpty
    } finally {
      inboxLock.unlock()
    }
  }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   */
  private def safelyCall(
      endpoint: RpcEndpoint,
      endpointRefName: String,
      message: InboxMessage)(action: => Unit): Unit = {
    def dealWithFatalError(fatal: Throwable): Unit = {
      try {
        inboxLock.lockInterruptibly()
        assert(numActiveThreads > 0, "The number of active threads should be positive.")
        // Should reduce the number of active threads before throw the error.
        numActiveThreads -= 1
      } finally {
        inboxLock.unlock()
      }
      logError(
        s"An error happened while processing message in the inbox for $endpointRefName",
        fatal)
      throw fatal
    }

    try action
    catch {
      case NonFatal(e) =>
        try endpoint.onError(e)
        catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
          case fatal: Throwable =>
            dealWithFatalError(fatal)
        }
      case fatal: Throwable =>
        dealWithFatalError(fatal)
    } finally {
      RpcMetricUtil.updateTime(message, RpcMetricUtil.Process)
      metrics.update(message)
    }
  }

  // exposed only for testing
  def getNumActiveThreads: Int = {
    try {
      inboxLock.lockInterruptibly()
      inbox.numActiveThreads
    } finally {
      inboxLock.unlock()
    }
  }
}

private[celeborn] object RpcMetricUtil {
  trait MetricsType
  case object Enqueue extends MetricsType
  case object Dequeue extends MetricsType
  case object Process extends MetricsType
  def updateTime(message: InboxMessage, op: MetricsType): Unit = {
    (message, op) match {
      case (msg: RpcMetricUtil, Enqueue) => msg.enqueueTime = System.nanoTime
      case (msg: RpcMetricUtil, Dequeue) => msg.dequeueTime = System.nanoTime
      case (msg: RpcMetricUtil, Process) => msg.endProcessTime = System.nanoTime
      case _ =>
    }
  }
}
