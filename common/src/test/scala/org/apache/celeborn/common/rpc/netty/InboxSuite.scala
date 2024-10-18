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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.rpc.{RpcAddress, TestRpcEndpoint}
import org.apache.celeborn.common.util.ThreadUtils

class InboxSuite extends CelebornFunSuite with BeforeAndAfter {

  private var inbox: Inbox = _
  private var endpoint: TestRpcEndpoint = _

  def initInbox[T](
      testRpcEndpoint: TestRpcEndpoint,
      onDropOverride: Option[InboxMessage => T]): Inbox = {
    val rpcEnvRef = mock(classOf[NettyRpcEndpointRef])
    if (onDropOverride.isEmpty) {
      new Inbox(rpcEnvRef, testRpcEndpoint, new CelebornConf())
    } else {
      new Inbox(rpcEnvRef, testRpcEndpoint, new CelebornConf()) {
        override protected def onDrop(message: InboxMessage): Unit = {
          onDropOverride.get(message)
        }
      }
    }
  }

  before {
    endpoint = new TestRpcEndpoint
    inbox = initInbox(endpoint, None)
  }

  test("post") {
    val dispatcher = mock(classOf[Dispatcher])

    val message = OneWayMessage(null, "hi")
    inbox.post(message)
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    endpoint.verifySingleReceiveMessage("hi")

    inbox.stop()
    inbox.process(dispatcher)
    assert(inbox.isEmpty)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
  }

  test("post: with reply") {
    val dispatcher = mock(classOf[Dispatcher])

    val message = RpcMessage(null, "hi", null)
    inbox.post(message)
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    endpoint.verifySingleReceiveAndReplyMessage("hi")
  }

  test("post: multiple threads") {
    val rpcEnvRef = mock(classOf[NettyRpcEndpointRef])
    val dispatcher = mock(classOf[Dispatcher])

    val numDroppedMessages = new AtomicInteger(0)

    val overrideOnDrop = (msg: InboxMessage) => {
      numDroppedMessages.incrementAndGet()
    }
    val inbox = initInbox(endpoint, Some(overrideOnDrop))

    val exitLatch = new CountDownLatch(10)

    for (_ <- 0 until 10) {
      ThreadUtils.newThreadWithDefaultUncaughtExceptionHandler(
        new Runnable {
          override def run(): Unit = {
            for (_ <- 0 until 100) {
              val message = OneWayMessage(null, "hi")
              inbox.post(message)
            }
            exitLatch.countDown()
          }
        },
        "inbox-test-thread").start()
    }
    // Try to process some messages
    inbox.process(dispatcher)
    inbox.stop()
    // After `stop` is called, further messages will be dropped. However, while `stop` is called,
    // some messages may be post to Inbox, so process them here.
    inbox.process(dispatcher)
    assert(inbox.isEmpty)

    exitLatch.await(30, TimeUnit.SECONDS)

    assert(1000 === endpoint.numReceiveMessages + numDroppedMessages.get)
    endpoint.verifyStarted()
    endpoint.verifyStopped()
  }

  test("post: Associated") {
    val dispatcher = mock(classOf[Dispatcher])
    val remoteAddress = RpcAddress("localhost", 11111)

    inbox.post(RemoteProcessConnected(remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnConnectedMessage(remoteAddress)
  }

  test("post: Disassociated") {
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)

    inbox.post(RemoteProcessDisconnected(remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnDisconnectedMessage(remoteAddress)
  }

  test("post: AssociationError") {
    val dispatcher = mock(classOf[Dispatcher])

    val remoteAddress = RpcAddress("localhost", 11111)
    val cause = new RuntimeException("Oops")

    inbox.post(RemoteProcessConnectionError(cause, remoteAddress))
    inbox.process(dispatcher)

    endpoint.verifySingleOnNetworkErrorMessage(cause, remoteAddress)
  }

  test("should reduce the number of active threads when fatal error happens") {
    val endpoint = mock(classOf[TestRpcEndpoint])
    when(endpoint.receive).thenThrow(new OutOfMemoryError())
    val dispatcher = mock(classOf[Dispatcher])
    val inbox = initInbox(endpoint, None)
    inbox.post(OneWayMessage(null, "hi"))
    intercept[OutOfMemoryError] {
      inbox.process(dispatcher)
    }
    assert(inbox.getNumActiveThreads === 0)
  }
}
