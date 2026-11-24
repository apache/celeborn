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

package org.apache.celeborn.service.deploy.worker.decommission

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_CHECKER
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.service.deploy.worker.decommission.DecommissionSignalMonitor.{isRecoverable, STOP_TIMEOUT_MS}

/**
 * Polls a [[DecommissionSignalChecker]] on a dedicated daemon thread and calls
 * `triggerDecommission` at most once, the first time the checker signals while the worker is not
 * already exiting.
 */
class DecommissionSignalMonitor(
    private[worker] val checker: DecommissionSignalChecker,
    intervalMs: Long,
    isWorkerExiting: () => Boolean,
    triggerDecommission: () => Unit,
    stopTimeoutMs: Long = STOP_TIMEOUT_MS) extends Logging {

  private val triggered = new AtomicBoolean(false)
  private val closed = new AtomicBoolean(false)
  @volatile private var scheduler: ScheduledExecutorService = _
  @volatile private var task: ScheduledFuture[_] = _

  private val checkerName = checker.getClass.getName

  private[worker] def isTriggered: Boolean = triggered.get

  def start(): Unit = synchronized {
    if (checker.isInstanceOf[NoOpDecommissionSignalChecker]) {
      logDebug("No decommission signal checker configured.")
      return
    }
    if (closed.get || scheduler != null) {
      return
    }
    scheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-decommission-signal-checker")
    task = scheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          try {
            checkOnce()
          } catch {
            case _: InterruptedException =>
              // Expected when stop() interrupts an in-flight poll via shutdownNow().
              logDebug("Decommission signal polling interrupted")
              Thread.currentThread().interrupt()
            case t: Throwable =>
              if (closed.get) {
                logDebug("Decommission signal polling stopped during shutdown", t)
              } else {
                logError("Decommission signal polling stopped due to a fatal error", t)
              }
              throw t
          }
        }
      },
      intervalMs,
      intervalMs,
      TimeUnit.MILLISECONDS)
    logInfo(s"Started decommission signal checker $checkerName with interval ${intervalMs}ms")
  }

  private[worker] def checkOnce(): Unit = {
    try {
      if (!triggered.get && !closed.get) {
        if (isWorkerExiting()) {
          logDebug("Worker is already exiting, skip polling the decommission signal checker.")
        } else if (pollChecker() && !closed.get && triggered.compareAndSet(false, true)) {
          try {
            logInfo(s"Self-decommission triggered by decommission signal checker $checkerName")
            triggerDecommission()
          } catch {
            case e: Throwable if isRecoverable(e) =>
              logError("Self-decommission trigger failed and will not be retried", e)
          } finally {
            if (task != null) {
              task.cancel(false)
            }
          }
          // Only after the trigger, so a slow getReason() can't hold back the decommission.
          logInfo(s"Self-decommission reason from $checkerName: ${reason()}")
        }
      }
    } catch {
      case e: Throwable if isRecoverable(e) =>
        logError(s"Unexpected error while checking decommission signal from $checkerName", e)
    }
  }

  private def pollChecker(): Boolean = {
    try {
      checker.shouldDecommission()
    } catch {
      case e: Throwable if isRecoverable(e) =>
        if (closed.get) {
          logDebug(s"Decommission signal checker $checkerName failed during shutdown", e)
        } else {
          logWarning(s"Decommission signal checker $checkerName failed; treating as no signal", e)
        }
        false
    }
  }

  private def reason(): String = {
    try {
      Option(checker.getReason).getOrElse("unspecified")
    } catch {
      case e: Throwable if isRecoverable(e) =>
        logWarning(s"Decommission signal checker $checkerName failed to report a reason", e)
        "unspecified"
    }
  }

  /**
   * Stops polling and closes the checker. Waits up to `stopTimeoutMs` for an in-flight check to
   * return, then calls close() once and waits up to `stopTimeoutMs` for it. A stuck checker never
   * blocks the worker's shutdown for longer than that.
   */
  def stop(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val executor = synchronized {
        if (task != null) {
          task.cancel(false)
        }
        if (scheduler != null) {
          scheduler.shutdownNow()
        }
        scheduler
      }
      if (executor != null) {
        try {
          if (!executor.awaitTermination(stopTimeoutMs, TimeUnit.MILLISECONDS)) {
            logWarning(s"Decommission signal checker $checkerName did not finish within " +
              s"${stopTimeoutMs}ms, closing it anyway")
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
        }
      }
      closeChecker()
    }
  }

  private def closeChecker(): Unit = {
    val closer = ThreadUtils.newDaemonThread(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError(checker.close())
      },
      "worker-decommission-signal-checker-close")
    closer.start()
    try {
      closer.join(stopTimeoutMs)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
    if (closer.isAlive) {
      logWarning(s"Decommission signal checker $checkerName did not close within " +
        s"${stopTimeoutMs}ms, continuing shutdown")
    }
  }
}

object DecommissionSignalMonitor {

  private[decommission] val STOP_TIMEOUT_MS = 5000L

  // Plugins commonly fail with LinkageError (e.g. a missing dependency jar), which NonFatal
  // does not match. Treat it as recoverable so it is logged or reported with the config key.
  private def isRecoverable(e: Throwable): Boolean = NonFatal(e) || e.isInstanceOf[LinkageError]

  /**
   * Instantiates the checker named by `celeborn.worker.decommission.signal.checker` through its
   * public `CelebornConf` constructor. Any failure is rethrown as a [[CelebornException]] naming
   * the config key and the class, so the worker fails fast.
   */
  def createChecker(conf: CelebornConf): DecommissionSignalChecker = {
    val key = WORKER_DECOMMISSION_SIGNAL_CHECKER.key
    val className = conf.workerDecommissionSignalChecker
    try {
      require(
        classOf[DecommissionSignalChecker].isAssignableFrom(Utils.classForName(className)),
        s"$className does not implement ${classOf[DecommissionSignalChecker].getName}")
      Utils.instantiateClassWithCelebornConf[DecommissionSignalChecker](className, conf)
    } catch {
      case e: Throwable if isRecoverable(e) =>
        // Unwrap the generic error from instantiateClassWithCelebornConf to report the real cause.
        val cause = e match {
          case ce: CelebornException if ce.getCause != null => ce.getCause
          case _ => e
        }
        throw new CelebornException(
          s"Failed to instantiate $key = $className: ${cause.getMessage}",
          cause)
    }
  }
}
