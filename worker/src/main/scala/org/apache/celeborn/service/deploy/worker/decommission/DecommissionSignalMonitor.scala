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

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

/**
 * Polls a [[DecommissionSignalChecker]] on a dedicated daemon thread and calls
 * `triggerDecommission` at most once, the first time the checker signals while the worker is not
 * already exiting.
 */
class DecommissionSignalMonitor(
    checker: DecommissionSignalChecker,
    intervalMs: Long,
    isWorkerExiting: () => Boolean,
    triggerDecommission: () => Unit) extends Logging {

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
        override def run(): Unit = checkOnce()
      },
      intervalMs,
      intervalMs,
      TimeUnit.MILLISECONDS)
    logInfo(s"Started decommission signal checker $checkerName with interval ${intervalMs}ms")
  }

  private[worker] def checkOnce(): Unit = Utils.tryLogNonFatalError {
    if (!triggered.get && !closed.get) {
      if (isWorkerExiting()) {
        logDebug("Worker is already exiting, skip polling the decommission signal checker.")
      } else if (pollChecker() && triggered.compareAndSet(false, true)) {
        val reason = Try(checker.getReason).toOption.flatMap(Option(_)).getOrElse("unspecified")
        logInfo(
          s"Self-decommission triggered by decommission signal checker $checkerName, " +
            s"reason: $reason")
        try {
          triggerDecommission()
        } finally {
          if (task != null) {
            task.cancel(false)
          }
        }
      }
    }
  }

  private def pollChecker(): Boolean = {
    try {
      checker.shouldDecommission()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Decommission signal checker $checkerName failed; treating as no signal", e)
        false
    }
  }

  def stop(): Unit = {
    if (closed.compareAndSet(false, true)) {
      synchronized {
        if (task != null) {
          task.cancel(false)
        }
        if (scheduler != null) {
          scheduler.shutdownNow()
        }
      }
      Utils.tryLogNonFatalError(checker.close())
    }
  }
}

object DecommissionSignalMonitor {

  /**
   * Instantiates and initialises the checker named by
   * `celeborn.worker.decommission.signal.checker.class`. Any failure is rethrown as a
   * [[CelebornException]] naming the config key and the class, so the worker fails fast.
   */
  def createChecker(conf: CelebornConf): DecommissionSignalChecker = {
    val key = WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS.key
    val className = conf.workerDecommissionSignalCheckerClass
    try {
      val clazz = Utils.classForName(className)
      require(
        classOf[DecommissionSignalChecker].isAssignableFrom(clazz),
        s"$className does not implement ${classOf[DecommissionSignalChecker].getName}")
      val checker =
        clazz.getDeclaredConstructor().newInstance().asInstanceOf[DecommissionSignalChecker]
      checker.init(conf)
      checker
    } catch {
      case NonFatal(e) =>
        throw new CelebornException(s"Failed to instantiate $key = $className: ${e.getMessage}", e)
    }
  }
}
