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

package org.apache.celeborn.service.deploy.lifecyclemanager

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{SignalUtils, Utils}

private[deploy] object LifecycleManagerDaemon extends Logging {

  private[lifecyclemanager] val shutdownLatch: CountDownLatch = new CountDownLatch(1)

  private[lifecyclemanager] val currentInstance: AtomicReference[LifecycleManager] =
    new AtomicReference[LifecycleManager]()

  private[lifecyclemanager] var exitFn: Int => Unit = (code: Int) => System.exit(code)

  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)
    val daemonArgs = LifecycleManagerDaemonArguments.parse(args)
    val conf = new CelebornConf()

    Utils.loadDefaultCelebornProperties(conf, daemonArgs.propertiesFile.orNull)
    applyArgsToConf(daemonArgs, conf)

    if (conf.authEnabledOnClient) {
      logError(
        "Standalone LifecycleManager does not support auth " +
          "(cpp/Rust client lacks SASL); set celeborn.auth.enabled=false")
      exitFn(1)
      return
    }

    if (daemonArgs.port < 1024) {
      logError(s"Port must be >= 1024, got ${daemonArgs.port}")
      exitFn(1)
      return
    }

    try {
      val lifecycleManager = new LifecycleManager(daemonArgs.appId, conf)
      currentInstance.set(lifecycleManager)

      val shutdownHookThread = new Thread("LifecycleManagerDaemon-ShutdownHook") {
        override def run(): Unit = {
          val watchdogTimeoutMs = conf.appHeartbeatTimeoutMs / 2
          val watchdog = new Thread("LifecycleManagerDaemon-Watchdog") {
            setDaemon(true)
            override def run(): Unit = {
              try {
                Thread.sleep(watchdogTimeoutMs)
                Runtime.getRuntime.halt(2)
              } catch {
                case _: InterruptedException => // normal exit
              }
            }
          }
          watchdog.start()

          try {
            val instance = currentInstance.get()
            if (instance != null) {
              instance.stop()
            }
          } catch {
            case e: Exception =>
              logError("Error stopping LifecycleManager during shutdown", e)
          } finally {
            watchdog.interrupt()
            shutdownLatch.countDown()
          }
        }
      }
      Runtime.getRuntime.addShutdownHook(shutdownHookThread)

      // scalastyle:off println
      println(s"LifecycleManager bound at ${lifecycleManager.getHost}:${lifecycleManager.getPort}")
      // scalastyle:on println

      shutdownLatch.await()
      exitFn(0)
    } catch {
      case e: Throwable =>
        logError("Initialize LifecycleManager failed.", e)
        e.printStackTrace()
        exitFn(1)
    }
  }

  private[lifecyclemanager] def runUntilStopped(lifecycleManager: LifecycleManager): Unit = {
    currentInstance.set(lifecycleManager)
    shutdownLatch.await()
  }

  private[lifecyclemanager] def applyArgsToConf(
      args: LifecycleManagerDaemonArguments,
      conf: CelebornConf): Unit = {
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, args.masterEndpoints)
    conf.set(CelebornConf.CLIENT_SHUFFLE_MANAGER_PORT.key, args.port.toString)
  }
}
