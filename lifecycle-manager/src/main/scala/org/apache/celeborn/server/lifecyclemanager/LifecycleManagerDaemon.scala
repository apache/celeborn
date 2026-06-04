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

package org.apache.celeborn.server.lifecyclemanager

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{SignalUtils, Utils}

object LifecycleManagerDaemon extends Logging {

  private[lifecyclemanager] val shutdownLatch: CountDownLatch = new CountDownLatch(1)

  private[lifecyclemanager] val currentInstance: AtomicReference[LifecycleManager] =
    new AtomicReference[LifecycleManager]()

  private[lifecyclemanager] var exitFn: Int => Unit =
    (code: Int) => System.exit(code)

  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)

    val parsedArgs = LifecycleManagerDaemonArguments.parseOrExit(args)
    val conf = new CelebornConf()

    // Load properties file before applying CLI args
    Utils.loadDefaultCelebornProperties(conf, parsedArgs.propertiesFile.orNull)

    applyArgsToConf(parsedArgs, conf)

    // Auth check: standalone LM does not support auth (cpp/Rust client lacks SASL).
    //
    // DEPLOYMENT WARNING: with auth disabled, this daemon exposes shuffle
    // registration and slot allocation to anything that can reach its RPC port.
    // It performs no authentication of incoming clients. Operators MUST bind it
    // to a trusted network only (e.g. set --host to a private interface and
    // restrict the RPC port via firewall / security groups / network policy);
    // never expose the port on an untrusted or public network.
    if (conf.authEnabledOnClient) {
      logError(
        "Standalone LifecycleManager does not support auth " +
          "(cpp/Rust client lacks SASL); set celeborn.auth.enabled=false")
      exitFn(1)
      return
    }

    logWarning(
      "Standalone LifecycleManager runs WITHOUT authentication. Ensure its RPC " +
        "port is reachable only from trusted networks (bind to a private " +
        "interface and restrict access via firewall / network policy).")

    // Propagate --host to Utils so LifecycleManager binds to the requested hostname
    parsedArgs.host.foreach { host =>
      logInfo(s"Setting custom hostname from --host: $host")
      Utils.setCustomHostname(host)
    }

    logInfo(s"Parsed args: appId=${parsedArgs.appId}, port=${parsedArgs.port}, " +
      s"masterEndpoints=${parsedArgs.masterEndpoints}")

    try {
      val lm = new LifecycleManager(parsedArgs.appId, conf)
      currentInstance.set(lm)

      installShutdownHook(conf)

      // scalastyle:off println
      println(s"LifecycleManager bound at ${lm.getHost}:${lm.getPort}")
      // scalastyle:on println

      logInfo("shutdown hook installed; press Ctrl-C to stop.")

      // Block until the shutdown hook fires (Ctrl-C / SIGTERM) and counts the
      // latch down. The hook is what drives the actual JVM exit, so there is no
      // need for an explicit exitFn(0) here — calling System.exit again from the
      // main thread while a shutdown hook is already running is redundant.
      shutdownLatch.await()
    } catch {
      case e: Exception =>
        logError("Failed to start LifecycleManager", e)
        exitFn(1)
    }
  }

  private[lifecyclemanager] def applyArgsToConf(
      args: LifecycleManagerDaemonArguments,
      conf: CelebornConf): Unit = {
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, args.masterEndpoints)
    conf.set(CelebornConf.CLIENT_SHUFFLE_MANAGER_PORT.key, args.port.toString)
  }

  private def installShutdownHook(conf: CelebornConf): Unit = {
    val shutdownTimeoutMs = conf.appHeartbeatTimeoutMs / 2

    // Watchdog: force halt if shutdown takes too long
    val watchdog = new Thread("celeborn-lm-shutdown-watchdog") {
      override def run(): Unit = {
        try {
          Thread.sleep(shutdownTimeoutMs)
        } catch {
          // Nothing interrupts this watchdog today, but if some future caller
          // ever does, treat the interruption itself as a sign that shutdown is
          // wedged and force a halt rather than silently exiting.
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
        }
        logError(s"Shutdown exceeded ${shutdownTimeoutMs}ms, forcing halt")
        Runtime.getRuntime.halt(2)
      }
    }
    watchdog.setDaemon(true)

    Runtime.getRuntime.addShutdownHook(new Thread("celeborn-lm-shutdown") {
      override def run(): Unit = {
        watchdog.start()
        val lm = currentInstance.get()
        if (lm != null) {
          try {
            lm.stop()
          } catch {
            case t: Throwable => logError("lm.stop() failed", t)
          }
        }
        shutdownLatch.countDown()
      }
    })
  }
}
