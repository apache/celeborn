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

package org.apache.celeborn.client

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, ApplicationLostResponse, HeartbeatFromApplication, HeartbeatFromApplicationResponse, ZERO_UUID}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

class ApplicationHeartbeater(
    appId: String,
    conf: CelebornConf,
    masterClient: MasterClient,
    shuffleMetrics: () => (Long, Long),
    workerStatusTracker: WorkerStatusTracker) extends Logging {

  private var stopped = false

  // Use independent app heartbeat threads to avoid being blocked by other operations.
  private val appHeartbeatIntervalMs = conf.appHeartbeatIntervalMs
  private val applicationUnregisterEnabled = conf.applicationUnregisterEnabled
  private val appHeartbeatHandlerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "celeborn-client-lifecycle-manager-app-heartbeater")
  private var appHeartbeat: ScheduledFuture[_] = _

  def start(): Unit = {
    appHeartbeat = appHeartbeatHandlerThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          try {
            require(masterClient != null, "When sending a heartbeat, client shouldn't be null.")
            val (tmpTotalWritten, tmpTotalFileCount) = shuffleMetrics()
            logInfo("Send app heartbeat with " +
              s"written: ${Utils.bytesToString(tmpTotalWritten)}, file count: $tmpTotalFileCount")
            val appHeartbeat =
              HeartbeatFromApplication(
                appId,
                tmpTotalWritten,
                tmpTotalFileCount,
                workerStatusTracker.getNeedCheckedWorkers().toList.asJava,
                ZERO_UUID,
                true)
            val response = requestHeartbeat(appHeartbeat)
            if (response.statusCode == StatusCode.SUCCESS) {
              logDebug("Successfully send app heartbeat.")
              workerStatusTracker.handleHeartbeatResponse(response)
            }
          } catch {
            case it: InterruptedException =>
              logWarning("Interrupted while sending app heartbeat.")
              Thread.currentThread().interrupt()
              throw it
            case t: Throwable =>
              logError("Error while send heartbeat", t)
          }
        }
      },
      0,
      appHeartbeatIntervalMs,
      TimeUnit.MILLISECONDS)
  }

  private def requestHeartbeat(message: HeartbeatFromApplication)
      : HeartbeatFromApplicationResponse = {
    try {
      masterClient.askSync[HeartbeatFromApplicationResponse](
        message,
        classOf[HeartbeatFromApplicationResponse])
    } catch {
      case e: Exception =>
        logError("AskSync HeartbeatFromApplication failed.", e)
        HeartbeatFromApplicationResponse(
          StatusCode.REQUEST_FAILED,
          List.empty.asJava,
          List.empty.asJava,
          List.empty.asJava)
    }
  }

  private def unregisterApplication(): Unit = {
    try {
      // Then unregister Application
      val response = masterClient.askSync[ApplicationLostResponse](
        ApplicationLost(appId),
        classOf[ApplicationLostResponse])
      logInfo(s"Unregister Application $appId with response status: ${response.status}")
    } catch {
      case e: Exception =>
        logWarning("AskSync unRegisterApplication failed.", e)
    }
  }

  def stop(): Unit = {
    stopped.synchronized {
      if (!stopped) {
        // Stop appHeartbeat first
        logInfo(s"Stop Application heartbeat $appId")
        appHeartbeat.cancel(true)
        ThreadUtils.shutdown(appHeartbeatHandlerThread, 800.millis)
        if (applicationUnregisterEnabled) {
          unregisterApplication()
        }
        stopped = true
      }
    }
  }
}
