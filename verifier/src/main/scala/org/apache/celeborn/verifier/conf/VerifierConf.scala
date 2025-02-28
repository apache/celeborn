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

package org.apache.celeborn.verifier.conf

import java.io.File
import java.util

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.rpc.RpcAddress
import org.apache.celeborn.common.util.Utils

class VerifierConf {

  val celebornConf = new CelebornConf()
  private val settings = new util.HashMap[String, String]()

  def get(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def set(key: String, value: String): Unit = {
    settings.put(key, value)
  }
}

object VerifierConf {

  private val defaultScriptsLocation: String =
    sys.env.get("CELEBORN_HOME").map { t => s"$t${File.separator}sbin" }
      .map { t => new File(s"$t") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull

  def schedulerAddress(conf: VerifierConf): RpcAddress = {
    val parts = conf.get(
      "verf.scheduler.address",
      s"${Utils.localHostName(conf.celebornConf)}:19097").split(":")
    RpcAddress(parts(0), parts(1).toInt)
  }

  def runnerTestMode(conf: VerifierConf): Boolean = {
    conf.get("verf.runner.test.mode", "false").toBoolean
  }

  def runnerTimeOutMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.timeout", "120s"))
  }

  def runnerRegisterRetryDelayMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.register.retry.delay", "5s"))
  }

  def runnerHeartBeatDelayMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.heartbeat.delay", "30s"))
  }

  def runnerHeartBeatIntervalMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.heartbeat.interval", "30s"))
  }

  def planActionBadInflightFile(conf: VerifierConf): String = {
    conf.get("verf.plan.action.block.bad.inflight.location", s"/root/badblock/inflight")
  }

  def planActionDefaultInterval(conf: VerifierConf): String = {
    conf.get("verf.plan.action.default.interval", "5s")
  }

  def planActionOccupyCpuMaxDurationMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.plan.action.occupycpu.maxduration", "120s"))
  }

  def planActionSelectorDefaultInterval(conf: VerifierConf): String = {
    conf.get("verf.plan.action.selector.default.interval", "5s")
  }

  def startMasterScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.master.start.script",
      s"$defaultScriptsLocation${File.separator}start-master.sh")
  }

  def stopMasterScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.master.stop.script",
      s"$defaultScriptsLocation${File.separator}stop-master.sh")
  }

  def startWorkerScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.worker.start.script",
      s"$defaultScriptsLocation${File.separator}start-worker.sh")
  }

  def stopWorkerScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.worker.stop.script",
      s"$defaultScriptsLocation${File.separator}stop-worker.sh")
  }
}
