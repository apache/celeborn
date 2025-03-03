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

package org.apache.celeborn.verifier.scheduler

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.info.RunnerInfo

class SchedulerContext(
    val conf: VerifierConf,
    val runnerInfos: ConcurrentHashMap[String, RunnerInfo]) {

  def duplicate(): SchedulerContext = {
    val runners = new ConcurrentHashMap[String, RunnerInfo]()
    this.runnerInfos.asScala.foreach(runnerInfo =>
      runners.put(runnerInfo._1, runnerInfo._2.duplicate()))
    new SchedulerContext(conf, runners)
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder()
    runnerInfos.asScala.foreach(runnerInfo => {
      stringBuilder.append(s"Runner: ${runnerInfo._1} info: ${runnerInfo._2.toString}.")
    })
    stringBuilder.toString()
  }
}
