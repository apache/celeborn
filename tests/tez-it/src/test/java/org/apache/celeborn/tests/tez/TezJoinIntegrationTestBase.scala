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

package org.apache.celeborn.tests.tez

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.tez.dag.api.TezConfiguration
import org.apache.tez.examples.JoinDataGen
import org.apache.tez.examples.JoinValidate
import org.junit.jupiter.api.Assertions.assertEquals

object TezJoinIntegrationTestBase {
  val STREAM_INPUT_PATH = "stream_input"
  val STREAM_INPUT_FILE_SIZE = "5000000"
  val HASH_INPUT_PATH = "hash_input"
  val HASH_INPUT_FILE_SIZE = "500000"
  val JOIN_EXPECTED_PATH = "join_expected"
  val NUM_TASKS = "2"
}
class TezJoinIntegrationTestBase extends TezIntegrationTestBase {
  @throws[Exception]
  protected def generateInputFile(): Unit = {
    fs.delete(new Path(TezJoinIntegrationTestBase.STREAM_INPUT_PATH), true)
    fs.delete(new Path(TezJoinIntegrationTestBase.HASH_INPUT_PATH), true)
    fs.delete(new Path(TezJoinIntegrationTestBase.JOIN_EXPECTED_PATH), true)
    val args = Array(
      TezJoinIntegrationTestBase.STREAM_INPUT_PATH,
      TezJoinIntegrationTestBase.STREAM_INPUT_FILE_SIZE,
      TezJoinIntegrationTestBase.HASH_INPUT_PATH,
      TezJoinIntegrationTestBase.HASH_INPUT_FILE_SIZE,
      TezJoinIntegrationTestBase.JOIN_EXPECTED_PATH,
      TezJoinIntegrationTestBase.NUM_TASKS)
    val dataGen = new JoinDataGen
    val appConf = new TezConfiguration(miniTezCluster.getConfig)
    updateCommonConfiguration(appConf)
    assertEquals(0, ToolRunner.run(appConf, dataGen, args), "JoinDataGen failed")
  }

  @throws[Exception]
  def run(path: String): Unit = {
    val appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateRssConfiguration(appConf)
    appendAndUploadRssJars(appConf)
    runTezApp(appConf, getTestTool, getTestArgs(path))
    verifyResults(TezJoinIntegrationTestBase.JOIN_EXPECTED_PATH, getOutputDir(path))
  }


  @throws[Exception]
  override def verifyResults(expectedPath: String, rssPath: String): Unit = {
    val args = Array(expectedPath, rssPath)
    val validate = new JoinValidate
    val appConf = new TezConfiguration(miniTezCluster.getConfig)
    updateCommonConfiguration(appConf)
    assertEquals(0, ToolRunner.run(appConf, validate, args), "JoinValidate failed")
  }
}
