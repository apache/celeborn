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

package org.apache.celeborn.tests.flink

import java.io.File

import scala.collection.JavaConverters._

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, ExecutionOptions}
import org.apache.flink.runtime.jobgraph.JobType
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{AUTH_ENABLED, INTERNAL_PORT_ENABLED}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.FallbackPolicy
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

abstract class WordCountTestBase extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  var workers: collection.Set[Worker] = null
  var port = 0

  protected def getMasterConf: Map[String, String]
  protected def getWorkerConf: Map[String, String]
  protected def getWorkerNum: Int = 3

  protected def getClientConf: Map[String, String] = Map()

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val (m, w) = setupMiniClusterWithRandomPorts(getMasterConf, getWorkerConf, getWorkerNum)
    workers = w
    port = m.conf.get(CelebornConf.MASTER_PORT)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  private def addClientConf(configuration: Configuration): Unit = {
    for ((k, v) <- getClientConf) {
      configuration.setString(k, v)
    }
  }

  test(getClass.getName + ": celeborn flink integration test - word count") {
    testWordCount(FallbackPolicy.AUTO)
  }

  test(getClass.getName + ": celeborn flink integration test with fallback - word count") {
    testWordCount(FallbackPolicy.ALWAYS)
  }

  private def testWordCount(fallbackPolicy: FallbackPolicy): Unit = {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = 8
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory")
    configuration.setString("celeborn.master.endpoints", s"localhost:$port")
    configuration.setString("celeborn.client.flink.shuffle.fallback.policy", fallbackPolicy.name())
    configuration.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_BLOCKING")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString("rest.bind-port", "8081-8099")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    addClientConf(configuration)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.getConfig.setParallelism(parallelism)
    env.disableOperatorChaining()
    // make parameters available in the web interface
    WordCountHelper.execute(env, parallelism)

    val graph = env.getStreamGraph
    graph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING)
    graph.setJobType(JobType.BATCH)
    env.execute(graph)
    checkFlushingFileLength()
  }

  private def checkFlushingFileLength(): Unit = {
    workers.map(worker => {
      worker.storageManager.workingDirWriters.values().asScala.map(writers => {
        writers.forEach((fileName, fileWriter) => {
          assert(new File(fileName).length() == fileWriter.getDiskFileInfo.getFileLength)
        })
      })
    })
  }
}

class WordCountTest extends WordCountTestBase {
  override protected def getMasterConf: Map[String, String] = Map()
  override protected def getWorkerConf: Map[String, String] = Map()
}

class WordCountTestWithAuthentication extends WordCountTestBase {

  private val authConfig = Map(
    AUTH_ENABLED.key -> "true",
    INTERNAL_PORT_ENABLED.key -> "true")

  override protected def getMasterConf: Map[String, String] = authConfig
  override protected def getWorkerConf: Map[String, String] = authConfig
  override protected def getClientConf: Map[String, String] = Map(AUTH_ENABLED.key -> "true")
}
