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
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class HybridShuffleWordCountTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  var workers: collection.Set[Worker] = null

  val NUM_PARALLELISM = 8

  val NUM_TASK_MANAGERS = 2

  val NUM_SLOTS_PER_TASK_MANAGER = 10

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> "9097")
    val workerConf = Map("celeborn.master.endpoints" -> "localhost:9097")
    workers = setupMiniClusterWithRandomPorts(masterConf, workerConf)._2
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("Celeborn Flink Hybrid Shuffle Integration test(Local) - word count") {
    assumeFlinkVersion()
    testLocalEnv()
  }

  test(
    "Celeborn Flink Hybrid Shuffle Integration test(Flink mini cluster) single tier - word count") {
    assumeFlinkVersion()
    testInMiniCluster()
  }

  private def assumeFlinkVersion(): Unit = {
    // Celeborn supports flink hybrid shuffle staring from flink 1.20
    val flinkVersion = sys.env.getOrElse("FLINK_VERSION", "")
    assume(
      flinkVersion.nonEmpty && FlinkVersion.fromVersionStr(
        flinkVersion.split("\\.").take(2).mkString(".")).isNewerOrEqualVersionThan(
        FlinkVersion.v1_20))
  }

  private def testLocalEnv(): Unit = {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = NUM_PARALLELISM
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory")
    configuration.setString(
      "taskmanager.network.hybrid-shuffle.external-remote-tier-factory.class",
      "org.apache.celeborn.plugin.flink.tiered.CelebornTierFactory")
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString(
      "execution.batch-shuffle-mode",
      "ALL_EXCHANGES_HYBRID_FULL")
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    configuration.setString(
      "jobmanager.partition.hybrid.partition-data-consume-constraint",
      "ALL_PRODUCERS_FINISHED")
    configuration.setString("rest.bind-port", "8081-8099")

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.getConfig.setParallelism(parallelism)
    env.disableOperatorChaining()
    // make parameters available in the web interface
    // TODO: WordCountHelper should execute with parallelism for [FLINK-37576][runtime] Fix the incorrect status of the isBroadcast field in AllToAllBlockingResultInfo when submitting a job graph.
    WordCountHelper.execute(env, 1)

    val graph = env.getStreamGraph
    env.execute(graph)
    checkFlushingFileLength()
  }

  private def testInMiniCluster(): Unit = {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = NUM_PARALLELISM
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory")
    configuration.setString(
      "taskmanager.network.hybrid-shuffle.external-remote-tier-factory.class",
      "org.apache.celeborn.plugin.flink.tiered.CelebornTierFactory")
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString(
      "execution.batch-shuffle-mode",
      "ALL_EXCHANGES_HYBRID_FULL")
    configuration.setString("taskmanager.memory.network.min", "256m")
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    configuration.setString(
      "jobmanager.partition.hybrid.partition-data-consume-constraint",
      "ALL_PRODUCERS_FINISHED")
    configuration.setString("rest.bind-port", "8081-8099")
    val env = getEnvironment(configuration);
    env.getConfig.setParallelism(parallelism)
    env.disableOperatorChaining()
    // make parameters available in the web interface
    // TODO: WordCountHelper should execute with parallelism for [FLINK-37576][runtime] Fix the incorrect status of the isBroadcast field in AllToAllBlockingResultInfo when submitting a job graph.
    WordCountHelper.execute(env, 1)

    val graph = env.getStreamGraph
    graph.setJobType(JobType.BATCH)
    val jobGraph = StreamingJobGraphGenerator.createJobGraph(graph)
    JobGraphRunningHelper.execute(
      jobGraph,
      configuration,
      NUM_TASK_MANAGERS,
      NUM_SLOTS_PER_TASK_MANAGER)
    checkFlushingFileLength()
  }

  def getEnvironment(configuration: Configuration): StreamExecutionEnvironment = {
    configuration.setString("taskmanager.network.hybrid-shuffle.enable-new-mode", "true")
    configuration.setString("execution.batch.adaptive.auto-parallelism.enabled", "true")
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "10")
    configuration.setString("restart-strategy.fixed-delay.delay", "0s")
    StreamExecutionEnvironment.getExecutionEnvironment(configuration)
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
