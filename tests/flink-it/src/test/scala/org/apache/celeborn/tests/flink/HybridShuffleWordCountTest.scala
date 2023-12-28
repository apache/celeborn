package org.apache.celeborn.tests.flink

import java.io.File

import scala.collection.JavaConverters._

import org.apache.flink.api.common.{BatchShuffleMode, RuntimeExecutionMode}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.{BatchExecutionOptions, Configuration, ExecutionOptions, NettyShuffleEnvironmentOptions, RestOptions}
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

  val MULTIPLE_TIER_FACTORY_CLASS_NAME =
    "org.apache.celeborn.plugin.flink.tiered.CelebornTierFactoryCreator"

  val SINGLE_TIER_FACTORY_CLASS_NAME =
    "org.apache.celeborn.plugin.flink.tiered.CelebornSingleTierFactoryCreator"

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> "9097")
    val workerConf = Map("celeborn.master.endpoints" -> "localhost:9097")
    workers = setUpMiniCluster(masterConf, workerConf)._2
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("Integration test(Local) multiple tier - word count") {
    testLocalEnv(MULTIPLE_TIER_FACTORY_CLASS_NAME)
  }

  test("Integration test(Local) single tier - word count") {
    testLocalEnv(SINGLE_TIER_FACTORY_CLASS_NAME)
  }

  test("Integration test(Flink mini cluster) multiple tier - word count") {
    testInMiniCluster(MULTIPLE_TIER_FACTORY_CLASS_NAME)
  }

  test("Integration test(Flink mini cluster) single tier - word count") {
    testInMiniCluster(SINGLE_TIER_FACTORY_CLASS_NAME)
  }

  private def testLocalEnv(tierFactoryCreatorClassName: String): Unit = {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = NUM_PARALLELISM
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory")
    configuration.setString(
      "taskmanager.network.hybrid-shuffle.tiered.factory.creator.class",
      tierFactoryCreatorClassName)
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.set(
      ExecutionOptions.BATCH_SHUFFLE_MODE,
      BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL)
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString(RestOptions.BIND_PORT, "8081-8089")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.getConfig.setParallelism(parallelism)
    env.disableOperatorChaining()
    // make parameters available in the web interface
    WordCountHelper.execute(env, parallelism)

    val graph = env.getStreamGraph
    env.execute(graph)
    checkFlushingFileLength()
  }

  private def testInMiniCluster(tierFactoryCreatorClassName: String): Unit = {
    // set up execution environment
    val configuration = new Configuration
    val parallelism = NUM_PARALLELISM
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory")
    //    configuration.setString("taskmanager.network.memory.floating-buffers-per-gate", "1000")
    configuration.setString(
      "taskmanager.network.hybrid-shuffle.tiered.factory.creator.class",
      tierFactoryCreatorClassName)
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.set(
      ExecutionOptions.BATCH_SHUFFLE_MODE,
      BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL)
    configuration.setString("taskmanager.memory.network.min", "256m")
    configuration.setString(RestOptions.BIND_PORT, "8081-8089")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    val env = getEnvironment(configuration);
    env.getConfig.setParallelism(parallelism)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))
    env.disableOperatorChaining()
    // make parameters available in the web interface
    WordCountHelper.execute(env, parallelism)

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
    configuration.setBoolean(
      NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE.key(),
      true)
    configuration.setBoolean(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0L))
    env
  }

  private def checkFlushingFileLength(): Unit = {
    workers.map(worker => {
      worker.storageManager.workingDirWriters.values().asScala.map(writers => {
        writers.forEach((fileName, fileWriter) => {
          assert(new File(fileName).length() == fileWriter.getFileInfo.getFileLength)
        })
      })
    })
  }
}
