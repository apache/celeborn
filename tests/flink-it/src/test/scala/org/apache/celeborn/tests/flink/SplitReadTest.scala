package org.apache.celeborn.tests.flink

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{ConfigConstants, Configuration, ExecutionOptions, RestOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature

class SplitReadTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    logInfo("test initialized , setup rss mini cluster")
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> "9097")
    val workerConf = Map("celeborn.master.endpoints" -> "localhost:9097")
    setUpMiniCluster(masterConf, workerConf)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop rss mini cluster")
    shutdownMiniCluster()
  }

  test("celeborn flink integration test - simple shuffle test") {
    val configuration = new Configuration
    val parallelism = 8
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory")
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_BLOCKING")
    configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString(RestOptions.BIND_PORT, "8081-8089")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
//    configuration.setString("celeborn.client.shuffle.partitionSplit.threshold", "32m")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    SplitReadHelper.runSplitRead(env)
  }
}
