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

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, ExecutionOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class SplitTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  var workers: collection.Set[Worker] = null
  var port = 0
  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val (m, w) = setupMiniClusterWithRandomPorts(workerConf =
      Map(CelebornConf.WORKER_FLUSHER_BUFFER_SIZE.key -> "10k"))
    workers = w
    port = m.conf.get(CelebornConf.MASTER_PORT)
  }
  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("celeborn flink integration test - shuffle partition split test") {
    val configuration = new Configuration
    val parallelism = 8
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory")
    configuration.setString(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$port")
    configuration.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_BLOCKING")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    configuration.setString("taskmanager.memory.network.min", "1024m")
    configuration.setString("rest.bind-port", "8081-8099")
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.min-parallelism",
      "" + parallelism)
    configuration.setString(
      "execution.batch.adaptive.auto-parallelism.max-parallelism",
      "" + parallelism)
    configuration.setString("restart-strategy.type", "fixed-delay")
    configuration.setString("restart-strategy.fixed-delay.attempts", "50")
    configuration.setString("restart-strategy.fixed-delay.delay", "5s")
    configuration.setString(CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key, "10k")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    env.getConfig.setParallelism(parallelism)
    SplitHelper.runSplitRead(env)
    env.execute("split test")
  }

}
