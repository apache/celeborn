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

package org.apache.celeborn.tests.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.tests.spark.SparkTestBase

trait MemorySparkTestBase extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll with BeforeAndAfterEach
  with SparkTestBase {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val workerConfs =
      Map(
        "celeborn.worker.directMemoryRatioForMemoryFileStorage" -> "0.2",
        "celeborn.worker.directMemoryRatioToResume" -> "0.4")
    setupMiniClusterWithRandomPorts(workerConf = workerConfs, workerNum = 5)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  override def updateSparkConf(sparkConf: SparkConf, mode: ShuffleMode): SparkConf = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set(
      "spark.shuffle.manager",
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
    sparkConf.set("spark.shuffle.useOldFetchProtocol", "true")
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    sparkConf.set("spark.shuffle.service.enabled", "false")
    sparkConf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    sparkConf.set(s"spark.${MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
    sparkConf.set(s"spark.${SPARK_SHUFFLE_WRITER_MODE.key}", mode.toString)
    sparkConf.set(s"spark.celeborn.storage.availableTypes", "HDD,MEMORY")

    sparkConf
  }
}
