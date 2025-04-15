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

package org.apache.celeborn.service.deploy.cluster

import java.io.File
import java.util

import scala.io.Source
import scala.util.Random

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.common.util.Utils.runCommand
import org.apache.celeborn.service.deploy.MiniClusterFeature

trait JavaReadCppWriteTestBase extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup Celeborn mini cluster")
    val (m, _) = setupMiniClusterWithRandomPorts()
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete, stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  def testJavaReadCppWrite(codec: CompressionCodec): Unit = {
    beforeAll()
    try {
      runJavaReadCppWrite(codec)
    } finally {
      afterAll()
    }
  }

  def runJavaReadCppWrite(codec: CompressionCodec): Unit = {
    val appUniqueId = "test-app"
    val shuffleId = 0
    val attemptId = 0

    // Create lifecycleManager.
    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, codec.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set(CelebornConf.READ_LOCAL_SHUFFLE_FILE, false)
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
    val lifecycleManager = new LifecycleManager(appUniqueId, clientConf)

    // Create writer shuffleClient.
    val shuffleClient =
      new ShuffleClientImpl(appUniqueId, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    // Generate random data, write with shuffleClient and calculate result.
    val numMappers = 2
    val numPartitions = 2
    val maxData = 1000000
    val numData = 1000
    var sums = new util.ArrayList[Long](numPartitions)
    val rand = new Random()
    for (mapId <- 0 until numMappers) {
      for (partitionId <- 0 until numPartitions) {
        sums.add(0)
        for (i <- 0 until numData) {
          val data = rand.nextInt(maxData)
          sums.set(partitionId, sums.get(partitionId) + data)
          val dataStr = "-" + data.toString
          shuffleClient.pushOrMergeData(
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            dataStr.getBytes,
            0,
            dataStr.length,
            numMappers,
            numPartitions,
            false,
            true)
        }
      }
      shuffleClient.pushMergedData(shuffleId, mapId, attemptId)
      shuffleClient.mapperEnd(shuffleId, mapId, attemptId, numMappers)
    }

    // Launch cpp reader to read data, calculate result and write to specific result file.
    val cppResultFile = "/tmp/celeborn-cpp-result.txt"
    val lifecycleManagerHost = lifecycleManager.getHost
    val lifecycleManagerPort = lifecycleManager.getPort
    val projectDirectory = new File(new File(".").getAbsolutePath)
    val cppBinRelativeDirectory = "cpp/build/celeborn/tests/"
    val cppBinFileName = "cppDataSumWithReaderClient"
    val cppBinFilePath = s"$projectDirectory/$cppBinRelativeDirectory/$cppBinFileName"
    // Execution command: $exec lifecycleManagerHost lifecycleManagerPort appUniqueId shuffleId attemptId numPartitions cppResultFile
    val command = {
      s"$cppBinFilePath $lifecycleManagerHost $lifecycleManagerPort $appUniqueId $shuffleId $attemptId $numPartitions $cppResultFile"
    }
    println(s"run command: $command")
    val commandOutput = runCommand(command)
    println(s"command output: $commandOutput")

    // Verify the sum result.
    var lineCount = 0
    for (line <- Source.fromFile(cppResultFile, "utf-8").getLines.toList) {
      val data = line.toLong
      Assert.assertEquals(data, sums.get(lineCount))
      lineCount += 1
    }
    Assert.assertEquals(lineCount, numPartitions)
    lifecycleManager.stop()
    shuffleClient.shutdown()
  }

}
