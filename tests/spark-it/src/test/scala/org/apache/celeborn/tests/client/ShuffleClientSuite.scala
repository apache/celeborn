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

package org.apache.celeborn.tests.client

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.util.PackedPartitionId
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ShuffleClientSuite extends AnyFunSuite with MiniClusterFeature
  with BeforeAndAfterAll {
  val masterPort = 19097
  val APP = "app-1"
  var shuffleClient: ShuffleClientImpl = _
  var lifecycleManager: LifecycleManager = _
  val numMappers = 8
  val mapId = 1
  val attemptId = 0

  override def beforeAll(): Unit = {
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> masterPort.toString)
    val workerConf = Map(
      "celeborn.master.endpoints" -> s"localhost:$masterPort")
    setUpMiniCluster(masterConf, workerConf)

    val clientConf = new CelebornConf()
      .set("celeborn.master.endpoints", s"localhost:$masterPort")
      .set("celeborn.push.replicate.enabled", "true")
      .set("celeborn.push.buffer.size", "256K")
    lifecycleManager = new LifecycleManager(APP, clientConf)
    shuffleClient = new ShuffleClientImpl(clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupMetaServiceRef(lifecycleManager.self)
  }

  test(s"test register map partition task") {
    val shuffleId = 1
    var location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId)
    Assert.assertEquals(location.getId, PackedPartitionId.packedPartitionId(mapId, attemptId))

    // retry register
    location = shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId)
    Assert.assertEquals(location.getId, PackedPartitionId.packedPartitionId(mapId, attemptId))

    // check all allocated slots
    var partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala
    var count =
      partitionLocationInfos.map(r => r.getAllMasterLocations(shuffleId.toString).size()).sum
    Assert.assertEquals(count, numMappers)

    // another mapId
    location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 1, attemptId)
    Assert.assertEquals(location.getId, PackedPartitionId.packedPartitionId(mapId + 1, attemptId))

    // another mapId with another attemptId
    location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 1, attemptId + 1)
    Assert.assertEquals(
      location.getId,
      PackedPartitionId.packedPartitionId(mapId + 1, attemptId + 1))

    // check all allocated all slots
    partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala
    print(partitionLocationInfos)
    count =
      partitionLocationInfos.map(r => r.getAllMasterLocations(shuffleId.toString).size()).sum
    Assert.assertEquals(count, numMappers + 1)
  }

  test(s"test map end & get reducer file group") {
    val shuffleId = 2
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId)
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 1, attemptId)
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 2, attemptId)
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId + 1)
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, numMappers, mapId, attemptId, mapId)
    // retry
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, numMappers, mapId, attemptId, mapId)
    // another attempt
    shuffleClient.mapPartitionMapperEnd(
      APP,
      shuffleId,
      numMappers,
      mapId,
      attemptId + 1,
      PackedPartitionId
        .packedPartitionId(mapId, attemptId + 1))
    // another mapper
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, numMappers, mapId + 1, attemptId, mapId + 1)

    // reduce file group size (for empty partitions)
    Assert.assertEquals(shuffleClient.getReduceFileGroupsMap.size(), 0)

    // reduce normal empty RssInputStream
    var stream = shuffleClient.readPartition(APP, shuffleId, 1, 1)
    Assert.assertEquals(stream.read(), -1)

    // reduce normal null partition for RssInputStream
    stream = shuffleClient.readPartition(APP, shuffleId, 3, 1)
    Assert.assertEquals(stream.read(), -1)
  }

  override def afterAll(): Unit = {
    // TODO refactor MiniCluster later
    println("test done")
    sys.exit(0)
  }
}
