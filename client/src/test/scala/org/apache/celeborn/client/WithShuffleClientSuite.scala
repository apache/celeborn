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

package org.apache.celeborn.client

import java.lang
import java.util.concurrent.Callable

import scala.collection.JavaConverters._

import org.junit.Assert

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.util.JavaUtils.timeOutOrMeetCondition

trait WithShuffleClientSuite extends CelebornFunSuite {

  protected val celebornConf: CelebornConf = new CelebornConf()

  protected val APP = "app-1"
  protected val userIdentifier: UserIdentifier = UserIdentifier("mock", "mock")
  private val numMappers = 8
  private val mapId = 1
  private val attemptId = 0

  private var lifecycleManager: LifecycleManager = _
  private var shuffleClient: ShuffleClientImpl = _
  private var shuffleId = 0

  override protected def afterEach() {
    if (lifecycleManager != null) {
      lifecycleManager.stop()
    }

    if (shuffleClient != null) {
      shuffleClient.shutdown()
    }
  }

  test("test register map partition task") {
    prepareService()
    shuffleId = 1
    var location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId, 1)
    Assert.assertEquals(location.getId, 1)

    // retry register
    location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId, 1)
    Assert.assertEquals(location.getId, 1)

    // check all allocated slots
    var partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala
    var count =
      partitionLocationInfos.map(r => r.getMasterPartitions().size()).sum
    Assert.assertEquals(count, numMappers)

    // another mapId
    location =
      shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 1, attemptId, 2)
    Assert.assertEquals(location.getId, 2)

    // another mapId with another attemptId
    location =
      shuffleClient.registerMapPartitionTask(
        APP,
        shuffleId,
        numMappers,
        mapId + 1,
        attemptId + 1,
        numMappers + 1)
    Assert.assertEquals(location.getId, numMappers + 1)

    // check all allocated all slots
    partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala
    logInfo(partitionLocationInfos.toString())
    count = partitionLocationInfos.map(r => r.getMasterPartitions().size()).sum
    Assert.assertEquals(count, numMappers + 1)
  }

  test("test batch release partition") {
    shuffleId = 2
    celebornConf.set(CelebornConf.CLIENT_BATCH_HANDLE_RELEASE_PARTITION_ENABLED.key, "true")
    prepareService()
    registerAndFinishPartition()

    val partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala

    // check batch release
    lifecycleManager.releasePartition(
      shuffleId,
      4)

    timeOutOrMeetCondition(new Callable[java.lang.Boolean] {
      override def call(): lang.Boolean = {
        partitionLocationInfos.map(r => r.getMasterPartitions().size()).sum == numMappers
      }
    })
  }

  test("test release single partition") {
    shuffleId = 3
    celebornConf.set(CelebornConf.CLIENT_BATCH_HANDLE_RELEASE_PARTITION_ENABLED.key, "false")
    celebornConf.set(CelebornConf.CLIENT_BATCH_HANDLED_RELEASE_PARTITION_INTERVAL.key, "1s")
    prepareService()
    registerAndFinishPartition()

    val partitionLocationInfos = lifecycleManager.workerSnapshots(shuffleId).values().asScala

    // check single release
    lifecycleManager.releasePartition(
      shuffleId,
      4)

    Assert.assertEquals(
      partitionLocationInfos.map(r => r.getMasterPartitions().size()).sum,
      numMappers)
  }

  test("test map end & get reducer file group") {
    shuffleId = 4
    prepareService()
    registerAndFinishPartition()

    // reduce file group size (for empty partitions)
    Assert.assertEquals(shuffleClient.getReduceFileGroupsMap.size(), 0)

    // reduce normal empty RssInputStream
    var stream = shuffleClient.readPartition(APP, shuffleId, 1, 1)
    Assert.assertEquals(stream.read(), -1)

    // reduce normal null partition for RssInputStream
    stream = shuffleClient.readPartition(APP, shuffleId, 3, 1)
    Assert.assertEquals(stream.read(), -1)
  }

  private def prepareService(): Unit = {
    lifecycleManager = new LifecycleManager(APP, celebornConf)
    shuffleClient = new ShuffleClientImpl(celebornConf, userIdentifier)
    shuffleClient.setupMetaServiceRef(lifecycleManager.self)
  }

  private def registerAndFinishPartition(): Unit = {
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId, 1)
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 1, attemptId, 2)
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId + 2, attemptId, 3)

    // task number incr to numMappers + 1
    shuffleClient.registerMapPartitionTask(APP, shuffleId, numMappers, mapId, attemptId + 1, 9)
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, mapId, attemptId, numMappers, 1)
    // retry
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, mapId, attemptId, numMappers, 1)
    // another attempt
    shuffleClient.mapPartitionMapperEnd(
      APP,
      shuffleId,
      mapId,
      attemptId + 1,
      numMappers,
      9)
    // another mapper
    shuffleClient.mapPartitionMapperEnd(APP, shuffleId, mapId + 1, attemptId, numMappers, mapId + 1)
  }
}
