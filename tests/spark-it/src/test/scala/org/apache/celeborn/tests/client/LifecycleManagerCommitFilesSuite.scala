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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.client.{LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.client.commit.CommitFilesParam
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.CommitInfo

class LifecycleManagerCommitFilesSuite extends WithShuffleClientSuite with MiniClusterFeature {

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (master, _) = setupMiniClusterWithRandomPorts()
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("test commit files without mocking failure") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.TEST_CLIENT_MOCK_COMMIT_FILES_FAILURE.key, "false")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    lifecycleManager.setupEndpoints(res.workerResource, shuffleId, new ShuffleFailedWorkers())

    lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    lifecycleManager.commitManager.registerShuffle(shuffleId, 1, false, Array.empty)
    0 until 10 foreach { partitionId =>
      lifecycleManager.commitManager.finishMapperAttempt(shuffleId, 0, 0, 1, partitionId)
    }

    val commitHandler = lifecycleManager.commitManager.getCommitHandler(shuffleId)
    val params = new ArrayBuffer[CommitFilesParam](res.workerResource.size())
    res.workerResource.asScala.foreach { case (workerInfo, (primaryIds, replicaIds)) =>
      params += (CommitFilesParam(
        workerInfo,
        primaryIds.asScala.map(_.getUniqueId).toList.asJava,
        replicaIds.asScala.map(_.getUniqueId).toList.asJava))
    }
    commitHandler.doParallelCommitFiles(
      shuffleId,
      lifecycleManager.commitManager.committedPartitionInfo.get(shuffleId),
      params,
      new ShuffleFailedWorkers)

    workerInfos.keySet.foreach { worker =>
      worker.controller.shuffleCommitInfos.get(
        Utils.makeShuffleKey(APP, shuffleId)).values().asScala.foreach { commitInfo =>
        commitInfo.status == CommitInfo.COMMIT_INPROCESS || commitInfo.status == CommitInfo.COMMIT_FINISHED
      }
    }

    lifecycleManager.stop()
  }

  test("test commit files with mocking failure") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.TEST_CLIENT_MOCK_COMMIT_FILES_FAILURE.key, "true")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    lifecycleManager.setupEndpoints(res.workerResource, shuffleId, new ShuffleFailedWorkers())

    lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    lifecycleManager.commitManager.registerShuffle(shuffleId, 1, false, Array.empty)
    0 until 10 foreach { partitionId =>
      lifecycleManager.commitManager.finishMapperAttempt(shuffleId, 0, 0, 1, partitionId)
    }

    val commitHandler = lifecycleManager.commitManager.getCommitHandler(shuffleId)
    val params = new ArrayBuffer[CommitFilesParam](res.workerResource.size())
    res.workerResource.asScala.foreach { case (workerInfo, (primaryIds, replicaIds)) =>
      params += (CommitFilesParam(
        workerInfo,
        primaryIds.asScala.map(_.getUniqueId).toList.asJava,
        replicaIds.asScala.map(_.getUniqueId).toList.asJava))
    }
    commitHandler.doParallelCommitFiles(
      shuffleId,
      lifecycleManager.commitManager.committedPartitionInfo.get(shuffleId),
      params,
      new ShuffleFailedWorkers)

    workerInfos.keySet.foreach { worker =>
      worker.controller.shuffleCommitInfos.get(
        Utils.makeShuffleKey(APP, shuffleId)).values().asScala.foreach { commitInfo =>
        commitInfo.status == CommitInfo.COMMIT_INPROCESS || commitInfo.status == CommitInfo.COMMIT_FINISHED
      }
    }

    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
