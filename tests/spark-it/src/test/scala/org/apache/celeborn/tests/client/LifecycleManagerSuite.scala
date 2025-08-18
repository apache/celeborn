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

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.client.{LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerSuite extends WithShuffleClientSuite with MiniClusterFeature {
  override protected val userIdentifier = UserIdentifier("test", "celeborn")

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
    .set(CelebornConf.USER_SPECIFIC_TENANT.key, userIdentifier.tenantId)
    .set(CelebornConf.USER_SPECIFIC_USERNAME.key, userIdentifier.name)
    .set(CelebornConf.CLIENT_APPLICATION_EXTRA_INFO.key, "k1=v1")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (master, _) = setupMiniClusterWithRandomPorts()
    logInfo(s"master address is: ${master.conf.get(CelebornConf.MASTER_ENDPOINTS.key)}")
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("CELEBORN-1151: test request slots with client blacklist worker with filter enabled") {
    celebornConf.set(CelebornConf.REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED, true)
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    val arrayList = new util.ArrayList[Integer]()
    (0 to 10).foreach(i => {
      arrayList.add(i)
    })

    // test request slots without worker excluded
    val headWorkerInfo = workerInfos.keySet.head.workerInfo
    val res1 = lifecycleManager.requestMasterRequestSlotsWithRetry(0, arrayList)
      .workerResource.keySet()
    assert(res1.contains(headWorkerInfo))

    // test request slots with 1 worker excluded, result should not contains the excluded worker
    val commitFilesFailedWorkers = new LifecycleManager.ShuffleFailedWorkers()
    commitFilesFailedWorkers.put(
      workerInfos.keySet.head.workerInfo,
      (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis()))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val res2 = lifecycleManager.requestMasterRequestSlotsWithRetry(1, arrayList)
      .workerResource.keySet()
    assert(!res2.contains(headWorkerInfo))

    // test request slots with all workers excluded, response should be WORKER_EXCLUDED
    workerInfos.keySet.foreach(worker =>
      commitFilesFailedWorkers.put(
        worker.workerInfo,
        (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis())))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val status = lifecycleManager.requestMasterRequestSlotsWithRetry(2, arrayList).status
    assert(status == StatusCode.WORKER_EXCLUDED)

    lifecycleManager.stop()
  }

  test("CELEBORN-1151: test request slots with client blacklist worker with filter not enabled") {
    celebornConf.set(CelebornConf.REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED, false)
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    val arrayList = new util.ArrayList[Integer]()
    (0 to 10).foreach(i => {
      arrayList.add(i)
    })

    // test request slots with all workers excluded, response should not excluded any worker
    val commitFilesFailedWorkers = new LifecycleManager.ShuffleFailedWorkers()
    workerInfos.keySet.foreach(worker =>
      commitFilesFailedWorkers.put(
        worker.workerInfo,
        (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis())))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(0, arrayList)
      .workerResource.keySet()
    assert(res.size() == workerInfos.size)
    assert(res.contains(workerInfos.keySet.head.workerInfo))
    lifecycleManager.stop()
  }

  test("CELEBORN-1258: Support to register application info with user identifier and extra info") {
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    val arrayList = new util.ArrayList[Integer]()
    (0 to 10).foreach(i => {
      arrayList.add(i)
    })

    lifecycleManager.requestMasterRequestSlotsWithRetry(0, arrayList)

    eventually(timeout(3.seconds), interval(0.milliseconds)) {
      val appInfo = masterInfo._1.statusSystem.applicationInfos.get(APP)
      assert(appInfo.userIdentifier == userIdentifier)
      assert(appInfo.extraInfo.get("k1") == "v1")
      assert(appInfo.registrationTime > 0 && appInfo.registrationTime < System.currentTimeMillis())
    }
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
