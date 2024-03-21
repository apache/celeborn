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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerApplicationMetaSuite extends CelebornFunSuite with MiniClusterFeature {
  protected var celebornConf: CelebornConf = _

  private val APP = "app-" + System.currentTimeMillis()
  private val userIdentifier = UserIdentifier("test", "celeborn")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty(CelebornConf.QUOTA_USER_SPECIFIC_TENANT.key, userIdentifier.tenantId)
    System.setProperty(CelebornConf.QUOTA_USER_SPECIFIC_USERNAME.key, userIdentifier.name)
    celebornConf = new CelebornConf()
    val (master, _) = setupMiniClusterWithRandomPorts()
    logInfo(s"master address is: ${master.conf.get(CelebornConf.MASTER_ENDPOINTS.key)}")
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("application meta") {
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    val arrayList = new util.ArrayList[Integer]()
    (0 to 10).foreach(i => {
      arrayList.add(i)
    })

    lifecycleManager.requestMasterRequestSlotsWithRetry(0, arrayList)

    assert(masterInfo._1.getApplicationList.contains(userIdentifier.toString))
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
