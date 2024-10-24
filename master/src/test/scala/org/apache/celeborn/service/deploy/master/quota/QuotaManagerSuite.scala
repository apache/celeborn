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

package org.apache.celeborn.service.deploy.master.quota

import java.io.File

import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.quota.{Quota, ResourceConsumption}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.service.config.DynamicConfigServiceFactory

class QuotaManagerSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {
  protected var quotaManager: QuotaManager = _

  // helper function
  final protected def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    DynamicConfigServiceFactory.reset()

    val conf = new CelebornConf()
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-quota.yaml").getPath)
    quotaManager = new QuotaManager(conf, DynamicConfigServiceFactory.getConfigService(conf))
  }

  test("test celeborn quota conf") {
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_01", "Jerry")),
      Quota(Utils.byteStringAsBytes("100G"), 10000, Utils.byteStringAsBytes("10G"), Long.MaxValue))
    // Fallback to tenant level
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_01", "name_not_exist")),
      Quota(Utils.byteStringAsBytes("10G"), 1000, Utils.byteStringAsBytes("10G"), Long.MaxValue))
    // Fallback to system level
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_not_exist", "Tom")),
      Quota(Utils.byteStringAsBytes("1G"), 100, Utils.byteStringAsBytes("1G"), Long.MaxValue))
  }

  test("test check quota return result") {
    val user = UserIdentifier("tenant_01", "Jerry")
    val rc1 =
      ResourceConsumption(Utils.byteStringAsBytes("10G"), 20, Utils.byteStringAsBytes("1G"), 40)
    val rc2 =
      ResourceConsumption(Utils.byteStringAsBytes("10G"), 20, Utils.byteStringAsBytes("30G"), 40)
    val rc3 =
      ResourceConsumption(
        Utils.byteStringAsBytes("200G"),
        20000,
        Utils.byteStringAsBytes("30G"),
        40)

    val res1 = quotaManager.checkQuotaSpaceAvailable(user, rc1)
    val res2 = quotaManager.checkQuotaSpaceAvailable(user, rc2)
    val res3 = quotaManager.checkQuotaSpaceAvailable(user, rc3)

    val exp1 = (true, "")
    val exp2 = (
      false,
      s"User $user used hdfsBytesWritten(30.0 GiB) exceeds quota(10.0 GiB). ")
    val exp3 = (
      false,
      s"User $user used diskBytesWritten (200.0 GiB) exceeds quota (100.0 GiB). " +
        s"User $user used diskFileCount(20000) exceeds quota(10000). " +
        s"User $user used hdfsBytesWritten(30.0 GiB) exceeds quota(10.0 GiB). ")

    assert(res1 == exp1)
    assert(res2 == exp2)
    assert(res3 == exp3)
  }
}
