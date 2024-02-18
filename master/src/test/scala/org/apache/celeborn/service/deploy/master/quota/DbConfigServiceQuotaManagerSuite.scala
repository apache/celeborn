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

import org.junit.Assert.assertEquals

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.quota.Quota
import org.apache.celeborn.common.util.Utils

class DbConfigServiceQuotaManagerSuite extends BaseQuotaManagerSuite {

  override def beforeAll(): Unit = {
    val conf = new CelebornConf()
    conf.set(
      CelebornConf.QUOTA_CONFIGURATION_PATH.key,
      getTestResourceFile("dynamicConfig-quota.yaml").getPath)
    conf.set(
      CelebornConf.QUOTA_MANAGER.key,
      classOf[DbConfigServiceQuotaManager].getName)
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_JDBC_URL,
      "jdbc:h2:mem:test;MODE=MYSQL;INIT=RUNSCRIPT FROM 'classpath:celeborn-0.5.0-h2-quota.sql'\\;" + "RUNSCRIPT FROM 'classpath:celeborn-0.5.0-h2-quota-ut-data.sql';DB_CLOSE_DELAY=-1;")
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_DRIVER_CLASS_NAME, "org.h2.Driver")
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_MAXIMUM_POOL_SIZE, 1)

    quotaManager = QuotaManager.instantiate(conf)
  }

  test("initialize QuotaManager") {
    assert(quotaManager.isInstanceOf[DbConfigServiceQuotaManager])
  }

  test("test celeborn quota conf") {
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_01", "Jerry")),
      Quota(Utils.byteStringAsBytes("100G"), 10000, Utils.byteStringAsBytes("10G"), 1000))
    // Fallback to tenant level
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_01", "name_not_exist")),
      Quota(Utils.byteStringAsBytes("10G"), 1000, Utils.byteStringAsBytes("10G"), 1000))
    // Fallback to system level
    assertEquals(
      quotaManager.getQuota(UserIdentifier("tenant_not_exist", "Tom")),
      Quota(Utils.byteStringAsBytes("1G"), 100, Utils.byteStringAsBytes("1G"), 100))
  }
}
