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

package com.aliyun.emr.rss.common.quota

import org.junit.Assert.assertEquals

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

class DefaultQuotaManagerSuite extends BaseQuotaManagerSuite {

  override def beforeAll(): Unit = {
    val conf = new RssConf()
    conf.set("rss.quota.{`aa`.`bb`}.diskBytesWritten", "100")
    conf.set("rss.quota.{`aa`.`bb`}.diskFileCount", "200")
    conf.set("rss.quota.{`aa`.`bb`}.hdfsBytesWritten", "300")
    conf.set("rss.quota.{`aa`.`bb`}.hdfsFileCount", "400")
    conf.set("rss.quota.{`aa`.`cc}.hdfsFileCount", "400")

    quotaManager = QuotaManager.instantiate(conf)
  }

  test("initialize QuotaManager") {
    assert(quotaManager.isInstanceOf[DefaultQuotaManager])
    println(quotaManager)
  }

  test("test rss quota conf") {
    assertEquals(quotaManager.getQuota(UserIdentifier("aa", "bb")), new Quota(100, 200, 300, 400))
    assertEquals(quotaManager.getQuota(UserIdentifier("aa", "cc")), new Quota(-1, -1, -1, -1))
  }
}
