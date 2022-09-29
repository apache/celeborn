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

import java.io.{File, FileInputStream}

import scala.collection.JavaConverters._

import org.yaml.snakeyaml.Yaml

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier
import com.aliyun.emr.rss.common.util.Utils

class DefaultQuotaManager(conf: RssConf) extends QuotaManager(conf) {
  import QuotaManager._
  override def refresh(): Unit = {
    // Not support refresh
  }

  override def initialize(): Unit = {
    Option(
      RssConf.quotaConfigurationPath(conf)
        .getOrElse(Utils.getDefaultQuotaConfigurationFile()))
      .foreach { quotaConfPath =>
        val stream = new FileInputStream(new File(quotaConfPath))
        val yaml = new Yaml()
        val quotas =
          yaml.load(stream)
            .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Object]]]
        quotas.asScala.foreach { quotaSetting =>
          val tenantId = quotaSetting.get("tenantId").asInstanceOf[String]
          val name = quotaSetting.get("name").asInstanceOf[String]
          val userIdentifier = UserIdentifier(tenantId, name)
          val quota = Quota()
          quotaSetting.get("quota")
            .asInstanceOf[java.util.HashMap[String, Object]]
            .asScala
            .foreach { case (key, value) =>
              quota.update(userIdentifier, key, value.toString.toLong)
            }
          userQuotas.put(userIdentifier, quota)
        }
      }

  }
}
