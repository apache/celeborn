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

package org.apache.celeborn.common.quota

import java.io.{File, FileInputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.yaml.snakeyaml.Yaml

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.{Identifier, SystemDefaultIdentifier, TenantIdentifier, UserIdentifier}
import org.apache.celeborn.common.util.{JavaUtils, Utils}

class DefaultQuotaManager(conf: CelebornConf) extends QuotaManager(conf) {

  private val quotaAttrs =
    Seq("diskBytesWritten", "diskFileCount", "hdfsBytesWritten", "hdfsFileCount")
  private val quotaConfig: ConcurrentHashMap[Identifier, Quota] = {
    JavaUtils.newConcurrentHashMap[Identifier, Quota]()
  }
  var systemDefaultQuota: Quota = Quota()

  override def refresh(): Unit = {
    // Not support refresh
  }

  override def initialize(): Unit = {
    Option(conf.quotaConfigurationPath.getOrElse(Utils.getDefaultQuotaConfigurationFile()))
      .foreach {
        quotaConfPath =>
          val stream = new FileInputStream(new File(quotaConfPath))
          val yaml = new Yaml()
          val data = yaml.load(stream).asInstanceOf[java.util.HashMap[String, Object]]

          val systemQuotaNode = data.get("system").asInstanceOf[java.util.Map[String, Object]]
          addQuota(systemQuotaNode, SystemDefaultIdentifier())

          Option(data.get("tenants")).map {
            _.asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala.map {
              tenantNode =>
                val tenantName = tenantNode.get("name").asInstanceOf[String]
                val tenantIdent = TenantIdentifier(tenantName)
                addQuota(tenantNode, tenantIdent)

                Option(tenantNode.get("users")).map {
                  _.asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala.map {
                    userNode =>
                      val userName = userNode.get("name").asInstanceOf[String]
                      val userIdent = UserIdentifier(tenantName, userName)
                      addQuota(userNode, userIdent)
                  }
                }
            }
          }
      }
  }

  override def getQuota(userIdentifier: UserIdentifier): Quota = {
    val quota = Option(quotaConfig.get(userIdentifier))
      .orElse(Option(quotaConfig.get(userIdentifier.toTenantIdentifier())))
      .getOrElse(systemDefaultQuota)
    quota
  }

  def addQuota(map: java.util.Map[String, Object], identifier: Identifier): Unit = {
    val quota = Quota()
    quotaAttrs.foreach {
      attr =>
        Option(map.get(attr)).map {
          value =>
            quota.update(identifier, attr, Utils.byteStringAsBytes(value.toString))
        }
    }
    identifier match {
      case _: SystemDefaultIdentifier => systemDefaultQuota = quota
      case _ => quotaConfig.put(identifier, quota)
    }
  }

}
