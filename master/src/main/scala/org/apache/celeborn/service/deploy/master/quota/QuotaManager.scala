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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{QUOTA_DISK_BYTES_WRITTEN, QUOTA_DISK_FILE_COUNT, QUOTA_HDFS_BYTES_WRITTEN, QUOTA_HDFS_FILE_COUNT}
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.internal.config.ConfigEntry
import org.apache.celeborn.common.quota.Quota
import org.apache.celeborn.server.common.service.config.{DynamicConfig, DynamicConfigServiceFactory}
import org.apache.celeborn.server.common.service.config.DynamicConfig.ConfigType

class QuotaManager(celebornConf: CelebornConf) extends Logging {
  private val conf = celebornConf.clone
  val configService = DynamicConfigServiceFactory.getConfigService(conf)
  def getQuota(userIdentifier: UserIdentifier): Quota = {
    if (configService != null) {
      val config =
        configService.getTenantUserConfigFromCache(userIdentifier.tenantId, userIdentifier.name)
      getQuota(config)
    } else{
      Quota.DEFAULT_QUOTA
    }
  }

  def getQuota(config: DynamicConfig): Quota = {
    Quota(
      config.getValue(
        QUOTA_DISK_BYTES_WRITTEN.key,
        QUOTA_DISK_BYTES_WRITTEN.asInstanceOf[ConfigEntry[AnyRef]],
        classOf[Long],
        ConfigType.BYTES),
      config.getValue(
        QUOTA_DISK_FILE_COUNT.key,
        QUOTA_DISK_FILE_COUNT.asInstanceOf[ConfigEntry[AnyRef]],
        classOf[Long],
        ConfigType.STRING),
      config.getValue(
        QUOTA_HDFS_BYTES_WRITTEN.key,
        QUOTA_HDFS_BYTES_WRITTEN.asInstanceOf[ConfigEntry[AnyRef]],
        classOf[Long],
        ConfigType.BYTES),
      config.getValue(
        QUOTA_HDFS_FILE_COUNT.key,
        QUOTA_HDFS_FILE_COUNT.asInstanceOf[ConfigEntry[AnyRef]],
        classOf[Long],
        ConfigType.STRING))
  }
}
