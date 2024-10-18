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
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.quota.{Quota, ResourceConsumption}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.service.config.ConfigService

class QuotaManager(celebornConf: CelebornConf, configService: ConfigService) extends Logging {
  val DEFAULT_QUOTA = Quota(
    celebornConf.get(CelebornConf.QUOTA_DISK_BYTES_WRITTEN),
    celebornConf.get(CelebornConf.QUOTA_DISK_FILE_COUNT),
    celebornConf.get(CelebornConf.QUOTA_HDFS_BYTES_WRITTEN),
    celebornConf.get(CelebornConf.QUOTA_HDFS_FILE_COUNT))
  def getQuota(userIdentifier: UserIdentifier): Quota = {
    if (configService != null) {
      val config =
        configService.getTenantUserConfigFromCache(userIdentifier.tenantId, userIdentifier.name)
      config.getQuota
    } else {
      DEFAULT_QUOTA
    }
  }

  private def getQuotaWithWatermark(
      userIdentifier: UserIdentifier,
      quotaWatermark: Double): Quota = {
    val quota = getQuota(userIdentifier)
    Quota(
      (quota.diskBytesWritten * quotaWatermark).toLong,
      (quota.diskFileCount * quotaWatermark).toLong,
      (quota.hdfsBytesWritten * quotaWatermark).toLong,
      (quota.hdfsFileCount * quotaWatermark).toLong)
  }

  def checkQuotaSpaceAvailable(
      userIdentifier: UserIdentifier,
      resourceResumption: ResourceConsumption,
      quotaWatermark: Double = 1.0): (Boolean, String) = {
    val quota = getQuotaWithWatermark(userIdentifier, quotaWatermark)

    val checkResults = Seq(
      checkDiskBytesWritten(userIdentifier, resourceResumption.diskBytesWritten, quota),
      checkDiskFileCount(userIdentifier, resourceResumption.diskFileCount, quota),
      checkHdfsBytesWritten(userIdentifier, resourceResumption.hdfsBytesWritten, quota),
      checkHdfsFileCount(userIdentifier, resourceResumption.hdfsFileCount, quota))
    val exceed = checkResults.foldLeft(false)(_ || _._1)
    val reason = checkResults.foldLeft("")(_ + _._2)
    (!exceed, reason)
  }

  private def checkDiskBytesWritten(
      userIdentifier: UserIdentifier,
      value: Long,
      quota: Quota): (Boolean, String) = {
    val exceed = (quota.diskBytesWritten > 0 && value >= quota.diskBytesWritten)
    var reason = ""
    if (exceed) {
      reason = s"User $userIdentifier used diskBytesWritten (${Utils.bytesToString(value)}) " +
        s"exceeds quota (${Utils.bytesToString(quota.diskBytesWritten)}). "
      logWarning(reason)
    }
    (exceed, reason)
  }

  private def checkDiskFileCount(
      userIdentifier: UserIdentifier,
      value: Long,
      quota: Quota): (Boolean, String) = {
    val exceed = (quota.diskFileCount > 0 && value >= quota.diskFileCount)
    var reason = ""
    if (exceed) {
      reason =
        s"User $userIdentifier used diskFileCount($value) exceeds quota(${quota.diskFileCount}). "
      logWarning(reason)
    }
    (exceed, reason)
  }

  private def checkHdfsBytesWritten(
      userIdentifier: UserIdentifier,
      value: Long,
      quota: Quota): (Boolean, String) = {
    val exceed = (quota.hdfsBytesWritten > 0 && value >= quota.hdfsBytesWritten)
    var reason = ""
    if (exceed) {
      reason = s"User $userIdentifier used hdfsBytesWritten(${Utils.bytesToString(value)}) " +
        s"exceeds quota(${Utils.bytesToString(quota.hdfsBytesWritten)}). "
      logWarning(reason)
    }
    (exceed, reason)
  }

  private def checkHdfsFileCount(
      userIdentifier: UserIdentifier,
      value: Long,
      quota: Quota): (Boolean, String) = {
    val exceed = (quota.hdfsFileCount > 0 && value >= quota.hdfsFileCount)
    var reason = ""
    if (exceed) {
      reason =
        s"User $userIdentifier used hdfsFileCount($value) exceeds quota(${quota.hdfsFileCount}). "
      logWarning(reason)
    }
    (exceed, reason)
  }
}
