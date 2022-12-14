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

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils

case class Quota(
    var diskBytesWritten: Long = -1,
    var diskFileCount: Long = -1,
    var hdfsBytesWritten: Long = -1,
    var hdfsFileCount: Long = -1) extends Logging {

  def update(userIdentifier: UserIdentifier, name: String, value: Long): Unit = {
    name match {
      case "diskBytesWritten" => diskBytesWritten = value
      case "diskFileCount" => diskFileCount = value
      case "hdfsBytesWritten" => hdfsBytesWritten = value
      case "hdfsFileCount" => hdfsFileCount = value
      case _ => logWarning(s"Unsupported quota name: $name for user: $userIdentifier.")
    }
  }

  def checkQuotaSpaceAvailable(
      userIdentifier: UserIdentifier,
      resourceResumption: ResourceConsumption): Boolean = {
    val exceed =
      checkDiskBytesWritten(userIdentifier, resourceResumption.diskBytesWritten) ||
        checkDiskFileCount(userIdentifier, resourceResumption.diskFileCount) ||
        checkHdfsBytesWritten(userIdentifier, resourceResumption.hdfsBytesWritten) ||
        checkHdfsFileCount(userIdentifier, resourceResumption.hdfsFileCount)
    !exceed
  }

  private def checkDiskBytesWritten(userIdentifier: UserIdentifier, value: Long): Boolean = {
    val exceed = (diskBytesWritten > 0 && value >= diskBytesWritten)
    if (exceed) {
      logWarning(s"User $userIdentifier quota exceed diskBytesWritten, " +
        s"${Utils.bytesToString(value)} >= ${Utils.bytesToString(diskBytesWritten)}")
    }
    exceed
  }

  private def checkDiskFileCount(userIdentifier: UserIdentifier, value: Long): Boolean = {
    val exceed = (diskFileCount > 0 && value >= diskFileCount)
    if (exceed) {
      logWarning(s"User $userIdentifier quota exceed diskFileCount, $value >= $diskFileCount")
    }
    exceed
  }

  private def checkHdfsBytesWritten(userIdentifier: UserIdentifier, value: Long): Boolean = {
    val exceed = (hdfsBytesWritten > 0 && value >= hdfsBytesWritten)
    if (exceed) {
      logWarning(s"User $userIdentifier quota exceed hdfsBytesWritten, " +
        s"${Utils.bytesToString(value)} >= ${Utils.bytesToString(hdfsBytesWritten)}")
    }
    exceed
  }

  private def checkHdfsFileCount(userIdentifier: UserIdentifier, value: Long): Boolean = {
    val exceed = (hdfsFileCount > 0 && value >= hdfsFileCount)
    if (exceed) {
      logWarning(s"User $userIdentifier quota exceed hdfsFileCount, $value >= $hdfsFileCount")
    }
    exceed
  }

  override def toString: String = {
    s"Quota[" +
      s"diskBytesWritten=${Utils.bytesToString(diskBytesWritten)}, " +
      s"diskFileCount=$diskFileCount, " +
      s"hdfsBytesWritten=${Utils.bytesToString(hdfsBytesWritten)}, " +
      s"hdfsFileCount=$hdfsFileCount" +
      s"]"
  }
}
