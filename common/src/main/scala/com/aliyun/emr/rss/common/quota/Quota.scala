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

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

class Quota(
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

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Quota]) {
      val others = obj.asInstanceOf[Quota]
      others.diskBytesWritten == diskBytesWritten &&
      others.diskFileCount == diskFileCount &&
      others.hdfsBytesWritten == hdfsBytesWritten &&
      others.hdfsFileCount == hdfsFileCount
    } else {
      false
    }
  }

  override def toString: String = {
    s"Quota[diskBytesWritten=$diskBytesWritten, diskFileCount=$diskFileCount, hdfsBytesWritten=$hdfsBytesWritten, hdfsFileCount=$hdfsFileCount]"
  }
}
