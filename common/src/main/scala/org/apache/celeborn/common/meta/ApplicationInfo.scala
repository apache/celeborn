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

package org.apache.celeborn.common.meta

import java.util.concurrent.atomic.LongAdder

import org.apache.celeborn.common.identity.UserIdentifier

class ApplicationInfo {
  private var userIdentifier: UserIdentifier = _
  private val totalWritten = new LongAdder()
  private val fileCount = new LongAdder()
  @volatile private var lastHeartbeatTime: Long = System.currentTimeMillis()

  def setUserIdentifier(userIdentifier: UserIdentifier): Unit = {
    if (this.userIdentifier == null) {
      this.userIdentifier = userIdentifier
    }
  }

  def updateTotalWritten(bytes: java.lang.Long): Unit = {
    totalWritten.add(bytes)
  }

  def updateFileCount(count: java.lang.Long): Unit = {
    fileCount.add(count)
  }

  def setHeartbeatTime(time: java.lang.Long): Unit = {
    lastHeartbeatTime = time
  }

  def getUserIdentifier: UserIdentifier = userIdentifier
  def getTotalWritten: java.lang.Long = totalWritten.sum()
  def getFileCount: java.lang.Long = fileCount.sum()
  def getHeartbeatTime: java.lang.Long = lastHeartbeatTime
}
