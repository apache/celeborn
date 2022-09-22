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

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

class DefaultQuotaManager(conf: RssConf) extends QuotaManager(conf) {
  import QuotaManager._
  override def refresh(): Unit = {
    // Not support refresh
  }

  /**
   * Initialize user quota settings.
   */
  override def initialize(): Unit = {
    conf.getAll.foreach { case (key, value) =>
      if (QUOTA_REGEX.findPrefixOf(key).isDefined) {
        val QUOTA_REGEX(user, suffix) = key
        val userIdentifier = UserIdentifier(user)
        val quotaValue =
          try {
            value.toLong
          } catch {
            case e =>
              logError(
                s"Quota value of ${userIdentifier} should be a long value, incorrect setting : ${value}")
              -1
          }
        val quota = userQuotas.getOrDefault(userIdentifier, new Quota())
        quota.update(userIdentifier, suffix, quotaValue)
        userQuotas.put(userIdentifier, quota)
      }
    }
  }
}
