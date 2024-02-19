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

import org.apache.celeborn.common.quota.Quota
import org.apache.celeborn.server.common.service.config.DynamicConfig
import org.apache.celeborn.server.common.service.config.DynamicConfig.ConfigType

trait ConfigServiceQuotaManager extends QuotaManager {
  def getQuota(config: DynamicConfig): Quota = {
    // TODO: Default value should be -1, to implement an ConfigEntry?
    Quota(
      config.getValue("diskBytesWritten", null, classOf[Long], ConfigType.BYTES),
      config.getValue("diskFileCount", null, classOf[Long], ConfigType.STRING),
      config.getValue("hdfsBytesWritten", null, classOf[Long], ConfigType.BYTES),
      config.getValue("hdfsFileCount", null, classOf[Long], ConfigType.STRING))
  }
}
