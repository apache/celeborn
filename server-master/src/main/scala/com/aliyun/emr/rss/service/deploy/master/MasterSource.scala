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

package com.aliyun.emr.rss.service.deploy.master

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.service.deploy.master.MasterSource.OFFER_SLOTS_DURATION

class MasterSource(rssConf: RssConf)
  extends AbstractSource(rssConf, MetricsSystem.ROLE_MASTER) with Logging {
  override val sourceName = s"master"

  addTimer(OFFER_SLOTS_DURATION)
  // start cleaner
  startCleaner()
}

object MasterSource {
  val SERVLET_PATH = "/metrics/prometheus"

  val WORKER_TOTAL = "master_worker_total"

  val BLACKLISTED_WORKER_TOTAL = "master_blacklisted_worker_total"

  val REGISTERED_SHUFFLE_TOTAL = "master_registered_shuffle_total"

  val IS_ACTIVE_MASTER = "master_is_active"

  val PARTITION_SIZE = "master_partition_size_total"

  val OFFER_SLOTS_DURATION = "master_offer_slots_duration_milliseconds"
}
