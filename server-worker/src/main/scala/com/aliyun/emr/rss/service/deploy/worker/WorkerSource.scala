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

package com.aliyun.emr.rss.service.deploy.worker

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.AbstractSource

class WorkerSource(rssConf: RssConf) extends AbstractSource(rssConf, MetricsSystem.ROLE_WORKER) {
  override val sourceName = "worker"

  import WorkerSource._
  // add counters
  addCounter(PUSHDATA_FAIL_TOTAL)

  // add Timers
  addTimer(COMMIT_FILES_DURATION)
  addTimer(RESERVE_SLOTS_DURATION)
  addTimer(FLUSH_DATA_DURATION)
  addTimer(MASTER_PUSHDATA_DURATION)
  addTimer(SLAVE_PUSHDATA_DURATION)

  addTimer(FETCH_CHUNK_DURATION)
  addTimer(OPEN_STREAM_DURATION)
  addTimer(TAKE_BUFFER_DURATION)
  addTimer(SORT_DURATION)

  // start cleaner thread
  startCleaner()
}

object WorkerSource {
  val SERVLET_PATH = "/metrics/prometheus"

  // slots
  val RESERVE_SLOTS_DURATION = "worker_reserve_slots_duration_milliseconds"
  val REGISTERED_SHUFFLE_TOTAL = "worker_registered_shuffle_total"
  val ALLOCATED_SLOTS_TOTAL = "worker_allocated_slots_total"

  // push data
  val MASTER_PUSHDATA_DURATION = "worker_master_pushdata_duration_milliseconds"
  val SLAVE_PUSHDATA_DURATION = "worker_slave_pushdata_duration_milliseconds"
  val PUSHDATA_FAIL_TOTAL = "worker_pushdata_fail_total"

  // flush data
  val TAKE_BUFFER_DURATION = "worker_take_buffer_duration_milliseconds"
  val FLUSH_DATA_DURATION = "worker_flush_data_duration_milliseconds"

  // commit data
  val COMMIT_FILES_DURATION = "worker_commit_files_duration_milliseconds"

  // fetch data
  val OPEN_STREAM_DURATION = "worker_open_stream_duration_milliseconds"

  // sort data
  val SORT_DURATION = "worker_sort_duration_milliseconds"
  val SORTING_FILES = "worker_sorting_files_total"

  val FETCH_CHUNK_DURATION = "worker_fetch_chunk_duration_milliseconds"

  // memory
  val NETTY_MEMORY_USAGE = "worker_netty_memory_usage_bytes"
  val DISK_BUFFER_USAGE = "worker_disk_buffer_memory_usage_bytes"
  val SORTER_MEMORY = "worker_sorter_memory_usage_bytes"
  val PAUSE_PUSHDATA_TOTAL = "worker_pause_pushdata_total"
  val PAUSE_PUSHDATA_AND_REPLICATE_TOTAL = "worker_pause_pushdata_and_replicate_total"
}
