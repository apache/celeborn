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

package com.aliyun.emr.rss.common.metrics.source

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.network.protocol.{ChunkFetchRequest, PushData, PushMergedData}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._

class RPCSource(rssConf: RssConf, role: String) extends AbstractSource(rssConf, role) {
  override val sourceName = "rpc"

  import RPCSource._

  // Worker RPC
  addCounter(RPC_RESERVE_SLOTS_REQUEST_TOTAL)
  addCounter(RPC_RESERVE_SLOTS_REQUEST_SIZE_TOTAL)
  addCounter(RPC_COMMIT_FILES_REQUEST_TOTAL)
  addCounter(RPC_COMMIT_FILES_REQUEST_SIZE_TOTAL)
  addCounter(RPC_DESTROY_REQUEST_TOTAL)
  addCounter(RPC_DESTROY_REQUEST_SIZE_TOTAL)
  addCounter(RPC_PUSH_DATA_REQUEST_TOTAL)
  addCounter(RPC_PUSH_DATA_REQUEST_SIZE_TOTAL)
  addCounter(RPC_PUSH_MERGED_DATA_REQUEST_TOTAL)
  addCounter(RPC_PUSH_MERGED_DATA_REQUEST_SIZE_TOTAL)
  addCounter(RPC_CHUNK_FETCH_REQUEST_TOTAL)

  // Master RPC
  addCounter(RPC_APPLICATION_HEARTBEAT_REQUEST_TOTAL)
  addCounter(RPC_WORKER_HEARTBEAT_REQUEST_TOTAL)
  addCounter(RPC_REGISTERED_SHUFFLE_REQUEST_TOTAL)
  addCounter(RPC_REQUEST_SLOTS_REQUEST_TOTAL)
  addCounter(RPC_RELEASE_SLOTS_REQUEST_TOTAL)
  addCounter(RPC_RELEASE_SLOTS_REQUEST_SIZE_TOTAL)
  addCounter(RPC_UNREGISTER_SHUFFLE_REQUEST_TOTAL)
  addCounter(RPC_GET_BLACKLIST_REQUEST_TOTAL)
  addCounter(RPC_REPORT_WORKER_FAILURE_REQUEST_TOTAL)
  addCounter(RPC_REPORT_WORKER_FAILURE_REQUEST_SIZE_TOTAL)
  addCounter(RPC_CHECK_ALIVE_REQUEST_TOTAL)

  def updateMessageMetrics(message: Any, messageLen: Long): Unit = {
    message match {
      case _: ReserveSlots =>
        incCounter(RPC_RESERVE_SLOTS_REQUEST_TOTAL)
        incCounter(RPC_RESERVE_SLOTS_REQUEST_SIZE_TOTAL, messageLen)
      case _: CommitFiles =>
        incCounter(RPC_COMMIT_FILES_REQUEST_TOTAL)
        incCounter(RPC_COMMIT_FILES_REQUEST_SIZE_TOTAL, messageLen)
      case _: Destroy =>
        incCounter(RPC_DESTROY_REQUEST_TOTAL)
        incCounter(RPC_DESTROY_REQUEST_SIZE_TOTAL, messageLen)
      case _: PushData =>
        incCounter(RPC_PUSH_DATA_REQUEST_TOTAL)
        incCounter(RPC_PUSH_DATA_REQUEST_SIZE_TOTAL, messageLen)
      case _: PushMergedData =>
        incCounter(RPC_PUSH_MERGED_DATA_REQUEST_TOTAL)
        incCounter(RPC_PUSH_MERGED_DATA_REQUEST_SIZE_TOTAL, messageLen)
      case _: ChunkFetchRequest =>
        incCounter(RPC_CHUNK_FETCH_REQUEST_TOTAL)
      case _: HeartbeatFromApplication =>
        incCounter(RPC_APPLICATION_HEARTBEAT_REQUEST_TOTAL)
      case _: HeartbeatFromWorker =>
        incCounter(RPC_WORKER_HEARTBEAT_REQUEST_TOTAL)
      case _: RegisterWorker =>
        incCounter(RPC_REGISTERED_SHUFFLE_REQUEST_TOTAL)
      case _: RequestSlots =>
        incCounter(RPC_REQUEST_SLOTS_REQUEST_TOTAL)
      case _: ReleaseSlots =>
        incCounter(RPC_RELEASE_SLOTS_REQUEST_TOTAL)
        incCounter(RPC_RELEASE_SLOTS_REQUEST_SIZE_TOTAL, messageLen)
      case _: UnregisterShuffle =>
        incCounter(RPC_UNREGISTER_SHUFFLE_REQUEST_TOTAL)
      case _: GetBlacklist =>
        incCounter(RPC_GET_BLACKLIST_REQUEST_TOTAL)
      case _: ReportWorkerFailure =>
        incCounter(RPC_REPORT_WORKER_FAILURE_REQUEST_TOTAL)
        incCounter(RPC_REPORT_WORKER_FAILURE_REQUEST_SIZE_TOTAL, messageLen)
      case CheckAlive =>
        incCounter(RPC_CHECK_ALIVE_REQUEST_TOTAL)
      case _ => // Do nothing
    }
  }
}

object RPCSource {
  // Worker RPC
  val RPC_RESERVE_SLOTS_REQUEST_TOTAL = "rpc_reserve_slots_request_total"
  val RPC_RESERVE_SLOTS_REQUEST_SIZE_TOTAL = "rpc_reserve_slots_request_size_total"
  val RPC_COMMIT_FILES_REQUEST_TOTAL = "rpc_commit_files_request_total"
  val RPC_COMMIT_FILES_REQUEST_SIZE_TOTAL = "rpc_commit_files_request_size_total"
  val RPC_DESTROY_REQUEST_TOTAL = "rpc_destroy_request_total"
  val RPC_DESTROY_REQUEST_SIZE_TOTAL = "rpc_destroy_request_size_total"
  val RPC_PUSH_DATA_REQUEST_TOTAL = "rpc_push_data_request_total"
  val RPC_PUSH_DATA_REQUEST_SIZE_TOTAL = "rpc_push_data_request_siz_total"
  val RPC_PUSH_MERGED_DATA_REQUEST_TOTAL = "rpc_push_merged_data_request_total"
  val RPC_PUSH_MERGED_DATA_REQUEST_SIZE_TOTAL = "rpc_push_merged_data_request_size_total"
  val RPC_CHUNK_FETCH_REQUEST_TOTAL = "rpc_chunk_fetch_request_total"

  // Master RPC
  val RPC_APPLICATION_HEARTBEAT_REQUEST_TOTAL = "rpc_application_heartbeat_request_total"
  val RPC_WORKER_HEARTBEAT_REQUEST_TOTAL = "rpc_worker_heartbeat_request_total"
  val RPC_REGISTERED_SHUFFLE_REQUEST_TOTAL = "rpc_registered_shuffle_request_total"
  val RPC_REQUEST_SLOTS_REQUEST_TOTAL = "rpc_request_slots_request_total"
  val RPC_RELEASE_SLOTS_REQUEST_TOTAL = "rpc_release_slots_request_total"
  val RPC_RELEASE_SLOTS_REQUEST_SIZE_TOTAL = "rpc_release_slots_request_size_total"
  val RPC_UNREGISTER_SHUFFLE_REQUEST_TOTAL = "rpc_unregister_shuffle_request_total"
  val RPC_GET_BLACKLIST_REQUEST_TOTAL = "rpc_get_blacklist_request_total"
  val RPC_REPORT_WORKER_FAILURE_REQUEST_TOTAL = "rpc_report_worker_failure_request_total"
  val RPC_REPORT_WORKER_FAILURE_REQUEST_SIZE_TOTAL = "rpc_report_worker_failure_request_size_total"
  val RPC_CHECK_ALIVE_REQUEST_TOTAL = "rpc_check_alive_request_total"
}
