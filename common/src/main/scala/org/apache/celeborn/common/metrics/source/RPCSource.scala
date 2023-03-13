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

package org.apache.celeborn.common.metrics.source

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.protocol.{ChunkFetchRequest, OpenStream, PushData, PushMergedData}
import org.apache.celeborn.common.protocol.{PbRegisterWorker, PbUnregisterShuffle}
import org.apache.celeborn.common.protocol.message.ControlMessages._

class RPCSource(conf: CelebornConf, role: String) extends AbstractSource(conf, role) {
  override val sourceName = "rpc"

  import RPCSource._

  // Worker RPC
  addCounter(RPC_RESERVE_SLOTS_NUM)
  addCounter(RPC_RESERVE_SLOTS_SIZE)
  addCounter(RPC_COMMIT_FILES_NUM)
  addCounter(RPC_COMMIT_FILES_SIZE)
  addCounter(RPC_DESTROY_NUM)
  addCounter(RPC_DESTROY_SIZE)
  addCounter(RPC_PUSH_DATA_NUM)
  addCounter(RPC_PUSH_DATA_SIZE)
  addCounter(RPC_PUSH_MERGED_DATA_NUM)
  addCounter(RPC_PUSH_MERGED_DATA_SIZE)
  addCounter(RPC_OPEN_STREAM_NUM)
  addCounter(RPC_CHUNK_FETCH_REQUEST_NUM)

  // Master RPC
  addCounter(RPC_HEARTBEAT_FROM_APPLICATION_NUM)
  addCounter(RPC_HEARTBEAT_FROM_WORKER_NUM)
  addCounter(RPC_REGISTER_WORKER_NUM)
  addCounter(RPC_REQUEST_SLOTS_NUM)
  addCounter(RPC_RELEASE_SLOTS_NUM)
  addCounter(RPC_RELEASE_SLOTS_SIZE)
  addCounter(RPC_UNREGISTER_SHUFFLE_NUM)
  addCounter(RPC_GET_BLACKLIST_NUM)
  addCounter(RPC_REPORT_WORKER_UNAVAILABLE_NUM)
  addCounter(RPC_REPORT_UNAVAILABLE_SIZE)
  addCounter(RPC_CHECK_QUOTA_NUM)

  def updateMessageMetrics(message: Any, messageLen: Long): Unit = {
    message match {
      case _: ReserveSlots =>
        incCounter(RPC_RESERVE_SLOTS_NUM)
        incCounter(RPC_RESERVE_SLOTS_SIZE, messageLen)
      case _: CommitFiles =>
        incCounter(RPC_COMMIT_FILES_NUM)
        incCounter(RPC_COMMIT_FILES_SIZE, messageLen)
      case _: Destroy =>
        incCounter(RPC_DESTROY_NUM)
        incCounter(RPC_DESTROY_SIZE, messageLen)
      case _: PushData =>
        incCounter(RPC_PUSH_DATA_NUM)
        incCounter(RPC_PUSH_DATA_SIZE, messageLen)
      case _: PushMergedData =>
        incCounter(RPC_PUSH_MERGED_DATA_NUM)
        incCounter(RPC_PUSH_MERGED_DATA_SIZE, messageLen)
      case _: ChunkFetchRequest =>
        incCounter(RPC_CHUNK_FETCH_REQUEST_NUM)
      case _: OpenStream =>
        incCounter(RPC_OPEN_STREAM_NUM)
      case _: HeartbeatFromApplication =>
        incCounter(RPC_HEARTBEAT_FROM_APPLICATION_NUM)
      case _: HeartbeatFromWorker =>
        incCounter(RPC_HEARTBEAT_FROM_WORKER_NUM)
      case _: PbRegisterWorker =>
        incCounter(RPC_REGISTER_WORKER_NUM)
      case _: RequestSlots =>
        incCounter(RPC_REQUEST_SLOTS_NUM)
      case _: ReleaseSlots =>
        incCounter(RPC_RELEASE_SLOTS_NUM)
        incCounter(RPC_RELEASE_SLOTS_SIZE, messageLen)
      case _: PbUnregisterShuffle =>
        incCounter(RPC_UNREGISTER_SHUFFLE_NUM)
      case _: GetBlacklist =>
        incCounter(RPC_GET_BLACKLIST_NUM)
      case _: ReportWorkerUnavailable =>
        incCounter(RPC_REPORT_WORKER_UNAVAILABLE_NUM)
        incCounter(RPC_REPORT_UNAVAILABLE_SIZE, messageLen)
      case CheckQuota =>
        incCounter(RPC_CHECK_QUOTA_NUM)
      case _ => // Do nothing
    }
  }
}

object RPCSource {
  // Worker RPC
  val RPC_RESERVE_SLOTS_NUM = "RPCReserveSlotsNum"
  val RPC_RESERVE_SLOTS_SIZE = "RPCReserveSlotsSize"
  val RPC_COMMIT_FILES_NUM = "RPCCommitFilesNum"
  val RPC_COMMIT_FILES_SIZE = "RPCCommitFilesSize"
  val RPC_DESTROY_NUM = "RPCDestroyNum"
  val RPC_DESTROY_SIZE = "RPCDestroySize"
  val RPC_PUSH_DATA_NUM = "RPCPushDataNum"
  val RPC_PUSH_DATA_SIZE = "RPCPushDataSize"
  val RPC_PUSH_MERGED_DATA_NUM = "RPCPushMergedDataNum"
  val RPC_PUSH_MERGED_DATA_SIZE = "RPCPushMergedDataSize"
  val RPC_OPEN_STREAM_NUM = "RPCOpenStreamNum"
  val RPC_CHUNK_FETCH_REQUEST_NUM = "RPCChunkFetchRequestNum"

  // Master RPC
  val RPC_HEARTBEAT_FROM_APPLICATION_NUM = "RPCHeartbeatFromApplicationNum"
  val RPC_HEARTBEAT_FROM_WORKER_NUM = "RPCHeartbeatFromWorkerNum"
  val RPC_REGISTER_WORKER_NUM = "RPCRegisterWorkerNum"
  val RPC_REQUEST_SLOTS_NUM = "RPCRequestSlotsNum"
  val RPC_RELEASE_SLOTS_NUM = "RPCReleaseSlotsNum"
  val RPC_RELEASE_SLOTS_SIZE = "RPCReleaseSlotsSize"
  val RPC_UNREGISTER_SHUFFLE_NUM = "RPCUnregisterShuffleNum"
  val RPC_GET_BLACKLIST_NUM = "RPCGetBlacklistNum"
  val RPC_REPORT_WORKER_UNAVAILABLE_NUM = "RPCReportWorkerUnavailableNum"
  val RPC_REPORT_UNAVAILABLE_SIZE = "RPCReportWorkerUnavailableSize"
  val RPC_CHECK_QUOTA_NUM = "RPCCheckQuotaNum"
}
