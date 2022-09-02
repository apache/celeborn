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
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.network.protocol.{ChunkFetchRequest, PushData, PushMergedData}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.{CommitFiles, Destroy, ReserveSlots}

class RPCSource(rssConf: RssConf)
  extends AbstractSource(rssConf, MetricsSystem.ROLE_WOKRER) with Logging {
  override val sourceName = "rpc"

  import RPCSource._

  // RPC
  addCounter(RPCReserveSlotsNum)
  addCounter(RPCReserveSlotsSize)
  addCounter(RPCCommitFilesNum)
  addCounter(RPCCommitFilesSize)
  addCounter(RPCDestroyNum)
  addCounter(RPCDestroySize)
  addCounter(RPCPushDataNum)
  addCounter(RPCPushDataSize)
  addCounter(RPCPushMergedDataNum)
  addCounter(RPCPushMergedDataSize)
  addCounter(RPCChunkFetchRequestNum)

  def updateMessageMetrics(message: Any, messageLen: Long): Unit = {
    message match {
      case _: ReserveSlots =>
        incCounter(RPCReserveSlotsNum)
        incCounter(RPCReserveSlotsSize, messageLen)
      case _: CommitFiles =>
        incCounter(RPCCommitFilesNum)
        incCounter(RPCCommitFilesSize, messageLen)
      case _: Destroy =>
        incCounter(RPCDestroyNum)
        incCounter(RPCDestroySize, messageLen)
      case _: PushData =>
        incCounter(RPCPushDataNum)
        incCounter(RPCPushDataSize, messageLen)
      case _: PushMergedData =>
        incCounter(RPCPushMergedDataNum)
        incCounter(RPCPushMergedDataSize, messageLen)
      case _: ChunkFetchRequest =>
        incCounter(RPCChunkFetchRequestNum)
      case _ => // Do nothing
    }
  }
}

object RPCSource {
  // RPC
  val RPCReserveSlotsNum = "RPCReserveSlotsNum"
  val RPCReserveSlotsSize = "RPCReserveSlotsSize"
  val RPCCommitFilesNum = "RPCCommitFilesNum"
  val RPCCommitFilesSize = "RPCCommitFilesSize"
  val RPCDestroyNum = "RPCDestroyNum"
  val RPCDestroySize = "RPCDestroySize"
  val RPCPushDataNum = "RPCPushDataNum"
  val RPCPushDataSize = "RPCPushDataSize"
  val RPCPushMergedDataNum = "RPCPushMergedDataNum"
  val RPCPushMergedDataSize = "RPCPushMergedDataSize"
  val RPCChunkFetchRequestNum = "RPCChunkFetchRequestNum"
}
