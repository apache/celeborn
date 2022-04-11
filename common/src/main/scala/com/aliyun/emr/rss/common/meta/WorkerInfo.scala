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

package com.aliyun.emr.rss.common.meta

import java.util
import java.util.Objects

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.TransportMessages.PbWorkerInfo
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef
import com.aliyun.emr.rss.common.rpc.netty.NettyRpcEndpointRef

class WorkerInfo(
    val host: String,
    val rpcPort: Int,
    val pushPort: Int,
    val fetchPort: Int,
    val replicatePort: Int,
    var numSlots: Int,
    var endpoint: RpcEndpointRef) extends Serializable with Logging {

  private var slotsUsed: Int = 0
  var lastHeartbeat: Long = 0

  // key: shuffleKey   value: slots allocated for the shuffle
  lazy val shuffleSlots = new util.HashMap[String, Int]()

  def this(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int, replicatePort: Int) {
    this(host, rpcPort, pushPort, fetchPort, replicatePort, -1, null)
  }

  def isActive: Boolean = {
    endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  def setNumSlots(numSlots: Int): Unit = this.synchronized {
    this.numSlots = numSlots
  }

  def usedSlots(): Int = this.synchronized {
    slotsUsed
  }

  def freeSlots(): Int = this.synchronized {
    numSlots - slotsUsed
  }

  def slotAvailable(): Boolean = this.synchronized {
    numSlots > slotsUsed
  }

  def allocateSlots(shuffleKey: String, slots: Int): Unit = this.synchronized {
    val allocated = shuffleSlots.getOrDefault(shuffleKey, 0)
    shuffleSlots.put(shuffleKey, allocated + slots)
    slotsUsed += slots
  }

  def releaseSlots(shuffleKey: String, slots: Int): Unit = this.synchronized {
    val allocated = shuffleSlots.getOrDefault(shuffleKey, 0)
    if (allocated < slots) {
      logWarning(s"Worker: $readableAddress for shuffle key: $shuffleKey " +
        s"allocated($allocated) is smaller than to release($slots)!")
    } else {
      shuffleSlots.put(shuffleKey, allocated - slots)
      slotsUsed -= slots
    }
  }

  def releaseSlots(shuffleKey: String): Int = this.synchronized {
    val allocated = shuffleSlots.getOrDefault(shuffleKey, 0)
    shuffleSlots.remove(shuffleKey)
    slotsUsed -= allocated
    allocated
  }

  def clearAll(): Unit = this.synchronized {
    slotsUsed = 0
  }

  def hasSameInfoWith(other: WorkerInfo): Boolean = {
    numSlots == other.numSlots &&
      slotsUsed == other.slotsUsed &&
      rpcPort == other.rpcPort &&
      pushPort == other.pushPort &&
      host == other.host &&
      fetchPort == other.fetchPort &&
      replicatePort == other.replicatePort
  }

  def setupEndpoint(endpointRef: RpcEndpointRef): Unit = {
    if (this.endpoint == null) {
      this.endpoint = endpointRef
    }
  }

  def readableAddress(): String = {
    s"Host:$host:RpcPort:$rpcPort:PushPort:$pushPort:" +
      s"FetchPort:$fetchPort:ReplicatePort:$replicatePort"
  }

  def toUniqueId(): String = {
    s"$host:$rpcPort:$pushPort:$fetchPort:$replicatePort"
  }

  override def toString(): String = {
    s"""
       |Host: $host
       |RpcPort: $rpcPort
       |PushPort: $pushPort
       |FetchPort: $fetchPort
       |ReplicatePort: $replicatePort
       |TotalSlots: $numSlots
       |SlotsUsed: $slotsUsed
       |SlotsAvailable: ${numSlots - slotsUsed}
       |LastHeartBeat: $lastHeartbeat
       |WorkerRef: $endpoint
       |""".stripMargin
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[WorkerInfo]
    host == other.host &&
      rpcPort == other.rpcPort &&
      pushPort == other.pushPort &&
      fetchPort == other.fetchPort
  }

  override def hashCode(): Int = {
    Objects.hashCode(host, rpcPort, pushPort, fetchPort)
  }
}

object WorkerInfo {
  private val SPLIT: String = "-"

  def encodeToPbStr(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int,
    replicatePort: Int, allocatedSize: Int): String = {
    s"$host$SPLIT$rpcPort$SPLIT$pushPort$SPLIT$fetchPort$SPLIT$replicatePort$SPLIT$allocatedSize"
  }

  def decodeFromPbMessage(pbStrList: util.List[String]): util.HashMap[WorkerInfo, Integer] = {
    val map = new util.HashMap[WorkerInfo, Integer]()
    import scala.collection.JavaConverters._
    pbStrList.asScala.foreach { str =>
      val splits = str.split(SPLIT)
      map.put(new WorkerInfo(splits(0), splits(1).toInt, splits(2).toInt,
        splits(3).toInt, splits(4).toInt, -1, null), splits(5).toInt)
    }
    map
  }

  def fromUniqueId(id: String): WorkerInfo = {
    val Array(host, rpcPort, pushPort, fetchPort, replicatePort) = id.split(":")
    new WorkerInfo(host, rpcPort.toInt, pushPort.toInt, fetchPort.toInt, replicatePort.toInt)
  }

  def fromPbWorkerInfo(pbWorker: PbWorkerInfo): WorkerInfo = {
    new WorkerInfo(pbWorker.getHost, pbWorker.getRpcPort, pbWorker.getPushPort,
      pbWorker.getFetchPort, pbWorker.getReplicatePort, pbWorker.getNumSlots, null)
  }

  def toPbWorkerInfo(workerInfo: WorkerInfo): PbWorkerInfo = {
    PbWorkerInfo.newBuilder()
      .setHost(workerInfo.host)
      .setRpcPort(workerInfo.rpcPort)
      .setFetchPort(workerInfo.fetchPort)
      .setPushPort(workerInfo.pushPort)
      .setReplicatePort(workerInfo.replicatePort)
      .setNumSlots(workerInfo.numSlots)
      .build()
  }
}
