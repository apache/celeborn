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

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.TransportMessages.{PbDiskInfo, PbWorkerInfo}
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef

class DiskInfo(
    val mountPoint: String,
    val usableSpace: Long,
    val flushTime: Double,
    var activeWriters: Long
) extends Serializable {
  var maxWriters: Long = 0
  lazy val diskRelatedWriters = new util.HashMap[String, Integer]()

  def availableSlots(): Long = {
    maxWriters - activeWriters
  }

  def allocateSlots(shuffleKey: String, slots: Int): Unit = {
    val allocated = diskRelatedWriters.getOrDefault(shuffleKey, 0)
    diskRelatedWriters.put(shuffleKey, allocated + slots)
    activeWriters = activeWriters + slots
  }

  def releaseSlots(shuffleKey: String, slots: Int): Unit = {
    val allocated = diskRelatedWriters.getOrDefault(shuffleKey, 0)
    if (allocated > slots) {
      diskRelatedWriters.put(shuffleKey, allocated - slots)
      activeWriters = activeWriters - slots
    } else {
      activeWriters = 0
      diskRelatedWriters.put(shuffleKey, 0)
    }
  }

  def releaseSlots(shuffleKey: String): Unit = {
    val allocated = diskRelatedWriters.remove(shuffleKey)
    if (allocated != null) {
      activeWriters = activeWriters - allocated
    }
  }
}

class WorkerInfo(
    val host: String,
    val rpcPort: Int,
    val pushPort: Int,
    val fetchPort: Int,
    val replicatePort: Int,
    val disks: util.Map[String, DiskInfo],
    var endpoint: RpcEndpointRef
) extends Serializable
  with Logging {

  var lastHeartbeat: Long = 0

  def this(workerInfo: WorkerInfo) {
    this(
      workerInfo.host,
      workerInfo.rpcPort,
      workerInfo.pushPort,
      workerInfo.fetchPort,
      workerInfo.replicatePort,
      workerInfo.disks,
      workerInfo.endpoint
    )
  }
  def this(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int, replicatePort: Int) {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      new util.HashMap[String, DiskInfo](),
      null
    )
  }

  def this(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      endpoint: RpcEndpointRef
  ) {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      new util.HashMap[String, DiskInfo](),
      endpoint
    )
  }

  def usedSlots(): Long = this.synchronized {
    disks.asScala.map(_._2.activeWriters).sum
  }

  def allocateSlots(shuffleKey: String, slotsDistributions: util.Map[String, Integer]): Unit =
    this.synchronized {
      slotsDistributions.asScala.foreach(it => {
        disks.get(it._1).allocateSlots(shuffleKey, it._2)
      })
    }

  def releaseSlots(shuffleKey: String, slots: util.Map[String, Integer]): Unit = this.synchronized {
    slots.asScala.foreach { diskSlots =>
      if (disks.containsKey(diskSlots)) {
        // disk hint is DEFAULT means that this location has no file
        disks.get(diskSlots).releaseSlots(shuffleKey, diskSlots._2)
      }
    }
  }

  def releaseSlots(shuffleKey: String): Unit = this.synchronized {
    disks.asScala.foreach(_._2.releaseSlots(shuffleKey))
  }

  def hasSameInfoWith(other: WorkerInfo): Boolean = {
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
    |SlotsUsed: $usedSlots()
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

  def encodeToPbStr(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      allocations: util.Map[String, Integer]
  ): String = {
    val allocationsStrBuf = new StringBuilder()
    allocations.asScala.foreach(allocate => {
      allocationsStrBuf.append(SPLIT)
      allocationsStrBuf.append(allocate._1)
      allocationsStrBuf.append(SPLIT)
      allocationsStrBuf.append(allocate._2)
    })

    s"$host$SPLIT$rpcPort$SPLIT$pushPort$SPLIT$fetchPort$SPLIT" +
      s"$replicatePort$SPLIT${allocations.size}" +
      s"${allocationsStrBuf.toString()}"
  }

  def decodeFromPbMessage(
      pbStrList: util.List[String]
  ): util.HashMap[WorkerInfo, util.HashMap[String, Integer]] = {
    val map = new util.HashMap[WorkerInfo, util.HashMap[String, Integer]]()
    import scala.collection.JavaConverters._
    val allocationsMap = new util.HashMap[String, Integer]()
    pbStrList.asScala.foreach { str =>
      val splits = str.split(SPLIT)
      val allocationsMapSize = splits(5).toInt
      if (allocationsMapSize > 0) {
        var index = 5
        while (index < splits.size - 1) {
          val mountPoint = splits(index + 1)
          val slots = splits(index + 2).toInt
          allocationsMap.put(mountPoint, slots)
          index = index + 2
        }
      } else {
        new util.HashMap[String, DiskInfo]()
      }
      map.put(
        new WorkerInfo(
          splits(0),
          splits(1).toInt,
          splits(2).toInt,
          splits(3).toInt,
          splits(4).toInt,
          null,
          null
        ),
        allocationsMap
      )
    }
    map
  }

  def fromUniqueId(id: String): WorkerInfo = {
    val Array(host, rpcPort, pushPort, fetchPort, replicatePort) = id.split(":")
    new WorkerInfo(host, rpcPort.toInt, pushPort.toInt, fetchPort.toInt, replicatePort.toInt)
  }

  def fromPbWorkerInfo(pbWorker: PbWorkerInfo): WorkerInfo = {
    val disks = if (pbWorker.getDisksCount > 0) {
      pbWorker.getDisksMap.asScala
        .map(item =>
          item._1 -> new DiskInfo(
            item._1,
            item._2.getUsableSpace,
            item._2.getFlushTime,
            item._2.getUsedSlots
          )
        )
        .asJava
    } else {
      new util.HashMap[String, DiskInfo]()
    }

    new WorkerInfo(
      pbWorker.getHost,
      pbWorker.getRpcPort,
      pbWorker.getPushPort,
      pbWorker.getFetchPort,
      pbWorker.getReplicatePort,
      disks,
      null
    )
  }

  def toPbWorkerInfo(workerInfo: WorkerInfo): PbWorkerInfo = {
    val disks = workerInfo.disks.asScala
      .map(item =>
        item._1 ->
          PbDiskInfo
            .newBuilder()
            .setUsableSpace(item._2.usableSpace)
            .setFlushTime(item._2.flushTime)
            .setUsedSlots(item._2.activeWriters)
            .build()
      )
      .asJava
    PbWorkerInfo
      .newBuilder()
      .setHost(workerInfo.host)
      .setRpcPort(workerInfo.rpcPort)
      .setFetchPort(workerInfo.fetchPort)
      .setPushPort(workerInfo.pushPort)
      .setReplicatePort(workerInfo.replicatePort)
      .putAllDisks(disks)
      .build()
  }
}
