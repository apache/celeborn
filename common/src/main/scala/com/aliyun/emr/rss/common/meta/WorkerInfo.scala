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
import javax.annotation.concurrent.NotThreadSafe

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.StorageInfo
import com.aliyun.emr.rss.common.protocol.TransportMessages.{PbDiskInfo, PbWorkerInfo}
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef
import com.aliyun.emr.rss.common.rpc.netty.NettyRpcEndpointRef

class DiskInfo(
  val mountPoint: String,
  val usableSpace: Long,
  // avgFlushTime is nano seconds
  var avgFlushTime: Long,
  var activeSlots: Long) extends Serializable {
  var maxSlots: Long = 0
  lazy val shuffleAllocations = new util.HashMap[String, Integer]()

  def availableSlots(): Long = {
    maxSlots - activeSlots
  }

  def allocateSlots(shuffleKey: String, slots: Int): Unit = {
    val allocated = shuffleAllocations.getOrDefault(shuffleKey, 0)
    shuffleAllocations.put(shuffleKey, allocated + slots)
    activeSlots = activeSlots + slots
  }

  def releaseSlots(shuffleKey: String, slots: Int): Unit = {
    val allocated = shuffleAllocations.getOrDefault(shuffleKey, 0)
    activeSlots = activeSlots - slots
    if (allocated > slots) {
      shuffleAllocations.put(shuffleKey, allocated - slots)
    } else {
      shuffleAllocations.put(shuffleKey, 0)
    }
  }

  def releaseSlots(shuffleKey: String): Unit = {
    val allocated = shuffleAllocations.remove(shuffleKey)
    if (allocated != null) {
      activeSlots = activeSlots - allocated
    }
  }

  override def toString: String = s"DiskInfo(maxSlots: $maxSlots," +
    s" shuffleAllocations: $shuffleAllocations," +
    s" mountPoint: $mountPoint," +
    s" usableSpace: $usableSpace," +
    s" avgFlushTime: $avgFlushTime," +
    s" activeSlots: $activeSlots)"
}

class WorkerInfo(
    val host: String,
    val rpcPort: Int,
    val pushPort: Int,
    val fetchPort: Int,
    val replicatePort: Int,
    val disks: util.Map[String, DiskInfo],
    var endpoint: RpcEndpointRef) extends Serializable with Logging {
  var unknownDiskSlots = 0
  var lastHeartbeat: Long = 0

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

  def isActive: Boolean = {
    endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  def usedSlots(): Long = this.synchronized {
    disks.asScala.map(_._2.activeSlots).sum + unknownDiskSlots
  }

  def allocateSlots(shuffleKey: String, slotsPerDisk: util.Map[String, Integer]): Unit =
    this.synchronized {
      logDebug(s"shuffle $shuffleKey allocations $slotsPerDisk")
      slotsPerDisk.asScala.foreach { case (disk, slots) =>
        if (!disks.containsKey(disk)) {
          logDebug(s"Unknown disk ${disk}")
          unknownDiskSlots = unknownDiskSlots + slots
        } else {
          disks.get(disk).allocateSlots(shuffleKey, slots)
        }
      }
    }

  def releaseSlots(shuffleKey: String, slots: util.Map[String, Integer]): Unit = this.synchronized {
    slots.asScala.foreach { case (disk, slot) =>
      if (disks.containsKey(disk)) {
        disks.get(disk).releaseSlots(shuffleKey, slot)
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

  def slotAvailable(): Boolean = this.synchronized {
    disks.asScala.exists { case (_, disk) => (disk.maxSlots - disk.activeSlots) > 0 }
  }

  def getTotalSlots(): Long = this.synchronized {
    disks.asScala.map(_._2.maxSlots).sum
  }

  def updateDiskMaxSlots(estimatedPartitionSize: Long): Unit = this.synchronized {
    disks.asScala.foreach { case (_, disk) =>
      disk.maxSlots_$eq(disk.usableSpace / estimatedPartitionSize)
    }
  }

  def totalAvailableSlots(): Long = this.synchronized {
    disks.asScala.map(_._2.availableSlots()).sum
  }

  def updateDiskInfos(newDiskInfos: java.util.Map[String, DiskInfo],
      estimatedPartitionSize: Long): Unit = this.synchronized {
    import scala.collection.JavaConverters._
    for (diskInfoEntry <- newDiskInfos.entrySet.asScala) {
      val mountPoint: String = diskInfoEntry.getKey
      if (disks.containsKey(mountPoint)) {
        disks.get(mountPoint).activeSlots_$eq(Math.max(disks.get(mountPoint).activeSlots,
          disks.get(mountPoint).activeSlots))
        disks.get(mountPoint).avgFlushTime_$eq(disks.get(mountPoint).avgFlushTime)
      }
      else {
        val diskInfo: DiskInfo = diskInfoEntry.getValue
        diskInfo.maxSlots_$eq(diskInfo.usableSpace / estimatedPartitionSize)
        disks.put(mountPoint, diskInfo)
      }
    }

    val nonExistsMountPoints: java.util.Set[String] = new util.HashSet[String]
    nonExistsMountPoints.addAll(disks.keySet)
    nonExistsMountPoints.removeAll(newDiskInfos.keySet)
    if (!(nonExistsMountPoints.isEmpty)) {
      for (nonExistsMountPoint <- nonExistsMountPoints.asScala) {
        disks.remove(nonExistsMountPoint)
      }
    }
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
       |Disks: $disks
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
            item._2.getAvgFlushTime,
            item._2.getUsedSlots
          )
        ).asJava
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
            .setAvgFlushTime(item._2.avgFlushTime)
            .setUsedSlots(item._2.activeSlots)
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
