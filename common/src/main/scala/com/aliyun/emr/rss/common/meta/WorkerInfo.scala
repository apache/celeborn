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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.PbWorkerInfo
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.{ResourceConsumption, UserIdentifier}
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef
import com.aliyun.emr.rss.common.rpc.netty.NettyRpcEndpointRef
import com.aliyun.emr.rss.common.util.PbSerDeUtils

class WorkerInfo(
    val host: String,
    val rpcPort: Int,
    val pushPort: Int,
    val fetchPort: Int,
    val replicatePort: Int,
    val diskInfos: util.Map[String, DiskInfo],
    var endpoint: RpcEndpointRef) extends Serializable with Logging {
  var unknownDiskSlots = new java.util.HashMap[String, Integer]()
  var lastHeartbeat: Long = 0
  val userResourceConsumption = new ConcurrentHashMap[UserIdentifier, ResourceConsumption]()

  def this(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int, replicatePort: Int) {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      new util.HashMap[String, DiskInfo](),
      null)
  }

  def this(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      endpoint: RpcEndpointRef) {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      new util.HashMap[String, DiskInfo](),
      endpoint)
  }

  val allocationBuckets = new Array[Int](61)
  var bucketIndex = 0
  var bucketTime = System.currentTimeMillis()
  var bucketAllocations = 0
  0 until allocationBuckets.length foreach { case idx =>
    allocationBuckets(idx) = 0
  }

  def isActive: Boolean = {
    endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  def usedSlots(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.activeSlots).sum +
      unknownDiskSlots.values().asScala.map(_.intValue()).sum
  }

  def allocateSlots(shuffleKey: String, slotsPerDisk: util.Map[String, Integer]): Unit =
    this.synchronized {
      logDebug(s"shuffle $shuffleKey allocations $slotsPerDisk")
      var totalSlots = 0
      slotsPerDisk.asScala.foreach { case (disk, slots) =>
        if (!diskInfos.containsKey(disk)) {
          logDebug(s"Unknown disk $disk")
          if (unknownDiskSlots.containsKey(shuffleKey)) {
            unknownDiskSlots.put(shuffleKey, slots + unknownDiskSlots.get(shuffleKey))
          } else {
            unknownDiskSlots.put(shuffleKey, slots)
          }
        } else {
          diskInfos.get(disk).allocateSlots(shuffleKey, slots)
        }
        totalSlots += slots
      }

      val current = System.currentTimeMillis()
      if (current - bucketTime > 60 * 1000) {
        bucketIndex = (bucketIndex + 1) % allocationBuckets.length
        allocationBuckets(bucketIndex) = 0
        bucketTime = current
      }
      allocationBuckets(bucketIndex) = allocationBuckets(bucketIndex) + totalSlots
    }

  def releaseSlots(shuffleKey: String, slots: util.Map[String, Integer]): Unit = this.synchronized {
    slots.asScala.foreach { case (disk, slot) =>
      if (diskInfos.containsKey(disk)) {
        diskInfos.get(disk).releaseSlots(shuffleKey, slot)
      } else {
        if (unknownDiskSlots.containsKey(shuffleKey)) {
          unknownDiskSlots.put(shuffleKey, unknownDiskSlots.get(shuffleKey) - slot)
        }
      }
    }
  }

  def releaseSlots(shuffleKey: String): Unit = this.synchronized {
    diskInfos.asScala.foreach(_._2.releaseSlots(shuffleKey))
    unknownDiskSlots.remove(shuffleKey)
  }

  def getShuffleKeySet(): util.HashSet[String] = this.synchronized {
    val shuffleKeySet = new util.HashSet[String]()
    diskInfos.values().asScala.foreach { diskInfo =>
      shuffleKeySet.addAll(diskInfo.getShuffleKeySet())
    }
    shuffleKeySet
  }

  def allocationsInLastHour(): Int = this.synchronized {
    var total = 0
    1 to 60 foreach { case delta =>
      total += allocationBuckets((bucketIndex + delta) % allocationBuckets.length)
    }
    total
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
    diskInfos.asScala.exists { case (_, disk) => (disk.maxSlots - disk.activeSlots) > 0 }
  }

  def getTotalSlots(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.maxSlots).sum
  }

  def updateDiskMaxSlots(estimatedPartitionSize: Long): Unit = this.synchronized {
    diskInfos.asScala.foreach { case (_, disk) =>
      disk.maxSlots_$eq(disk.actualUsableSpace / estimatedPartitionSize)
    }
  }

  def totalAvailableSlots(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.availableSlots()).sum
  }

  def updateDiskInfos(
      newDiskInfos: java.util.Map[String, DiskInfo],
      estimatedPartitionSize: Long): Unit = this.synchronized {
    import scala.collection.JavaConverters._
    for (newDisk <- newDiskInfos.values().asScala) {
      val mountPoint: String = newDisk.mountPoint
      val curDisk = diskInfos.get(mountPoint)
      if (curDisk != null) {
        curDisk.actualUsableSpace_$eq(newDisk.actualUsableSpace)
        curDisk.activeSlots_$eq(Math.max(curDisk.activeSlots, newDisk.activeSlots))
        curDisk.avgFlushTime_$eq(newDisk.avgFlushTime)
        curDisk.maxSlots_$eq(curDisk.actualUsableSpace / estimatedPartitionSize)
        curDisk.setStatus(newDisk.status)
      } else {
        newDisk.maxSlots_$eq(newDisk.actualUsableSpace / estimatedPartitionSize)
        diskInfos.put(mountPoint, newDisk)
      }
    }

    val nonExistsMountPoints: java.util.Set[String] = new util.HashSet[String]
    nonExistsMountPoints.addAll(diskInfos.keySet)
    nonExistsMountPoints.removeAll(newDiskInfos.keySet)
    if (!nonExistsMountPoints.isEmpty) {
      for (nonExistsMountPoint <- nonExistsMountPoints.asScala) {
        diskInfos.remove(nonExistsMountPoint)
      }
    }
  }

  def updateUserResourceConsumption(consumption: util.Map[UserIdentifier, ResourceConsumption])
      : Unit = {
    userResourceConsumption.clear()
    userResourceConsumption.putAll(consumption)
  }

  override def toString(): String = {
    s"""
       |Host: $host
       |RpcPort: $rpcPort
       |PushPort: $pushPort
       |FetchPort: $fetchPort
       |ReplicatePort: $replicatePort
       |SlotsUsed: $usedSlots()
       |LastHeartbeat: $lastHeartbeat
       |Disks: $diskInfos
       |WorkerRef: $endpoint
       |""".stripMargin
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[WorkerInfo]
    host == other.host &&
    rpcPort == other.rpcPort &&
    pushPort == other.pushPort &&
    fetchPort == other.fetchPort &&
    replicatePort == other.replicatePort
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
    val disks =
      if (pbWorker.getDisksCount > 0) {
        pbWorker.getDisksList.asScala
          .map { pbDiskInfo => pbDiskInfo.getMountPoint -> PbSerDeUtils.fromPbDiskInfo(pbDiskInfo) }
          .toMap.asJava
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
      null)
  }

  def toPbWorkerInfo(workerInfo: WorkerInfo): PbWorkerInfo = {
    val disks = workerInfo.diskInfos.asScala.values
      .map { diskInfo => PbSerDeUtils.toPbDiskInfo(diskInfo) }
      .asJava
    PbWorkerInfo
      .newBuilder()
      .setHost(workerInfo.host)
      .setRpcPort(workerInfo.rpcPort)
      .setFetchPort(workerInfo.fetchPort)
      .setPushPort(workerInfo.pushPort)
      .setReplicatePort(workerInfo.replicatePort)
      .addAllDisks(disks)
      .build()
  }
}
