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

package org.apache.celeborn.common.meta

import java.io.File
import java.util
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.slf4j.LoggerFactory

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.util.{JavaUtils, Utils}
import org.apache.celeborn.common.util.Utils.runCommand

class DiskInfo(
    val mountPoint: String,
    var actualUsableSpace: Long,
    var avgFlushTime: Long, // in nano seconds
    var avgFetchTime: Long, // in nano seconds
    var activeSlots: Long,
    val dirs: List[File],
    val deviceInfo: DeviceInfo) extends Serializable with Logging {

  def this(
      mountPoint: String,
      usableSpace: Long,
      avgFlushTime: Long,
      avgFetchTime: Long,
      activeSlots: Long) = {
    this(mountPoint, usableSpace, avgFlushTime, avgFetchTime, activeSlots, List.empty, null)
  }

  def this(
      mountPoint: String,
      usableSpace: Long,
      avgFlushTime: Long,
      avgFetchTime: Long,
      activeSlots: Long,
      storageType: StorageInfo.Type) = {
    this(mountPoint, usableSpace, avgFlushTime, avgFetchTime, activeSlots, List.empty, null)
    this.storageType = storageType
  }

  def this(
      mountPoint: String,
      dirs: List[File],
      deviceInfo: DeviceInfo,
      conf: CelebornConf) = {
    this(mountPoint, 0, 0, 0, 0, dirs, deviceInfo)
    flushTimeMetrics =
      new TimeWindow(
        conf.workerDiskTimeSlidingWindowSize,
        conf.workerDiskTimeSlidingWindowMinFlushCount)
    fetchTimeMetrics =
      new TimeWindow(
        conf.workerDiskTimeSlidingWindowSize,
        conf.workerDiskTimeSlidingWindowMinFetchCount)
  }

  var flushTimeMetrics: TimeWindow = _
  var fetchTimeMetrics: TimeWindow = _
  var status: DiskStatus = DiskStatus.HEALTHY
  var threadCount = 1
  var configuredUsableSpace = 0L
  var storageType: StorageInfo.Type = StorageInfo.Type.SSD
  var maxSlots: Long = 0
  lazy val shuffleAllocations = new util.HashMap[String, Integer]()
  lazy val applicationAllocations = new util.HashMap[String, Integer]()

  def setStorageType(storageType: StorageInfo.Type) = {
    this.storageType = storageType
  }

  def setStatus(status: DiskStatus): this.type = this.synchronized {
    this.status = status
    this
  }

  def setUsableSpace(usableSpace: Long): this.type = this.synchronized {
    this.actualUsableSpace = usableSpace
    this
  }

  def updateFlushTime(): Unit = {
    avgFlushTime = flushTimeMetrics.getAverage()
  }

  def updateFetchTime(): Unit = {
    avgFetchTime = fetchTimeMetrics.getAverage()
  }

  /**
   * Returns the available slots of the disk calculated by maxSlots minus activeSlots.
   * Returns zero for the negative slots calculated.
   *
   * <b>Note:</b>`maxSlots` is calculated by actualUsableSpace divided estimatedPartitionSize.
   * Meanwhile, `activeSlots` include slots reserved.
   *
   * @return the available slots of the disk.
   */
  def availableSlots(): Long = this.synchronized {
    math.max(maxSlots - activeSlots, 0L)
  }

  def allocateSlots(shuffleKey: String, slots: Int): Unit = this.synchronized {
    val applicationId = Utils.splitShuffleKey(shuffleKey)._1
    val shuffleAllocated = shuffleAllocations.getOrDefault(shuffleKey, 0)
    val applicationAllocated = applicationAllocations.getOrDefault(applicationId, 0)
    shuffleAllocations.put(shuffleKey, shuffleAllocated + slots)
    applicationAllocations.put(applicationId, applicationAllocated + slots)
    activeSlots = activeSlots + slots
  }

  def releaseSlots(shuffleKey: String, slots: Int): Unit = this.synchronized {
    val applicationId = Utils.splitShuffleKey(shuffleKey)._1
    val shuffleAllocated = shuffleAllocations.getOrDefault(shuffleKey, 0)
    val applicationAllocated = applicationAllocations.getOrDefault(applicationId, 0)
    if (shuffleAllocated < slots) {
      logError(s"allocated $shuffleAllocated is less than to release $slots !")
    } else {
      shuffleAllocations.put(shuffleKey, shuffleAllocated - slots)
      applicationAllocations.put(applicationId, applicationAllocated - slots)
    }
    activeSlots = activeSlots - Math.min(shuffleAllocated, slots)
  }

  def releaseSlots(shuffleKey: String): Unit = this.synchronized {
    val allocated = shuffleAllocations.remove(shuffleKey)
    if (allocated != null) {
      val applicationId = Utils.splitShuffleKey(shuffleKey)._1
      var applicationAllocated = applicationAllocations.getOrDefault(applicationId, 0)
      applicationAllocated = applicationAllocated - allocated
      if (applicationAllocated <= 0) {
        applicationAllocations.remove(applicationId)
      } else {
        applicationAllocations.put(applicationId, applicationAllocated)
      }
      activeSlots = activeSlots - allocated
    }
  }

  def getShuffleKeySet(): util.HashSet[String] = this.synchronized {
    new util.HashSet(shuffleAllocations.keySet())
  }

  def getApplicationIdSet(): util.HashSet[String] = this.synchronized {
    new util.HashSet(applicationAllocations.keySet())
  }

  override def toString: String = this.synchronized {
    val (emptyShuffles, nonEmptyShuffles) = shuffleAllocations.asScala.partition(_._2 == 0)
    s"DiskInfo(maxSlots: $maxSlots," +
      s" committed shuffles ${emptyShuffles.size}," +
      s" running applications ${applicationAllocations.size}," +
      s" shuffleAllocations: ${nonEmptyShuffles.toMap}," +
      s" mountPoint: $mountPoint," +
      s" usableSpace: ${Utils.bytesToString(actualUsableSpace)}," +
      s" avgFlushTime: ${Utils.nanoDurationToString(avgFlushTime)}," +
      s" avgFetchTime: ${Utils.nanoDurationToString(avgFetchTime)}," +
      s" activeSlots: $activeSlots," +
      s" storageType: ${storageType})" +
      s" status: $status" +
      s" dirs ${dirs.mkString("\t")}"

  }
}

class DeviceInfo(val name: String) extends Serializable {
  var diskInfos: ListBuffer[DiskInfo] = new ListBuffer[DiskInfo]()
  // if deviceStatAvailable is false means that there is no device info found.
  var deviceStatAvailable = true

  def addDiskInfo(diskInfo: DiskInfo): Unit = {
    diskInfos.append(diskInfo)
  }

  override def hashCode(): Int = {
    name.hashCode
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[DeviceInfo] && name.equals(obj.asInstanceOf[DeviceInfo].name)
  }

  override def toString: String = {
    s"DeviceName: ${name}\tMount Infos: ${diskInfos.mkString("\n")}"
  }
}

object DeviceInfo {
  val logger = LoggerFactory.getLogger(classOf[DeviceInfo])

  def getMountPoint(absPath: String, mountPoints: util.Set[String]): String = {
    var curMax = -1
    var curMount = ""
    mountPoints.asScala.foreach(mount => {
      if (absPath.startsWith(mount) && mount.length > curMax) {
        if (absPath.length == mount.length) {
          return mount
        } else if (absPath.length > mount.length &&
          (mount == "/" || absPath.substring(mount.length, mount.length + 1) == "/")) {
          curMax = mount.length
          curMount = mount
        }
      }
    })
    curMount
  }

  def getMountPoint(absPath: String, diskInfos: util.Map[String, DiskInfo]): String = {
    getMountPoint(absPath, diskInfos.keySet())
  }
}
