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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.util.Utils
import scala.collection.JavaConverters._

abstract class DiskInfoBase(
                             val mountPoint: String,
                             var actualUsableSpace: Long,
                             var avgFlushTime: Long, // in nano seconds
                             var avgFetchTime: Long, // in nano seconds
                             var activeSlots: Long,
                             val dirs: List[File],
                             val deviceInfo: DeviceInfo) extends Serializable with Logging {

  var flushTimeMetrics: TimeWindow = _
  var fetchTimeMetrics: TimeWindow = _
  var status: DiskStatus = DiskStatus.HEALTHY
  var threadCount = 1
  var configuredUsableSpace = 0L
  var totalSpace = 0L
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

  def setTotalSpace(totalSpace: Long): this.type = this.synchronized {
    this.totalSpace = totalSpace
    this
  }

  def updateFlushTime(): Unit = {
    avgFlushTime = flushTimeMetrics.getAverage()
  }

  def updateFetchTime(): Unit = {
    avgFetchTime = fetchTimeMetrics.getAverage()
  }

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
    activeSlots -= Math.min(shuffleAllocated, slots)
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
      s" totalSpace: ${Utils.bytesToString(totalSpace)}," +
      s" avgFlushTime: ${Utils.nanoDurationToString(avgFlushTime)}," +
      s" avgFetchTime: ${Utils.nanoDurationToString(avgFetchTime)}," +
      s" activeSlots: $activeSlots," +
      s" storageType: ${storageType})" +
      s" status: $status" +
      s" dirs ${dirs.mkString("\t")}"

  }
}
