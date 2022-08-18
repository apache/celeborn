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

import java.io.File
import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.slf4j.LoggerFactory

import com.aliyun.emr.rss.common.util.Utils.runCommand

class DiskInfo(
  val mountPoint: String,
  var usableSpace: Long,
  // avgFlushTime is nano seconds
  var avgFlushTime: Long,
  var activeSlots: Long,
  @transient val deviceInfo: DeviceInfo) extends Serializable {

  def this(mountPoint: String, usableSpace: Long, avgFlushTime: Long, activeSlots: Long) = {
    this(mountPoint, usableSpace, avgFlushTime, activeSlots, null)
  }

  def this(mountPoint: String, deviceInfo: DeviceInfo) = {
    this(mountPoint, 0, 0, 0, deviceInfo)
  }

  def update(usableSpace: Long, avgFlushTime: Long): Unit = {
    this.usableSpace = usableSpace;
    this.avgFlushTime = avgFlushTime;
  }

  @transient
  val dirInfos: ListBuffer[File] = new ListBuffer[File]()
  @transient
  val mountPointFile = new File(mountPoint)

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

  def addDir(dir: File): Unit = {
    dirInfos.append(dir)
  }

  override def toString: String = s"DiskInfo(maxSlots: $maxSlots," +
    s" shuffleAllocations: $shuffleAllocations," +
    s" mountPoint: $mountPoint," +
    s" usableSpace: $usableSpace," +
    s" avgFlushTime: $avgFlushTime," +
    s" activeSlots: $activeSlots)" +
    s" dirs ${dirInfos.mkString("\t")}"
}

class DeviceInfo(val name: String) extends Serializable {
  var diskInfos: ListBuffer[DiskInfo] = new ListBuffer[DiskInfo]()
  // if noDevice is true means that there is no device info found.
  var deviceStatAvailable = false

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

  /**
   *
   * @param workingDirs
   * @return it will return three maps
   *         (deviceName -> deviceInfo)
   *         (mount point -> diskInfo)
   *         (working dir -> diskInfo)
   */
  def getDeviceAndDiskInfos(workingDirs: util.List[File]): (
      util.Map[String, DeviceInfo],
      util.Map[String, DiskInfo],
      util.Map[String, DiskInfo]) = {
    val allDevices = new util.HashMap[String, DeviceInfo]()
    val allDisks = new util.HashMap[String, DiskInfo]()

    // (/dev/vdb, /mnt/disk1)
    val dfResult = runCommand("df -h").trim
    logger.info(s"df result $dfResult")
    val fsMounts = dfResult
      .split("[\n\r]")
      .tail
      .map(line => {
        val tokens = line.trim.split("[ \t]+")
        (tokens.head, tokens.last)
      })

    // (vda, vdb)
    val lsBlockResult = runCommand("ls /sys/block/").trim
    logger.info(s"ls block $lsBlockResult")
    val blocks = lsBlockResult
      .split("[ \n\r\t]+")

    fsMounts.foreach { case (fileSystem, mountpoint) =>
      val deviceName = fileSystem.substring(fileSystem.lastIndexOf('/') + 1)
      var index = -1
      var maxLength = -1
      blocks.zipWithIndex.foreach(block => {
        if (deviceName.startsWith(block._1) && block._1.length > maxLength) {
          index = block._2
          maxLength = block._1.length
        }
      })

      val newDeviceInfoFunc =
        new util.function.Function[String, DeviceInfo]() {
          override def apply(s: String): DeviceInfo = {
            val deviceInfo = new DeviceInfo(s)
            if (index < 0) {
              deviceInfo.deviceStatAvailable = true
            }
            deviceInfo
          }
        }

      val deviceInfo = if (index >= 0) {
        allDevices.computeIfAbsent(blocks(index), newDeviceInfoFunc)
      } else {
        allDevices.computeIfAbsent(deviceName, newDeviceInfoFunc)
      }
      val diskInfo = new DiskInfo(mountpoint, deviceInfo = deviceInfo)
      deviceInfo.addDiskInfo(diskInfo)
      allDisks.putIfAbsent(mountpoint, diskInfo)
    }

    val retDeviceInfos = new ConcurrentHashMap[String, DeviceInfo]()
    val retDiskInfos = new ConcurrentHashMap[String, DiskInfo]()
    val retWorkingDiskInfos = new ConcurrentHashMap[String, DiskInfo]()

    workingDirs.asScala.foreach(dir => {
      val mount = getMountPoint(dir.getAbsolutePath, allDisks)
      val diskInfo = allDisks.get(mount)
      diskInfo.addDir(dir)
      retDiskInfos.putIfAbsent(diskInfo.mountPoint, diskInfo)
      retDeviceInfos.putIfAbsent(diskInfo.deviceInfo.name, diskInfo.deviceInfo)
      retWorkingDiskInfos.put(dir.getAbsolutePath, diskInfo)
    })

    retDeviceInfos.asScala.foreach(entry => {
      val diskInfos = entry._2.diskInfos.filter(_.dirInfos.nonEmpty)
      entry._2.diskInfos = diskInfos
    })
    logger.info(s"Device initialization \n " +
      s"$retDeviceInfos \n $retDiskInfos \n $retWorkingDiskInfos")

    (retDeviceInfos, retDiskInfos, retWorkingDiskInfos)
  }

  def getMountPoint(absPath: String, diskInfos: util.Map[String, DiskInfo]): String = {
    var curMax = -1
    var curMount = ""
    diskInfos.keySet().asScala.foreach(mount => {
      if (absPath.startsWith(mount) && mount.length > curMax) {
        curMax = mount.length
        curMount = mount
      }
    })
    curMount
  }
}
