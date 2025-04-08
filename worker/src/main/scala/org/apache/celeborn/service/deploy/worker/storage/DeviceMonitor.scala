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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DeviceInfo, DiskFileInfo, DiskInfo, DiskStatus, FileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.util.{DiskUtils, ThreadUtils, Utils}
import org.apache.celeborn.common.util.Utils._
import org.apache.celeborn.service.deploy.worker.WorkerSource

trait DeviceMonitor {
  def startCheck() {}
  def registerFileWriter(fileWriter: PartitionDataWriter, filePath: String): Unit = {}
  def unregisterFileWriter(fileWriter: PartitionDataWriter): Unit = {}
  // Only local flush needs device monitor.
  def registerFlusher(flusher: LocalFlusher): Unit = {}
  def unregisterFlusher(flusher: LocalFlusher): Unit = {}
  def reportNonCriticalError(mountPoint: String, e: IOException, diskStatus: DiskStatus): Unit = {}
  def close() {}
}

object EmptyDeviceMonitor extends DeviceMonitor

class LocalDeviceMonitor(
    conf: CelebornConf,
    observer: DeviceObserver,
    deviceInfos: util.Map[String, DeviceInfo],
    diskInfos: util.Map[String, DiskInfo],
    workerSource: AbstractSource) extends DeviceMonitor with Logging {

  // (deviceName -> ObservedDevice)
  var observedDevices: util.Map[DeviceInfo, ObservedDevice] = _

  val diskCheckInterval = conf.workerDiskMonitorCheckInterval

  // we should choose what the device needs to detect
  val deviceMonitorCheckList = conf.workerDiskMonitorCheckList
  val checkIoHang = deviceMonitorCheckList.contains("iohang")
  val checkReadWrite = deviceMonitorCheckList.contains("readwrite")
  val checkDiskUsage = deviceMonitorCheckList.contains("diskusage")
  private val diskChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-disk-checker")

  def init(): Unit = {
    this.observedDevices = new util.HashMap[DeviceInfo, ObservedDevice]()
    deviceInfos.asScala.filter(!_._2.deviceStatAvailable).foreach { case (deviceName, _) =>
      logWarning(s"Device monitor may not work properly on $deviceName " +
        s"because device $deviceName not exists.")
    }
    deviceInfos.asScala.foreach { entry =>
      val observedDevice = new ObservedDevice(entry._2, conf, workerSource)
      observedDevice.addObserver(observer)
      observedDevices.put(entry._2, observedDevice)
    }
    diskInfos
      .asScala
      .values
      .toList
      .groupBy(_.deviceInfo)
      .foreach { case (deviceInfo: DeviceInfo, diskInfos: List[DiskInfo]) =>
        val deviceLabel = Map("device" -> deviceInfo.name)
        def usage: DeviceMonitor.DiskUsageInfo =
          try {
            DeviceMonitor.getDiskUsageInfos(diskInfos.head)
          } catch {
            case t: Throwable =>
              logError("Device monitor get usage infos failed.", t)
              DeviceMonitor.DiskUsageInfo(0L, 0L, 0L, 0)
          }
        workerSource.addGauge(WorkerSource.DEVICE_OS_TOTAL_CAPACITY, deviceLabel) { () =>
          usage.totalSpace
        }
        workerSource.addGauge(WorkerSource.DEVICE_OS_FREE_CAPACITY, deviceLabel) { () =>
          usage.freeSpace
        }
        workerSource.addGauge(WorkerSource.DEVICE_CELEBORN_TOTAL_CAPACITY, deviceLabel) { () =>
          diskInfos.map(_.totalSpace).sum
        }
        workerSource.addGauge(WorkerSource.DEVICE_CELEBORN_FREE_CAPACITY, deviceLabel) { () =>
          diskInfos.map(_.actualUsableSpace).sum
        }
      }
  }

  override def startCheck(): Unit = {
    diskChecker.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          logDebug("Device check start")
          try {
            observedDevices.values().asScala.foreach(device => {
              val mountPoints = device.diskInfos.keySet.asScala.toList
              // tolerate time accuracy for better performance
              val now = System.currentTimeMillis()
              for (concurrentList <- device.nonCriticalErrors.values().asScala) {
                for (time <- concurrentList.asScala) {
                  if (now - time > device.notifyErrorExpireTimeout) {
                    concurrentList.remove(time)
                  }
                }
              }
              val nonCriticalErrorSum = device.nonCriticalErrors.values().asScala.map(_.size).sum
              if (nonCriticalErrorSum > device.notifyErrorThreshold) {
                logError(s"Device ${device.deviceInfo.name} has accumulated $nonCriticalErrorSum non-critical " +
                  s"error within the past ${Utils.msDurationToString(device.notifyErrorExpireTimeout)} , its sum has " +
                  s"exceed the threshold (${device.notifyErrorThreshold}), device monitor will notify error to " +
                  s"observed device.")
                val mountPoints = device.diskInfos.values().asScala.map(_.mountPoint).toList
                device.notifyObserversOnError(mountPoints, DiskStatus.CRITICAL_ERROR)
              } else {
                if (checkIoHang && device.ioHang()) {
                  logError(s"Encounter device io hang error!" +
                    s"${device.deviceInfo.name}, notify observers")
                  device.notifyObserversOnNonCriticalError(mountPoints, DiskStatus.IO_HANG)
                } else {
                  device.diskInfos.values().asScala.foreach { diskInfo =>
                    if (checkDiskUsage && DeviceMonitor.highDiskUsage(conf, diskInfo)) {
                      logError(s"${diskInfo.mountPoint} high_disk_usage error, notify observers")
                      device.notifyObserversOnHighDiskUsage(diskInfo.mountPoint)
                    } else if (checkReadWrite &&
                      DeviceMonitor.readWriteError(conf, diskInfo.dirs.head)) {
                      logError(s"${diskInfo.mountPoint} read-write error, notify observers")
                      // We think that if one dir in device has read-write problem, if possible all
                      // dirs in this device have the problem
                      device.notifyObserversOnNonCriticalError(
                        List(diskInfo.mountPoint),
                        DiskStatus.READ_OR_WRITE_FAILURE)
                    } else if (nonCriticalErrorSum <= device.notifyErrorThreshold * 0.5) {
                      device.notifyObserversOnHealthy(diskInfo.mountPoint)
                    }
                  }
                }
              }
            })
          } catch {
            case t: Throwable =>
              logError("Device check failed.", t)
          }
        }
      },
      0,
      diskCheckInterval,
      TimeUnit.MILLISECONDS)
  }

  override def registerFileWriter(fileWriter: PartitionDataWriter, filePath: String): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(filePath, diskInfos)
    observedDevices.get(diskInfos.get(mountPoint).deviceInfo).addObserver(fileWriter)
  }

  override def unregisterFileWriter(fileWriter: PartitionDataWriter): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(fileWriter.getFilePath, diskInfos)
    observedDevices.get(diskInfos.get(mountPoint).deviceInfo).removeObserver(fileWriter)
  }

  override def registerFlusher(flusher: LocalFlusher): Unit = {
    observedDevices.get(diskInfos.get(flusher.mountPoint).deviceInfo).addObserver(flusher)
  }

  override def unregisterFlusher(flusher: LocalFlusher): Unit = {
    observedDevices.get(diskInfos.get(flusher.mountPoint).deviceInfo).removeObserver(flusher)
  }

  override def reportNonCriticalError(
      mountPoint: String,
      e: IOException,
      diskStatus: DiskStatus): Unit = {
    logError(s"Receive non-critical exception, disk: $mountPoint, $e")
    observedDevices.get(diskInfos.get(mountPoint).deviceInfo)
      .notifyObserversOnNonCriticalError(List(mountPoint), diskStatus)
  }

  override def close(): Unit = {
    if (null != DeviceMonitor.deviceCheckThreadPool) {
      DeviceMonitor.deviceCheckThreadPool.shutdownNow()
    }
  }
}

object DeviceMonitor extends Logging {
  var deviceCheckThreadPool: ThreadPoolExecutor = _

  def createDeviceMonitor(
      conf: CelebornConf,
      deviceObserver: DeviceObserver,
      deviceInfos: util.Map[String, DeviceInfo],
      diskInfos: util.Map[String, DiskInfo],
      workerSource: AbstractSource): DeviceMonitor = {
    try {
      if (conf.workerDiskMonitorEnabled) {
        deviceCheckThreadPool = ThreadUtils.newDaemonCachedThreadPool("worker-device-checker", 5)
        val monitor =
          new LocalDeviceMonitor(conf, deviceObserver, deviceInfos, diskInfos, workerSource)
        monitor.init()
        logInfo("Device monitor init success")
        monitor
      } else {
        EmptyDeviceMonitor
      }
    } catch {
      case t: Throwable =>
        logError("Device monitor init failed.", t)
        throw t
    }
  }

  case class DiskUsageInfo(totalSpace: Long, freeSpace: Long, usedSpace: Long, usedPercent: Int)

  // unit is byte
  def getDiskUsageInfos(diskInfo: DiskInfo): DiskUsageInfo = {
    val dirFile = Files.getFileStore(Paths.get(diskInfo.mountPoint))
    val totalSpace = dirFile.getTotalSpace
    val freeSpace = dirFile.getUsableSpace
    val usedSpace = totalSpace - freeSpace
    val usedPercent = (usedSpace * 100.0 / totalSpace).toInt
    DiskUsageInfo(totalSpace, freeSpace, usedSpace, usedPercent)
  }

  /**
   * check if the disk is high usage
   *
   * @param conf     conf
   * @param diskInfo diskInfo
   * @return true if high disk usage
   */
  def highDiskUsage(conf: CelebornConf, diskInfo: DiskInfo): Boolean = {
    tryWithTimeoutAndCallback({
      val usage = getDiskUsageInfos(diskInfo)
      // assume no single device capacity exceeds 1EB in this era
      val actualReserveSize = DiskUtils.getActualReserveSize(
        diskInfo,
        conf.workerDiskReserveSize,
        conf.workerDiskReserveRatio)
      val highDiskUsage =
        usage.freeSpace < actualReserveSize || diskInfo.actualUsableSpace <= 0
      if (highDiskUsage) {
        logWarning(s"${diskInfo.mountPoint} usage is above threshold." +
          s" Disk usage(Report by OS): {total:${Utils.bytesToString(usage.totalSpace)}," +
          s" free:${Utils.bytesToString(usage.freeSpace)}, used_percent:${usage.usedPercent}}," +
          s" usage(Report by Celeborn): {" +
          s" total:${Utils.bytesToString(diskInfo.configuredUsableSpace)}," +
          s" free:${Utils.bytesToString(diskInfo.actualUsableSpace)} }")
      }
      highDiskUsage
    })(false)(
      deviceCheckThreadPool,
      conf.workerDiskMonitorStatusCheckTimeout,
      s"Disk: ${diskInfo.mountPoint} Usage Check Timeout")
  }

  /**
   * check if the data dir has read-write problem
   *
   * @param conf    conf
   * @param dataDir one of shuffle data dirs in mount disk
   * @return true if disk has read-write problem
   */
  def readWriteError(conf: CelebornConf, dataDir: File): Boolean = {
    if (null == dataDir) {
      return false
    }

    tryWithTimeoutAndCallback({
      try {
        if (!dataDir.exists()) {
          dataDir.mkdirs()
        }
        val file = new File(dataDir, s"_SUCCESS_${System.currentTimeMillis()}")
        if (!file.exists() && !file.createNewFile()) {
          true
        } else {
          FileUtils.write(file, "test", Charset.defaultCharset)
          var fileInputStream: FileInputStream = null
          var inputStreamReader: InputStreamReader = null
          var bufferReader: BufferedReader = null
          try {
            fileInputStream = FileUtils.openInputStream(file)
            inputStreamReader = new InputStreamReader(fileInputStream, Charset.defaultCharset())
            bufferReader = new BufferedReader(inputStreamReader)
            bufferReader.readLine()
          } finally {
            Option(bufferReader).foreach(_.close())
            Option(inputStreamReader).foreach(_.close())
            Option(fileInputStream).foreach(_.close())
          }
          FileUtils.forceDelete(file)
          false
        }
      } catch {
        case t: Throwable =>
          logError(s"Disk dir $dataDir cannot read or write", t)
          true
      }
    })(false)(
      deviceCheckThreadPool,
      conf.workerDiskMonitorStatusCheckTimeout,
      s"Disk: $dataDir Read_Write Check Timeout")
  }

  def EmptyMonitor(): DeviceMonitor = EmptyDeviceMonitor
}
