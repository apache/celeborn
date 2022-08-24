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

package com.aliyun.emr.rss.service.deploy.worker.storage

import java.io.{BufferedReader, File, FileInputStream, IOException, InputStreamReader}
import java.nio.charset.Charset
import java.util

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.meta.{DeviceInfo, DiskInfo, DiskStatus}
import com.aliyun.emr.rss.common.util.ThreadUtils
import com.aliyun.emr.rss.common.util.Utils._
import com.aliyun.emr.rss.service.deploy.worker.FileWriter
import com.aliyun.emr.rss.service.deploy.worker.storage.local.{LocalDeviceMonitor, LocalFlusher}

trait DeviceMonitor {
  def startCheck() {}
  def registerFileWriter(fileWriter: FileWriter): Unit = {}
  def unregisterFileWriter(fileWriter: FileWriter): Unit = {}
  // Only local flush needs device monitor.
  def registerFlusher(flusher: LocalFlusher): Unit = {}
  def unregisterFlusher(flusher: LocalFlusher): Unit = {}
  def reportDeviceError(mountPoint: String, e: IOException, diskStatus: DiskStatus): Unit = {}
  def close() {}
}

object EmptyDeviceMonitor extends DeviceMonitor

object DeviceMonitor {
  val logger = LoggerFactory.getLogger(classOf[DeviceMonitor])
  val deviceCheckThreadPool = ThreadUtils.newDaemonCachedThreadPool("device-check-thread", 5)

  def createDeviceMonitor(
    rssConf: RssConf,
    deviceObserver: DeviceObserver,
    deviceInfos: util.Map[String, DeviceInfo],
    diskInfos: util.Map[String, DiskInfo]): DeviceMonitor = {
    try {
      if (RssConf.deviceMonitorEnabled(rssConf)) {
        val monitor = new LocalDeviceMonitor(rssConf, deviceObserver, deviceInfos, diskInfos)
        monitor.init()
        logger.info("Device monitor init success")
        monitor
      } else {
        EmptyDeviceMonitor
      }
    } catch {
      case t: Throwable =>
        logger.error("Device monitor init failed.", t)
        throw t
    }
  }

  /**
   * check if the disk is high usage
   * @param rssConf conf
   * @param diskRootPath disk root path
   * @return true if high disk usage
   */
  def highDiskUsage(rssConf: RssConf, diskRootPath: String): Boolean = {
    tryWithTimeoutAndCallback({
      val usage = runCommand(s"df -B 1G $diskRootPath").trim.split("[ \t]+")
      val totalSpace = usage(usage.length - 1)
      val freeSpace = usage(usage.length - 3)
      val used_percent = usage(usage.length - 2)

      val status = freeSpace.toLong < RssConf.diskMinimumReserveSize(rssConf) / 1024 / 1024 / 1024
      if (status) {
        logger.warn(s"$diskRootPath usage:{total:$totalSpace GB," +
          s" free:$freeSpace GB, used_percent:$used_percent}")
      }
      status
    })(false)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(rssConf),
      s"Disk: $diskRootPath Usage Check Timeout")
  }

  /**
   * check if the data dir has read-write problem
   * @param rssConf conf
   * @param dataDir one of shuffle data dirs in mount disk
   * @return true if disk has read-write problem
   */
  def readWriteError(rssConf: RssConf, dataDir: File): Boolean = {
    if (null == dataDir || !dataDir.isDirectory) {
      return false
    }

    tryWithTimeoutAndCallback({
      try {
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
            bufferReader.close()
            inputStreamReader.close()
            fileInputStream.close()
          }
          FileUtils.forceDelete(file)
          false
        }
      } catch {
        case t: Throwable =>
          logger.error(s"Disk dir $dataDir cannot read or write", t)
          true
      }
    })(false)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(rssConf),
      s"Disk: $dataDir Read_Write Check Timeout")
  }

  def EmptyMonitor(): DeviceMonitor = EmptyDeviceMonitor
}
