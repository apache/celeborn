package com.aliyun.emr.rss.service.deploy.worker.storage.local

import java.io.IOException

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.DiskStatus
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.protocol.StorageInfo
import com.aliyun.emr.rss.service.deploy.worker.DeviceMonitor
import com.aliyun.emr.rss.service.deploy.worker.storage.{DeviceObserver, Flusher}


private[worker] class LocalFlusher(
    workerSource: AbstractSource,
    val deviceMonitor: DeviceMonitor,
    threadCount: Int,
    val mountPoint: String,
    flushAvgTimeWindowSize: Int,
    flushAvgTimeMinimumCount: Int,
    val diskType: StorageInfo.Type) extends Flusher(
  workerSource,
  threadCount,
  flushAvgTimeWindowSize,
  flushAvgTimeMinimumCount)
  with DeviceObserver with Logging {

  deviceMonitor.registerFlusher(this)

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    stopFlag.set(true)
    logError(s"$this write failed, report to DeviceMonitor, eception: $e")
    deviceMonitor.reportDeviceError(mountPoint, e, deviceErrorType)
  }

  override def notifyError(mountPoint: String,
    diskStatus: DiskStatus): Unit = {
    logError(s"$this is notified Disk $mountPoint $diskStatus! Stop LocalFlusher.")
    stopFlag.set(true)
    try {
      workers.foreach(_.interrupt())
    } catch {
      case e: Exception =>
        logError(s"Exception when interrupt worker: ${workers.mkString(",")}, $e")
    }
    workingQueues.foreach { queue =>
      queue.asScala.foreach { task =>
        task.buffer.removeComponents(0, task.buffer.numComponents())
        task.buffer.clear()
      }
    }
    deviceMonitor.unregisterFlusher(this)
  }

  override def hashCode(): Int = {
    mountPoint.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[LocalFlusher] &&
      obj.asInstanceOf[LocalFlusher].mountPoint.equals(mountPoint)
  }

  override def toString(): String = {
    s"LocalFlusher@$flusherId-$mountPoint"
  }
}
