package com.aliyun.emr.rss.service.deploy.worker.storage

import com.aliyun.emr.rss.common.meta.DiskStatus

trait DeviceObserver {
  def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = {}
  def notifyHealthy(mountPoint: String): Unit = {}
  def notifyHighDiskUsage(mountPoint: String): Unit = {}
}