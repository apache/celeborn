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

package org.apache.celeborn.service.deploy.master.metrics

import java.time.LocalDateTime
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, mapAsScalaMapConverter}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

case class AppDiskUsage(var appId: String, var usage: Long) {
  override def toString: String = s"Application ${appId} used ${Utils.bytesToString(usage)} "
}

class AppDiskUsageSnapShot(val topItemCount: Int) extends Logging {
  val topNItems = new Array[AppDiskUsage](topItemCount)
  val startSnapShotTime = LocalDateTime.now()
  var endSnapShotTime: LocalDateTime = _

  def commit(): Unit = {
    endSnapShotTime = LocalDateTime.now()
  }

  def updateAppDiskUsage(appId: String, usage: Long): Unit = {
    val dropIndex = topNItems.indexWhere(usage => usage != null && usage.appId == appId)
    if (dropIndex != -1) {
      drop(dropIndex)
    }
    val insertIndex = findInsertPosition(usage)
    if (insertIndex != -1) {
      shift(insertIndex)
      topNItems(insertIndex) = AppDiskUsage(appId, usage)
    }
  }

  def shift(index: Int): Unit = {
    for (i <- topItemCount - 1 until index by -1) {
      topNItems(i) = topNItems(i - 1)
    }
  }

  def drop(index: Int): Unit = {
    for (i <- index until topItemCount - 1) {
      topNItems(i) = topNItems(i + 1)
    }
    topNItems(topItemCount - 1) = null
  }

  def findInsertPosition(usage: Long): Int = {
    if (topNItems(0) == null) {
      return 0
    }
    for (i <- 0 until topItemCount) {
      if (topNItems(i) == null || topNItems(i).usage < usage) {
        return i
      }
    }
    -1
  }

  override def toString(): String = s"Snapshot " +
    s"start ${startSnapShotTime} end ${endSnapShotTime}" +
    s" ${topNItems.filter(_ != null).mkString(",")}"
}

class AppDiskUsageMetric(conf: CelebornConf) extends Logging {
  val usageCount = conf.metricsAppTopDiskUsageCount
  val snapshotCount = conf.metricsAppTopDiskUsageWindowSize
  val interval = conf.metricsAppTopDiskUsageInterval
  val snapShots = new Array[AppDiskUsageSnapShot](snapshotCount)
  val logExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("App_disk_usage_log_thread")
  val updateExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("App_disk_usage_metric_thread")
  var currentSnapShot: AtomicReference[AppDiskUsageSnapShot] =
    new AtomicReference[AppDiskUsageSnapShot]()

  def update(appDiskUsage: java.util.Map[String, java.lang.Long]): Unit = {
    updateExecutor.submit(new Runnable {
      override def run(): Unit = {
        if (currentSnapShot.get() != null) {
          appDiskUsage.asScala.foreach { case (key, usage) =>
            currentSnapShot.get().updateAppDiskUsage(key, usage)
          }
        }
      }
    })
  }

  logExecutor.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        if (currentSnapShot.get() != null) {
          currentSnapShot.get().commit()
        }
        currentSnapShot.set(getNewSnapShot())
        logInfo(s"App Disk Usage Top${usageCount} Report ${summary()}")
      }
    },
    60,
    interval,
    TimeUnit.SECONDS)

  def getNewSnapShot(): AppDiskUsageSnapShot = {
    for (i <- snapshotCount - 1 until 0 by -1) {
      snapShots(i) = snapShots(i - 1)
    }
    snapShots(0) = new AppDiskUsageSnapShot(usageCount)
    snapShots(0)
  }

  def summary(): String = {
    val stringBuilder = new StringBuilder()
    for (i <- 0 until snapshotCount) {
      if (snapShots(i) != null) {
        stringBuilder.append(snapShots(i))
        stringBuilder.append("    \n")
      }
    }
    stringBuilder.toString()
  }
}
