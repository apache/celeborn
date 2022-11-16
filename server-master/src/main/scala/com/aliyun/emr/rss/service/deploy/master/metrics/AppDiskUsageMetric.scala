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

package com.aliyun.emr.rss.service.deploy.master.metrics

import java.time.LocalDateTime
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, mapAsScalaMapConverter}

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}

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

class AppDiskUsageMetric(conf: RssConf) extends Logging {
  val usageCount = RssConf.metricsAppTopDiskUsageCount(conf)
  val snapshotCount = RssConf.metricsAppTopDiskUsageWindowSize(conf)
  val interval = RssConf.metricsAppTopDiskUsageInterval(conf)
  val snapShots = new Array[AppDiskUsageSnapShot](snapshotCount)
  val logExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("App_disk_usage_metric_thread")
  val updateExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("App_disk_usage_metric_thread")
  var currentSnapShot: AtomicReference[AppDiskUsageSnapShot] =
    new AtomicReference[AppDiskUsageSnapShot]()

  def update(map: java.util.Map[WorkerInfo, java.util.Map[String, java.lang.Long]]): Unit = {
    updateExecutor.submit(new Runnable {
      override def run(): Unit = {
        val aggregatedAppDiskUsage = new util.HashMap[String, Long]()
        map.values().asScala.foreach(_.asScala.foreach { case (shuffleKey, usage) =>
          val appId = shuffleKey.split("-")(0)
          if (aggregatedAppDiskUsage.containsKey(appId)) {
            aggregatedAppDiskUsage.put(appId, aggregatedAppDiskUsage.get(appId) + usage)
          } else {
            aggregatedAppDiskUsage.put(appId, usage)
          }
        })
        if (currentSnapShot.get() != null) {
          aggregatedAppDiskUsage.asScala.foreach { case (key, usage) =>
            currentSnapShot.get().updateAppDiskUsage(key, usage)
          }
        }
      }
    })
  }

  logExecutor.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      if (currentSnapShot.get() != null) {
        currentSnapShot.get().commit()
      }
      currentSnapShot.set(getNewSnapShot())
      logInfo(s"App Disk Usage Top${usageCount} Report ${summary()}")
    }
  }, interval, interval, TimeUnit.SECONDS)

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
