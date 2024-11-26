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

package org.apache.celeborn.service.deploy.master

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.AppDiskUsageSnapShot
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager

class AppDiskUsageMetricManager(conf: CelebornConf, metaManager: AbstractMetaManager)
  extends Logging {
  val usageCount = conf.metricsAppTopDiskUsageCount
  val snapshotCount = conf.metricsAppTopDiskUsageWindowSize
  val interval = conf.metricsAppTopDiskUsageInterval
  val snapShots = new Array[AppDiskUsageSnapShot](snapshotCount)
  val logExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-app-disk-usage-metrics-logger")
  val updateExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-app-disk-usage-metrics-updater")
  var currentSnapShot: AtomicReference[AppDiskUsageSnapShot] =
    new AtomicReference[AppDiskUsageSnapShot]()

  def update(): Unit = {
    if (currentSnapShot.get() != null) {
      metaManager.appHeartbeatTime.keySet().asScala.foreach(appId => {
        val usage =
          metaManager.workersMap.values().asScala.map(_.getAppDiskUsage(appId).toLong).sum
        currentSnapShot.get().updateAppDiskUsage(appId, usage)
      })
    }
  }

  updateExecutor.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        update()
      }
    },
    60,
    interval,
    TimeUnit.SECONDS)

  logExecutor.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        if (currentSnapShot.get() != null) {
          currentSnapShot.get().commit()
        }
        currentSnapShot.set(getNewSnapShot())
        val summaryStr = Some(summary()).getOrElse("")
        logInfo(s"App Disk Usage Top$usageCount Report: $summaryStr")
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
      if (snapShots(i) != null && snapShots(i).topNItems.exists(_ != null)) {
        stringBuilder.append("\n")
        stringBuilder.append(snapShots(i))
      }
    }
    stringBuilder.toString()
  }

  def restoreFromSnapshot(array: Array[AppDiskUsageSnapShot]): Unit = {
    // Restored snapshots only contains values not null
    for (i <- 0 until snapshotCount) {
      if (i < array.length) {
        snapShots(i) = array(i)
      } else {
        snapShots(i) = null
      }
    }
  }

  def topSnapshots(): Seq[AppDiskUsageSnapShot] = {
    snapShots.take(snapshotCount)
      .filter(_ != null)
      .filter(_.topNItems.exists(_ != null))
  }
}
