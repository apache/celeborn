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

package org.apache.celeborn.client

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.util.ThreadUtils

class ReleasePartitionManager(
    appId: String,
    val conf: CelebornConf,
    lifecycleManager: LifecycleManager)
  extends Logging {

  // shuffleId -> (partitionId set of release)
  private val shuffleReleasePartitionRequests = new ConcurrentHashMap[Int, util.Set[Int]]
  private val batchHandleReleasePartitionEnabled = conf.batchHandleReleasePartitionEnabled
  private val batchHandleReleasePartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "rss-lifecycle-manager-release-partition-executor",
    conf.batchHandleReleasePartitionNumThreads)
  private val batchHandleReleasePartitionRequestInterval =
    conf.batchHandleReleasePartitionRequestInterval
  private val batchHandleReleasePartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleReleasePartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "rss-lifecycle-manager-release-partition-scheduler"))
    } else {
      None
    }
  private var batchHandleReleasePartition: Option[ScheduledFuture[_]] = _

  def start(): Unit = {
    batchHandleReleasePartition = batchHandleReleasePartitionSchedulerThread.map {
      // noinspection ConvertExpressionToSAM
      _.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit = {
            try {
              shuffleReleasePartitionRequests.asScala.foreach {
                case (shuffleId, unReleasedPartitionIdRequestSet) =>
                  batchHandleReleasePartitionExecutors.submit {
                    new Runnable {
                      override def run(): Unit = {
                        val unReleasePartitionIds = new util.HashSet[Int]
                        unReleasedPartitionIdRequestSet.synchronized {
                          unReleasePartitionIds.addAll(unReleasedPartitionIdRequestSet)
                          unReleasedPartitionIdRequestSet.clear()
                        }

                        lifecycleManager.workerSnapshots(shuffleId).asScala.foreach {
                          case (workerInfo, partitionLocationInfo) =>
                            val destroyResource = new WorkerResource
                            unReleasePartitionIds.asScala.foreach {
                              partitionId =>
                                addDestroyResource(
                                  destroyResource,
                                  workerInfo,
                                  partitionLocationInfo,
                                  partitionId)
                            }

                            if (!destroyResource.isEmpty) {
                              lifecycleManager.destroySlotsWithRetry(
                                appId,
                                shuffleId,
                                destroyResource)
                              logTrace(s"Destroyed partition resource for shuffle $shuffleId $destroyResource")
                            }
                        }
                      }
                    }
                  }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
            }
          }
        },
        0,
        batchHandleReleasePartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    batchHandleReleasePartition.foreach(_.cancel(true))
    batchHandleReleasePartitionSchedulerThread.foreach(ThreadUtils.shutdown(_, 800.millis))
  }

  def releasePartition(shuffleId: Int, partitionId: Int): Unit = {
    if (batchHandleReleasePartitionEnabled) {
      shuffleReleasePartitionRequests.putIfAbsent(shuffleId, new util.HashSet[Int])
      val unReleasedPartitionIdRequestSet = shuffleReleasePartitionRequests.get(shuffleId)
      unReleasedPartitionIdRequestSet.synchronized {
        unReleasedPartitionIdRequestSet.add(partitionId)
      }
    } else {
      val destroyResource = new WorkerResource
      lifecycleManager.workerSnapshots(shuffleId).asScala.foreach {
        case (workerInfo, partitionLocationInfo) =>
          addDestroyResource(destroyResource, workerInfo, partitionLocationInfo, partitionId)
      }

      if (!destroyResource.isEmpty) {
        lifecycleManager.destroySlotsWithRetry(appId, shuffleId, destroyResource)
        logTrace(
          s"Destroyed partition resource for partition $shuffleId-$partitionId, $destroyResource")
      }
    }
  }

  private def addDestroyResource(
      workerResource: WorkerResource,
      workerInfo: WorkerInfo,
      partitionLocationInfo: ShufflePartitionLocationInfo,
      partitionId: Int): Unit = {
    if (partitionLocationInfo.containsPartition(partitionId)) {
      val masterLocations = partitionLocationInfo.removeMasterPartitions(partitionId)
      if (masterLocations != null && !masterLocations.isEmpty) {
        workerResource.computeIfAbsent(
          workerInfo,
          lifecycleManager.newLocationFunc)._1.addAll(masterLocations)
      }

      val slaveLocations = partitionLocationInfo.removeSlavePartitions(partitionId)
      if (slaveLocations != null && !slaveLocations.isEmpty) {
        workerResource.computeIfAbsent(
          workerInfo,
          lifecycleManager.newLocationFunc)._2.addAll(slaveLocations)
      }
    }
  }
}
