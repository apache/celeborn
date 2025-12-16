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

package org.apache.spark.celeborn

import java.time.Instant
import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.CelebornSparkContextHelper
import org.apache.spark.listener.{CelebornShuffleEarlyCleanup, ShuffleStatsTrackingListener}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.shuffle.celeborn.SparkShuffleManager

import org.apache.celeborn.common.internal.Logging

class StageDependencyManager(shuffleManager: SparkShuffleManager) extends Logging {

  // celeborn shuffle id to all stages reading it, this is needed when we determine when to
  // clean the shuffle
  private[celeborn] val readShuffleToStageDep = new mutable.HashMap[Int, mutable.HashSet[Int]]()
  // stage id to all celeborn shuffle ids it reads from, this structure is needed for fast
  // tracking when a stage is completed
  private val stageToReadCelebornShuffleDep = new mutable.HashMap[Int, mutable.HashSet[Int]]()
  // spark stage id to celeborn shuffle id which it writes,
  // we need to save this mapping so that we can query which celeborn shuffles is depended on by a
  // certain stage when it is submitted
  private val stageToCelebornShuffleIdWritten = new mutable.HashMap[Int, Int]()
  // app shuffle id to all app shuffle ids it reads from, this structure used as the intermediate data
  private val appShuffleIdToUpstream = new mutable.HashMap[Int, mutable.HashSet[Int]]()
  // stage id to app shuffle id it writes, this structure used as the intermediate data
  // to build appShuffleIdToUpstream and is needed when we need to
  // invalidate all app shuffle map output location when a stage is failed
  private val stageToAppShuffleIdWritten = new mutable.HashMap[Int, Int]()

  private val celebornToAppShuffleIdentifier = new mutable.HashMap[Int, String]()
  private val appShuffleIdentifierToSize = new mutable.HashMap[String, Long]()

  private val shuffleIdsToBeCleaned = new LinkedBlockingQueue[Int]()

  private lazy val cleanInterval = shuffleManager.getLifecycleManager
    .conf.clientShuffleEarlyDeletionIntervalMs

  def addShuffleAndStageDep(celebornShuffleId: Int, stageId: Int): Unit = this.synchronized {
    val newStageIdSet =
      readShuffleToStageDep.getOrElseUpdate(celebornShuffleId, new mutable.HashSet[Int]())
    newStageIdSet += stageId
    val newShuffleIdSet =
      stageToReadCelebornShuffleDep.getOrElseUpdate(stageId, new mutable.HashSet[Int]())
    newShuffleIdSet += celebornShuffleId
    val correctionResult = shuffleIdsToBeCleaned.remove(celebornShuffleId)
    if (correctionResult) {
      logInfo(s"shuffle $celebornShuffleId is later recognized as needed by stage $stageId, " +
        s"removed it from to be cleaned list")
    }
  }

  private def stageOutputToShuffleOrS3(stageInfo: StageInfo): Boolean = {
    stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten > 0 ||
    stageInfo.taskMetrics.outputMetrics.bytesWritten > 0
  }

  private def removeStageAndReadInfo(stageId: Int): Unit = {
    stageToReadCelebornShuffleDep.remove(stageId)
  }

  // it is called when the stage is completed
  def addAppShuffleIdentifierToSize(appShuffleIdentifier: String, bytes: Long): Unit =
    this.synchronized {
      appShuffleIdentifierToSize += appShuffleIdentifier -> bytes
    }

  // this is called when a shuffle is cleaned up
  def queryShuffleSizeByAppShuffleIdentifier(appShuffleIdentifier: String): Long =
    this.synchronized {
      appShuffleIdentifierToSize.getOrElse(
        appShuffleIdentifier, {
          logError(s"unexpected case: cannot find size information for shuffle identifier" +
            s" $appShuffleIdentifier")
          -1L
        })
    }

  def removeShuffleAndStageDep(stageInfo: StageInfo): Unit = this.synchronized {
    val stageId = stageInfo.stageId
    val allReadCelebornIds = stageToReadCelebornShuffleDep.get(stageId)
    allReadCelebornIds.foreach { celebornShuffleIds =>
      celebornShuffleIds.foreach { celebornShuffleId =>
        val allStages = readShuffleToStageDep.get(celebornShuffleId)
        allStages.foreach { stages =>
          stages.remove(stageId)
          if (stages.nonEmpty) {
            readShuffleToStageDep += celebornShuffleId -> stages
          } else {
            val readyToDelete = {
              if (shuffleManager.getLifecycleManager.conf.clientShuffleEarlyDeletionCheckProp) {
                val propertySet = System.getProperty("CELEBORN_EARLY_SHUFFLE_DELETION", "false")
                propertySet.toBoolean && stageOutputToShuffleOrS3(stageInfo)
              } else {
                stageOutputToShuffleOrS3(stageInfo)
              }
            }
            if (readyToDelete) {
              removeCelebornShuffleInternal(celebornShuffleId, stageId = Some(stageInfo.stageId))
            } else {
              logInfo(
                s"not ready to delete shuffle $celebornShuffleId while stage $stageId finished")
            }
          }
        }
      }
    }
  }

  private[celeborn] def removeCelebornShuffleInternal(
      celebornShuffleId: Int,
      stageId: Option[Int]): Unit = {
    shuffleIdsToBeCleaned.put(celebornShuffleId)
    readShuffleToStageDep.remove(celebornShuffleId)
    val appShuffleIdentifierOpt = celebornToAppShuffleIdentifier.get(celebornShuffleId)
    if (appShuffleIdentifierOpt.isEmpty) {
      logWarning(s"cannot find appShuffleIdentifier for celeborn shuffle: $celebornShuffleId")
      return
    }
    val appShuffleIdentifier = appShuffleIdentifierOpt.get
    val Array(appShuffleId, stageOfShuffleBeingDeleted, _) =
      appShuffleIdentifier.split('-')
    val shuffleSize = queryShuffleSizeByAppShuffleIdentifier(appShuffleIdentifier)
    celebornToAppShuffleIdentifier.remove(celebornShuffleId)
    logInfo(s"clean up app shuffle id $appShuffleIdentifier," +
      s" celeborn shuffle id : $celebornShuffleId")
    stageId.foreach(sid => removeStageAndReadInfo(sid))
    //    ClientMetricsSystem.updateShuffleWrittenBytes(shuffleSize * -1)
    stageId.foreach(sid =>
      CelebornSparkContextHelper.eventLogger.foreach(e => {
        // for shuffles being deleted when no one refers to it, we need to make a record of
        // stage reading it to calculate the cost saving accurately
        e.onOtherEvent(CelebornShuffleEarlyCleanup(
          celebornShuffleId,
          appShuffleId.toInt,
          stageOfShuffleBeingDeleted.toInt,
          shuffleSize,
          readStageId = sid,
          timeToEnqueue = Instant.now().toEpochMilli))
      }))
  }

  def queryCelebornShuffleIdByWriterStageId(stageId: Int): Option[Int] = this.synchronized {
    stageToCelebornShuffleIdWritten.get(stageId)
  }

  def getAppShuffleIdByStageId(stageId: Int): Int = this.synchronized {
    // return -1 means the stage is not writing any shuffle
    stageToAppShuffleIdWritten.getOrElse(stageId, -1)
  }

  def getAllUpstreamAppShuffleIdsByStageId(stageId: Int): Array[Int] = this.synchronized {
    val writtenAppShuffleId = stageToAppShuffleIdWritten.getOrElse(
      stageId,
      throw new IllegalStateException(s"cannot find app shuffle id written by stage $stageId"))
    val allUpstreamAppShuffleIds = appShuffleIdToUpstream.getOrElse(
      writtenAppShuffleId,
      throw new IllegalStateException(s"cannot find upstream shuffle ids written of shuffle " +
        s"$writtenAppShuffleId"))
    allUpstreamAppShuffleIds.toArray
  }

  def addStageToCelebornShuffleIdRef(celebornShuffleId: Int, appShuffleIdentifier: String): Unit =
    this.synchronized {
      val Array(appShuffleId, stageId, _) = appShuffleIdentifier.split('-')
      stageToCelebornShuffleIdWritten += stageId.toInt -> celebornShuffleId
      stageToAppShuffleIdWritten += stageId.toInt -> appShuffleId.toInt
    }

  def addCelebornToAppShuffleIdMapping(
      celebornShuffleId: Int,
      appShuffleIdentifier: String): Unit = {
    this.synchronized {
      celebornToAppShuffleIdentifier += celebornShuffleId -> appShuffleIdentifier
    }
  }

  def addCelebornShuffleIdReadingStageDep(
      celebornShuffleId: Int,
      appShuffleIdentifier: String): Unit = {
    this.synchronized {
      val Array(_, stageId, _) = appShuffleIdentifier.split('-')
      val stageIds =
        readShuffleToStageDep.getOrElseUpdate(celebornShuffleId, new mutable.HashSet[Int]())
      stageIds += stageId.toInt
      val celebornShuffleIds =
        stageToReadCelebornShuffleDep.getOrElseUpdate(stageId.toInt, new mutable.HashSet[Int]())
      celebornShuffleIds += celebornShuffleId
    }
  }

  def addAppShuffleIdReadingStageDep(appShuffleId: Int, appShuffleIdentifier: String): Unit = {
    this.synchronized {
      val Array(_, sid, _) = appShuffleIdentifier.split('-')
      val stageId = sid.toInt
      // update shuffle id to all upstream
      if (stageToAppShuffleIdWritten.contains(stageId)) {
        val upstreamAppShuffleIds = appShuffleIdToUpstream.getOrElseUpdate(
          stageToAppShuffleIdWritten(stageId),
          new mutable.HashSet[Int]())
        if (!upstreamAppShuffleIds.contains(appShuffleId)) {
          logInfo(s"new upstream shuffleId detected for shuffle" +
            s" ${stageToAppShuffleIdWritten(stageId)}, latest: $appShuffleIdToUpstream")
          upstreamAppShuffleIds += appShuffleId
        }
      }
    }
  }

  def hasAllUpstreamShuffleIdsInfo(stageId: Int): Boolean = this.synchronized {
    stageToAppShuffleIdWritten.contains(stageId) &&
    appShuffleIdToUpstream.contains(stageToAppShuffleIdWritten(stageId))
  }

  private var stopped: Boolean = false

  def start(): Unit = {
    val cleanerThread = new Thread() {
      override def run(): Unit = {
        while (!stopped) {
          val allShuffleIds = new util.ArrayList[Int]
          shuffleIdsToBeCleaned.drainTo(allShuffleIds)
          allShuffleIds.asScala.foreach { shuffleId =>
            shuffleManager.getLifecycleManager.unregisterShuffle(shuffleId)
            logInfo(s"sent unregister shuffle request for shuffle $shuffleId (celeborn shuffle id)")
          }
          Thread.sleep(cleanInterval)
        }
      }
    }

    cleanerThread.setName("shuffle early cleaner thread")
    cleanerThread.setDaemon(true)
    cleanerThread.start()
  }

  def stop(): Unit = {
    stopped = true
  }
}
