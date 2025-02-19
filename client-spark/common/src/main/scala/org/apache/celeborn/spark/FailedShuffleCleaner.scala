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
package org.apache.celeborn.spark
import java.util
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.scheduler.{RunningStageManager, RunningStageManagerImpl}

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.internal.Logging

private[celeborn] object FailedShuffleCleaner extends Logging {

  private val lifecycleManager = new AtomicReference[LifecycleManager](null)
  // in celeborn ids
  private val shufflesToBeCleand = new LinkedBlockingQueue[Int]()
  private val cleanedShuffleIds = new mutable.HashSet[Int]
  // celeborn shuffle id to stage id referred to it
  private[celeborn] val celebornShuffleIdToReferringStages =
    new ConcurrentHashMap[Int, mutable.HashSet[Int]]()

  private val lock = new Object
  val RUNNING_STAGE_CHECKER_CLASS = "CELEBORN_TEST_RUNNING_STAGE_CHECKER_IMPL"

  private[celeborn] var runningStageManager: RunningStageManager = buildRunningStageChecker()

  // for testing
  private def buildRunningStageChecker(): RunningStageManager = {
    if (System.getProperty(RUNNING_STAGE_CHECKER_CLASS) == null) {
      new RunningStageManagerImpl
    } else {
      val className = System.getProperty(RUNNING_STAGE_CHECKER_CLASS)
      val claz = Class.forName(className)
      claz.getDeclaredConstructor().newInstance().asInstanceOf[RunningStageManager]
    }
  }

  // for test
  def reset(): Unit = {
    lifecycleManager.set(null)
    shufflesToBeCleand.clear()
    cleanedShuffleIds.clear()
    celebornShuffleIdToReferringStages.clear()
    runningStageManager = buildRunningStageChecker()
  }

  def addShuffleIdReferringStage(celebornShuffleId: Int, appShuffleIdentifier: String): Unit = {
    // this is only implemented/tested with Spark for now
    val Array(_, stageId, _) = appShuffleIdentifier.split('-')
    val stageIds =
      celebornShuffleIdToReferringStages.computeIfAbsent(
        celebornShuffleId,
        (_) => new mutable.HashSet[Int]())
    lock.synchronized {
      stageIds.add(stageId.toInt)
    }
  }

  private def onlyCurrentStageReferred(celebornShuffleId: Int, stageId: Int): Boolean = {
    val ret = celebornShuffleIdToReferringStages.get(celebornShuffleId).size == 1 &&
      celebornShuffleIdToReferringStages.get(celebornShuffleId).contains(stageId)
    if (ret) {
      logInfo(s"only stage $stageId refers to shuffle $celebornShuffleId, adding for clean up")
    }
    ret
  }

  def addShuffleIdToBeCleaned(
      celebornShuffleId: Int,
      appShuffleIdentifier: String): Unit = {
    val Array(appShuffleId, stageId, _) = appShuffleIdentifier.split('-')
    lifecycleManager.get().getShuffleIdMapping.get(appShuffleId.toInt).foreach {
      case (pastAppShuffleIdentifier, (celebornShuffleId, _)) => {
        if (!celebornShuffleIdToReferringStages.containsKey(celebornShuffleId)
          || onlyCurrentStageReferred(celebornShuffleId, stageId.toInt)
          || noRunningDownstreamStage(celebornShuffleId)
          || !committedSuccessfully(celebornShuffleId)) {
          val Array(_, stageId, attemptId) = pastAppShuffleIdentifier.split('-')
          shufflesToBeCleand.put(celebornShuffleId)
        }
      }
    }
  }

  private def committedSuccessfully(celebornShuffleId: Int): Boolean = {
    val ret = !lifecycleManager.get().commitManager.getCommitHandler(celebornShuffleId)
      .isStageDataLost(celebornShuffleId)
    if (!ret) {
      logInfo(s"shuffle $celebornShuffleId is failed to commit, adding for cleaning up")
    }
    ret
  }

  def setLifecycleManager(ref: LifecycleManager): Unit = {
    lifecycleManager.compareAndSet(null, ref)
  }

  private def noRunningDownstreamStage(shuffleId: Int): Boolean = {
    val allReferringStageIds = celebornShuffleIdToReferringStages.get(shuffleId)
    require(allReferringStageIds != null, s"no stage referring to shuffle $shuffleId")
    val ret =
      allReferringStageIds.count(stageId => runningStageManager.isRunningStage(stageId)) == 0
    if (ret) {
      logInfo(s"no running downstream stages refers to $shuffleId")
    } else {
      logInfo(s"there is more than one running downstream stage referring to shuffle $shuffleId," +
        s" ignore it for cleanup ")
    }
    ret
  }

  private val cleanerThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        val allShuffleIds = new util.ArrayList[Int]
        shufflesToBeCleand.drainTo(allShuffleIds)
        allShuffleIds.asScala.foreach { shuffleId =>
          if (!cleanedShuffleIds.contains(shuffleId)) {
            lifecycleManager.get().unregisterShuffle(shuffleId)
            logInfo(s"sent unregister shuffle request for shuffle $shuffleId (celeborn shuffle id)")
            cleanedShuffleIds += shuffleId
          }
        }
        Thread.sleep(1000)
      }
    }
  }

  cleanerThread.setName("shuffle cleaner thread")
  cleanerThread.setDaemon(true)
  cleanerThread.start()
}
