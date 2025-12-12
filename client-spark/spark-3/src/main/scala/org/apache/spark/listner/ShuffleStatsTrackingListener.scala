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

package org.apache.spark.listener

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager

class ShuffleStatsTrackingListener extends SparkListener with Logging {

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    logInfo(s"stage ${stageSubmitted.stageInfo.stageId}.${stageSubmitted.stageInfo.attemptNumber()} started")
    val stageId = stageSubmitted.stageInfo.stageId
    val shuffleMgr = SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager]
    val parentStages = stageSubmitted.stageInfo.parentIds
    if (shuffleMgr.getLifecycleManager.conf.clientShuffleEarlyDeletion) {
      parentStages.foreach { parentStageId =>
        val celebornShuffleId = shuffleMgr.getStageDepManager
          .queryCelebornShuffleIdByWriterStageId(parentStageId)
        celebornShuffleId.foreach { sid =>
          shuffleMgr.getStageDepManager.addShuffleAndStageDep(sid, stageId)
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageIdentifier = s"${stageCompleted.stageInfo.stageId}-" +
      s"${stageCompleted.stageInfo.attemptNumber()}"
    logInfo(s"stage $stageIdentifier finished with" +
      s" ${stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten} shuffle bytes")
    val shuffleMgr = SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager]
    if (shuffleMgr.getLifecycleManager.conf.clientShuffleEarlyDeletion ||
      shuffleMgr.getLifecycleManager.conf.clientFetchCleanFailedShuffle) {
      val shuffleIdOpt = stageCompleted.stageInfo.shuffleDepId
      shuffleIdOpt.foreach { appShuffleId =>
        val appShuffleIdentifier = s"$appShuffleId-${stageCompleted.stageInfo.stageId}-" +
          s"${stageCompleted.stageInfo.attemptNumber()}"
        shuffleMgr.getStageDepManager.addAppShuffleIdentifierToSize(
          appShuffleIdentifier,
          stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten)
      }
    }
    if (shuffleMgr.getLifecycleManager.conf.clientShuffleEarlyDeletion &&
      stageCompleted.stageInfo.failureReason.isEmpty) {
      shuffleMgr.getStageDepManager.removeShuffleAndStageDep(stageCompleted.stageInfo)
    }
  }
}
