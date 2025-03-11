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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

import org.apache.celeborn.common.internal.Logging

class ShuffleFetchFailureReportTaskCleanListener extends SparkListener with Logging {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
    val shuffleFetchFailureTaskIds =
      SparkUtils.removeStageReportedShuffleFetchFailureTaskIds(stageId, stageAttemptId)
    if (shuffleFetchFailureTaskIds != null) {
      val taskSetManager = SparkUtils.getTaskSetManager(stageId, stageAttemptId)
      if (taskSetManager != null && taskSetManager.runningTasks > 0) {
        logInfo(s"Killing the ${taskSetManager.runningTasks} running tasks in stage" +
          s" $stageId (attempt $stageAttemptId) due to shuffle fetch failure." +
          s" The entire stage will be rerun.")
        SparkUtils.killTaskSetManagerRunningTasks(
          taskSetManager,
          "The entire stage will be rerun due to shuffle fetch failure.")
      }
    }
  }
}
