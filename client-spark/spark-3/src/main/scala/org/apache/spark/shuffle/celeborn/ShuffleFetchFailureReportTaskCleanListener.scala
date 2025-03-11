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

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

class ShuffleFetchFailureReportTaskCleanListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
    val shuffleFetchFailureTaskIds =
      SparkUtils.removeStageReportedShuffleFetchFailureTaskIds(stageId, stageAttemptId)
    if (shuffleFetchFailureTaskIds != null) {
      shuffleFetchFailureTaskIds.asScala.headOption.foreach { case taskId =>
        val taskSetManager = SparkUtils.getTaskSetManager(taskId)
        if (taskSetManager != null && taskSetManager.runningTasks > 0) {
          taskSetManager.abort(
            s"Aborting the ${taskSetManager.runningTasks} running tasks in stage" +
              s" $stageId (attempt $stageAttemptId) due to shuffle fetch failure." +
              s" The entire stage will be rerun.")
        }
      }
    }
  }
}
