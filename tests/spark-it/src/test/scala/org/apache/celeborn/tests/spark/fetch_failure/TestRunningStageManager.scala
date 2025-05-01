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
package org.apache.celeborn.tests.spark.fetch_failure

import scala.collection.mutable

import org.apache.spark.scheduler.SparkSchedulerHelper

import org.apache.celeborn.spark.RunningStageManager

class TestRunningStageManager extends RunningStageManager {
  import TestRunningStageManager._

  override def isRunningStage(stageId: Int): Boolean = {
    if (runningStages.contains(stageId)) {
      true
    } else {
      SparkSchedulerHelper.runningStages.map(_.id).contains(stageId)
    }
  }

  override def isDeterministicStage(stageId: Int): Boolean = {
    if (indeterministicStages.contains(stageId)) {
      false
    } else {
      SparkSchedulerHelper.stageIdToStage.get(stageId).exists(_.isIndeterminate)
    }
  }
}

object TestRunningStageManager {
  val runningStages = new mutable.HashSet[Int]
  val indeterministicStages = new mutable.HashSet[Int]()
}
