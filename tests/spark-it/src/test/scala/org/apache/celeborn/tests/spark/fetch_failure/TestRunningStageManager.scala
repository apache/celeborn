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

import org.apache.spark.scheduler.{RunningStageManager, SparkSchedulerHelper}

class TestRunningStageManager extends RunningStageManager {
  import TestRunningStageManager._
  def setRunningStages(stageIds: Seq[Int]): Unit = {
    stageIds.foreach(stageId => runningStages += stageId)
  }
  override def isRunningStage(stageId: Int): Boolean = {
    if (runningStages.contains(stageId)) {
      println(s"instrumented running stages contains $stageId")
      true
    } else {
      SparkSchedulerHelper.runningStages.map(_.id).contains(stageId)
    }
  }
}
object TestRunningStageManager {
  val runningStages = new mutable.HashSet[Int]
}
