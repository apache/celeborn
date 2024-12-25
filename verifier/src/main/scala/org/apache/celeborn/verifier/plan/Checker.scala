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

package org.apache.celeborn.verifier.plan

import java.util

import scala.collection.JavaConverters.{mapAsScalaConcurrentMapConverter, seqAsJavaListConverter}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.verifier.action.{ActionTarget, ActionTendency}
import org.apache.celeborn.verifier.scheduler.SchedulerContext

trait Checker extends Serializable with Logging {

  def tendency: Int

  def availableTargets: util.HashSet[String]

  def validate(deducedContext: SchedulerContext): Boolean
}

object Checker {

  private val DUMMY_CHECKER = new DummyChecker()

  def getChecker(str: Option[String]): Checker = {
    if (str.isEmpty) {
      return DUMMY_CHECKER
    }
    str.get match {
      case "resource" => new ResourceChecker
      case _ => DUMMY_CHECKER
    }
  }
}

class DummyChecker extends Checker {

  override def tendency: Int = 0

  override def availableTargets: util.HashSet[String] = new util.HashSet[String]()

  override def validate(deducedContext: SchedulerContext): Boolean = true
}

class ResourceChecker extends Checker {

  var tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override val availableTargets: util.HashSet[String] = new util.HashSet[String](
    List(
      ActionTarget.TARGET_RUNNER,
      ActionTarget.TARGET_SCHEDULER,
      ActionTarget.TARGET_WORKER,
      ActionTarget.TARGET_MASTER,
      ActionTarget.TARGET_DISK,
      ActionTarget.TARGET_CPU).asJava)

  override def validate(deducedContext: SchedulerContext): Boolean = {
    validateSingleContext(deducedContext)
  }

  private def validateSingleContext(context: SchedulerContext): Boolean = {
    val aliveMasters = context.runnerInfos.asScala.filter(_._2.resource.masterAlive).keys
    val aliveWorkers = context.runnerInfos.asScala.filter(_._2.resource.workerAlive).keys
    val masterExhausted = aliveMasters.size <= context.runnerInfos.size() / 2
    val workerExhausted = aliveWorkers.size <= 2
    val workingDirExhausted =
      aliveWorkers.map(context.runnerInfos.get(_).resource.workingDirs
        .map(_._2.available()).reduce(_ || _)).toList.count(p => p) <= 2
    if (tendency == ActionTendency.TENDENCY_NEGATIVE) {
      if (masterExhausted) {
        availableTargets.remove(ActionTarget.TARGET_MASTER)
      }
      if (workerExhausted) {
        availableTargets.remove(ActionTarget.TARGET_WORKER)
      }
      if (workingDirExhausted) {
        availableTargets.remove(ActionTarget.TARGET_DISK)
      }
    }
    val resourceExhausted = masterExhausted && workerExhausted && workingDirExhausted
    if (resourceExhausted && tendency != ActionTendency.TENDENCY_POSITIVE) {
      logDebug("Checker tendency changed from NEGATIVE to POSITIVE.")
      tendency = ActionTendency.TENDENCY_POSITIVE
      availableTargets.add(ActionTarget.TARGET_MASTER)
      availableTargets.add(ActionTarget.TARGET_WORKER)
      availableTargets.add(ActionTarget.TARGET_DISK)
    }
    val masterFull = aliveMasters.size == context.runnerInfos.size()
    val workerFull = aliveWorkers.size == context.runnerInfos.size()
    val workingDirFull = aliveWorkers.map(
      context.runnerInfos.get(_).resource.workingDirs
        .map(_._2.available()).reduce(_ && _)).reduce(_ && _)
    if (tendency == ActionTendency.TENDENCY_POSITIVE) {
      if (masterFull) {
        availableTargets.remove(ActionTarget.TARGET_MASTER)
      }
      if (workerFull) {
        availableTargets.remove(ActionTarget.TARGET_WORKER)
      }
      if (workingDirFull) {
        availableTargets.remove(ActionTarget.TARGET_DISK)
      } else {
        availableTargets.add(ActionTarget.TARGET_DISK)
      }
    }
    if (masterFull && workerFull && workingDirFull && tendency != ActionTendency.TENDENCY_NEGATIVE) {
      logDebug("Checker tendency changed from POSITIVE to NEGATIVE.")
      tendency = ActionTendency.TENDENCY_NEGATIVE
      availableTargets.add(ActionTarget.TARGET_MASTER)
      availableTargets.add(ActionTarget.TARGET_WORKER)
      availableTargets.add(ActionTarget.TARGET_DISK)
    }
    !resourceExhausted
  }
}
