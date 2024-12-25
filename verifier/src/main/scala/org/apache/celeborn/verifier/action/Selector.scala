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

package org.apache.celeborn.verifier.action

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, enumerationAsScalaIteratorConverter, mapAsScalaConcurrentMapConverter}
import scala.util.Random

import com.alibaba.fastjson2.JSONObject

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.plan.exception.PlanInvalidException
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Selector(val interval: Long) extends Serializable {

  private val skipInterval = getInterval == 0

  def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget]

  def getInterval: Long = this.interval

  def getItemListByIndices[T](elems: List[T], index: List[Int]): List[T] = {
    index.map(index => index -> elems(index)).map(_._2)
  }

  override def toString: String = s"Selector($skipInterval, $interval)"

  protected def getAllNodes(schedulerContext: SchedulerContext): List[String] = {
    schedulerContext.runnerInfos.asScala.keys.toList
  }

  protected def getMasterNodeListByState(
      context: SchedulerContext,
      statusToBe: Boolean): List[String] = {
    context.runnerInfos.asScala.filter(_._2.resource.masterAlive != statusToBe).keys.toList
  }

  protected def getWorkerNodeListByState(
      context: SchedulerContext,
      statusToBe: Boolean): List[String] = {
    context.runnerInfos.asScala.filter(_._2.resource.workerAlive != statusToBe).keys.toList
  }

  protected def getDisksByState(
      context: SchedulerContext,
      diskError: String,
      statusToBe: Boolean): List[(String, String)] = {
    val diskWithStatus = context.runnerInfos.asScala.flatMap(runnerInfo =>
      runnerInfo._2.resource.workingDirs.toList
        .map(workingDir => (runnerInfo._1, workingDir._1, workingDir._2))).toList
    val disks = diskError match {
      case DiskError.DISK_CORRUPT =>
        if (statusToBe) {
          diskWithStatus.filter(_._3.unwritable == false)
        } else {
          diskWithStatus.filter(_._3.unwritable)
        }
      case DiskError.DISK_HANG =>
        if (statusToBe) {
          diskWithStatus.filter(_._3.hanging == false)
        } else {
          diskWithStatus.filter(_._3.hanging)
        }
    }
    disks.map(item => (item._1, item._2))
  }
}

object SelectorType {
  val SELECTOR_ASSIGN = "assign"
  val SELECTOR_RANDOM = "random"
}

object Selector {

  val dummySelector = new DummySelector(0)
  val dummyDisk = "dummyDisk"

  def fromJson(obj: JSONObject, conf: VerifierConf): Selector = {
    val selectorType = Option(obj.getString("type")).getOrElse("assign")
    val intervalInMs = Utils.timeStringAsMs(Option(obj.getString("interval")).getOrElse(
      VerifierConf.planActionSelectorDefaultInterval(conf)))
    selectorType match {
      case SelectorType.SELECTOR_ASSIGN =>
        val indices =
          Option(obj.getJSONArray("indices")).get.asScala.map(_.asInstanceOf[Int]).toList
        val devices =
          if (obj.containsKey("device")) {
            Option(obj.getJSONArray("device")).get
              .asScala.map(_.asInstanceOf[Int]).toList
          } else {
            List.empty[Int]
          }
        new AssignSelector(intervalInMs, indices, devices)
      case SelectorType.SELECTOR_RANDOM =>
        new RandomSelector(intervalInMs)
      case unknownType: String =>
        throw new PlanInvalidException(
          s"Selector supports ${SelectorType.SELECTOR_ASSIGN} and ${SelectorType.SELECTOR_RANDOM}, but got $unknownType.")
    }
  }
}

class AssignSelector(override val interval: Long, val indices: List[Int], val devices: List[Int])
  extends Selector(interval) {

  def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {
    val masters = getAllNodes(schedulerContext)
    val workers = getAllNodes(schedulerContext)
    val targets = target match {
      case ActionTarget.TARGET_DISK =>
        getItemListByIndices(workers, indices).flatMap(node =>
          getItemListByIndices(
            schedulerContext.runnerInfos.get(node).resource.workingDirs.keys.toList,
            devices).map(workingDir => ActionTarget(node, workingDir)))
      case ActionTarget.TARGET_MASTER =>
        getItemListByIndices(masters, indices).map(node =>
          ActionTarget(node, Selector.dummyDisk))
      case ActionTarget.TARGET_RUNNER =>
        getItemListByIndices(schedulerContext.runnerInfos.keys().asScala.toList, indices).map(
          node =>
            ActionTarget(node, Selector.dummyDisk))
      case ActionTarget.TARGET_WORKER =>
        getItemListByIndices(workers, indices).map(node =>
          ActionTarget(node, Selector.dummyDisk))
    }
    targets
  }

  override def getInterval: Long = this.interval
}

class DummySelector(override val interval: Long) extends Selector(interval) {

  override def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {
    List.empty[ActionTarget]
  }

  override def getInterval: Long = interval
}

class RandomSelector(override val interval: Long) extends Selector(interval) {

  private var statusToBe: Boolean = _
  private var diskErrorOperation: String = _

  def updateSelectorStat(status: Boolean, diskErrorOperation: String): Unit = {
    this.statusToBe = status
    this.diskErrorOperation = diskErrorOperation
  }

  override def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {
    val targets = target match {
      case ActionTarget.TARGET_DISK =>
        val requiredDisks = getDisksByState(schedulerContext, diskErrorOperation, statusToBe)
        if (requiredDisks.isEmpty) {
          null
        } else {
          val selectedDisk = selectRandomResource(requiredDisks)
          ActionTarget(selectedDisk._1, selectedDisk._2)
        }
      case ActionTarget.TARGET_MASTER =>
        ActionTarget(
          selectRandomResource(getMasterNodeListByState(schedulerContext, statusToBe)),
          Selector.dummyDisk)
      case ActionTarget.TARGET_RUNNER =>
        ActionTarget(
          selectRandomResource(schedulerContext.runnerInfos.keys().asScala.toList),
          Selector.dummyDisk)
      case ActionTarget.TARGET_WORKER =>
        ActionTarget(
          selectRandomResource(getWorkerNodeListByState(schedulerContext, statusToBe)),
          Selector.dummyDisk)
    }
    if (targets == null) {
      List.empty[ActionTarget]
    } else {
      List(targets)
    }
  }

  private def selectRandomResource[T](list: List[T]): T = {
    list(Random.nextInt(list.size))
  }
}
