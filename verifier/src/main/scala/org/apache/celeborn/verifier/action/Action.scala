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

import com.alibaba.fastjson2.JSONObject

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.plan.exception.PlanInvalidException
import org.apache.celeborn.verifier.runner.RunnerContext
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Action(val selector: Selector, val interval: Long) extends Serializable
  with Logging {

  val target: String = ActionTarget.TARGET_SCHEDULER
  val tendency: Int = ActionTendency.TENDENCY_NEUTRAL

  def identity(): String

  def generateOperations(context: SchedulerContext): List[Operation]

  def deduce(context: SchedulerContext, operations: List[Operation]): SchedulerContext = {
    val deduced = context.duplicate()
    operations.foreach(op => {
      val fakeContext =
        RunnerContext(context.conf, deduced.runnerInfos.get(op.actionTarget.identity).resource)
      op.updateContextBlock(fakeContext)
    })
    deduced
  }

  override def toString: String =
    s"Action($target, $tendency, $selector, $interval, ${identity()})"
}

case class ActionTarget(identity: String, workingDir: String)

object DiskError {
  val DISK_CORRUPT = "corrupt"
  val DISK_HANG = "hang"
}

abstract class DiskAction(
    override val selector: Selector,
    diskError: String,
    status: Boolean,
    hangIo: Boolean,
    override val interval: Long)
  extends Action(selector, interval) {

  override val target: String = ActionTarget.TARGET_DISK

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    selector match {
      case randomSelector: RandomSelector =>
        randomSelector.updateSelectorStat(status, diskError)
      case _ =>
    }
    val targets = selector.select(context, target)
    if (targets.isEmpty) {
      return List.empty[Operation]
    }
    targets.map(ta => {
      val corruptDiskCmd = s"sh chmod 400 ${ta.workingDir}"
      val resumeDiskCmd = s"sh chmod 777 ${ta.workingDir}"
      val corruptDiskBlock = (ctx: RunnerContext) => {
        ctx.resource.workingDirs(ta.workingDir).hanging = true
      }
      val resumeDiskBlock = (ctx: RunnerContext) => {
        ctx.resource.workingDirs(ta.workingDir).hanging = true
      }
      val systemBlock = context.conf.celebornConf.workerDiskMonitorSysBlockDir
      val deviceName =
        context.runnerInfos.get(ta.identity).resource.workingDirs(ta.workingDir).deviceName
      val badInflightFile = VerifierConf.planActionBadInflightFile(context.conf)
      val hangIoDiskCmd = s"ln -sfn $badInflightFile $systemBlock/$deviceName/inflight"
      val resumeIoHangCmd =
        s"ln -sfn $systemBlock/$deviceName/inflight $systemBlock/$deviceName/inflight"
      val hangIoBlock = (ctx: RunnerContext) => {
        ctx.resource.workingDirs(ta.workingDir).hanging = true
      }
      val resumeIoBlock = (ctx: RunnerContext) => {
        ctx.resource.workingDirs(ta.workingDir).hanging = false
      }
      val (cmd, block) = diskError match {
        case DiskError.DISK_CORRUPT =>
          if (status) {
            (corruptDiskCmd, corruptDiskBlock)
          } else {
            (resumeDiskCmd, resumeDiskBlock)
          }
        case DiskError.DISK_HANG =>
          if (hangIo) {
            (hangIoDiskCmd, hangIoBlock)
          } else {
            (resumeIoHangCmd, resumeIoBlock)
          }
      }
      new BashOperation(ta, cmd, block, selector.interval)
    })
  }
}

class CorruptDiskAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskError.DISK_CORRUPT, true, false, interval) {

  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_DISK_CORRUPT_DISK
}

class CorruptMetaAction(override val selector: Selector, override val interval: Long)
  extends Action(selector, interval) {

  override val target: String = ActionTarget.TARGET_MASTER
  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_DISK_CORRUPT_META

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    val targets = selector.select(context, target)
    targets.map(tar =>
      new BashOperation(
        tar,
        s"rm -rf ${context.conf.celebornConf.haMasterRatisStorageDir}",
        interval = selector.interval))
  }
}

class HangIoAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskError.DISK_HANG, false, true, interval) {

  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_DISK_HANG_IO
}

class OccupyCpuAction(
    override val selector: Selector,
    cores: Int,
    duration: Long,
    override val interval: Long)
  extends Action(selector, interval) {

  override val target: String = ActionTarget.TARGET_RUNNER
  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_OCCUPY_CPU

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    val targets = selector.select(context, target)
    targets.map(node =>
      new OccupyCpuOperation(node, cores, duration, selector.interval))
  }

  override def deduce(context: SchedulerContext, operations: List[Operation]): SchedulerContext =
    context.duplicate()
}

class ResumeDiskAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskError.DISK_CORRUPT, false, false, interval) {

  override val tendency: Int = ActionTendency.TENDENCY_POSITIVE

  override def identity(): String = ActionIdentity.ACTION_RESUME_DISK
}

class ResumeIoAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskError.DISK_HANG, false, false, interval) {

  override val tendency: Int = ActionTendency.TENDENCY_POSITIVE

  override def identity(): String = ActionIdentity.ACTION_RESUME_IO
}

abstract class NodeAction(
    override val selector: Selector,
    conf: VerifierConf,
    toStart: Boolean,
    isMaster: Boolean,
    override val interval: Long)
  extends Action(selector, interval) {

  private val startMasterScript: String = VerifierConf.startMasterScript(conf)
  private val stopMasterScript: String = VerifierConf.stopMasterScript(conf)
  private val startWorkerScript: String = VerifierConf.startWorkerScript(conf)
  private val stopWorkerScript: String = VerifierConf.stopWorkerScript(conf)
  private val startMasterCmd = s"sh $startMasterScript"
  private val stopMasterCmd = s"sh $stopMasterScript"
  private val startWorkerCmd = s"sh $startWorkerScript"
  private val stopWorkerCmd = s"sh $stopWorkerScript"
  private val runnerStartMaster = (ctx: RunnerContext) => {
    ctx.resource.masterAlive = true
  }
  private val runnerStopMaster = (ctx: RunnerContext) => {
    ctx.resource.masterAlive = false
  }
  private val runnerStartWorker = (ctx: RunnerContext) => {
    ctx.resource.workerAlive = true
  }
  private val runnerStopWorker = (ctx: RunnerContext) => {
    ctx.resource.workerAlive = false
  }

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    val (cmd, block) =
      if (isMaster) {
        if (toStart) {
          (startMasterCmd, runnerStartMaster)
        } else {
          (stopMasterCmd, runnerStopMaster)
        }
      } else {
        if (toStart) {
          (startWorkerCmd, runnerStartWorker)
        } else {
          (stopWorkerCmd, runnerStopWorker)
        }
      }
    selector match {
      case randomSelector: RandomSelector =>
        randomSelector.updateSelectorStat(toStart, "")
      case _ =>
    }
    selector.select(context, target).map(it => new BashOperation(it, cmd, block, selector.interval))
  }
}

class StartMasterAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, true, true, interval) {

  override val target: String = ActionTarget.TARGET_MASTER
  override val tendency: Int = ActionTendency.TENDENCY_POSITIVE

  override def identity(): String = ActionIdentity.ACTION_START_MASTER
}

class StartWorkerAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, true, false, interval) {

  override val target: String = ActionTarget.TARGET_WORKER
  override val tendency: Int = ActionTendency.TENDENCY_POSITIVE

  override def identity(): String = ActionIdentity.ACTION_START_WORKER
}

class StopMasterAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, false, true, interval) {

  override val target: String = ActionTarget.TARGET_MASTER
  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_STOP_MASTER
}

class StopWorkerAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, false, false, interval) {

  override val target: String = ActionTarget.TARGET_WORKER
  override val tendency: Int = ActionTendency.TENDENCY_NEGATIVE

  override def identity(): String = ActionIdentity.ACTION_STOP_WORKER
}

object ActionTarget {
  val TARGET_CPU = "cpu"
  val TARGET_DISK = "disk"
  val TARGET_MASTER = "master"
  val TARGET_RUNNER = "runner"
  val TARGET_SCHEDULER = "scheduler"
  val TARGET_WORKER = "worker"
}

object ActionTendency {
  val TENDENCY_NEUTRAL: Int = 0
  val TENDENCY_POSITIVE: Int = 1
  val TENDENCY_NEGATIVE: Int = -1
}

object ActionIdentity {
  val ACTION_DISK_CORRUPT_DISK: String = "corrupt-disk"
  val ACTION_DISK_CORRUPT_META: String = "corrupt-meta"
  val ACTION_DISK_HANG_IO: String = "hang-io"
  val ACTION_OCCUPY_CPU: String = "occupy-cpu"
  val ACTION_RESUME_DISK: String = "resume-disk"
  val ACTION_RESUME_IO: String = "resume-io"
  val ACTION_START_MASTER: String = "start-master"
  val ACTION_START_WORKER: String = "start-worker"
  val ACTION_STOP_MASTER: String = "stop-master"
  val ACTION_STOP_WORKER: String = "stop-worker"
}

object Action {

  def parseJson(obj: JSONObject, conf: VerifierConf): Action = {
    val idOpt = Option(obj.getString("id"))
    if (idOpt.isEmpty) {
      throw new PlanInvalidException("Action must define id.")
    }
    val selectorObj = obj.getJSONObject("selector")
    val selector =
      if (selectorObj != null) {
        Selector.fromJson(selectorObj, conf)
      } else {
        Selector.dummySelector
      }
    val interval =
      if (selectorObj.containsKey("interval")) {
        Utils.timeStringAsMs(selectorObj.getString("interval"))
      } else {
        Utils.timeStringAsMs(VerifierConf.planActionDefaultInterval(conf))
      }
    idOpt.get match {
      case ActionIdentity.ACTION_DISK_CORRUPT_DISK => new CorruptDiskAction(selector, interval)
      case ActionIdentity.ACTION_DISK_CORRUPT_META => new CorruptMetaAction(selector, interval)
      case ActionIdentity.ACTION_DISK_HANG_IO => new HangIoAction(selector, interval)
      case ActionIdentity.ACTION_OCCUPY_CPU =>
        new OccupyCpuAction(
          selector,
          obj.getIntValue("cores"),
          Math.min(
            Utils.timeStringAsMs(Option(obj.getString("duration")).getOrElse("10s")),
            VerifierConf.planActionOccupyCpuMaxDurationMs(conf)),
          interval)
      case ActionIdentity.ACTION_RESUME_DISK => new ResumeDiskAction(selector, interval)
      case ActionIdentity.ACTION_RESUME_IO => new ResumeIoAction(selector, interval)
      case ActionIdentity.ACTION_START_MASTER => new StartMasterAction(selector, conf, interval)
      case ActionIdentity.ACTION_START_WORKER => new StartWorkerAction(selector, conf, interval)
      case ActionIdentity.ACTION_STOP_MASTER => new StopMasterAction(selector, conf, interval)
      case ActionIdentity.ACTION_STOP_WORKER => new StopWorkerAction(selector, conf, interval)
      case unknownAction: String =>
        throw new PlanInvalidException(
          s"Unknown action: $unknownAction, verification plan is invalid.")
    }
  }
}
