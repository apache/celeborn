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

package org.apache.celeborn.verifier.scheduler

import java.util.concurrent.{ConcurrentHashMap, Future, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters.{enumerationAsScalaIteratorConverter, mapAsScalaConcurrentMapConverter}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration.Duration
import scala.util.Random

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.verifier.action.Operation
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.info.{NodeStatus, RunnerInfo}
import org.apache.celeborn.verifier.plan.VerificationPlan
import org.apache.celeborn.verifier.plan.exception.PlanExecutionException
import org.apache.celeborn.verifier.protocol._

class Scheduler(override val rpcEnv: RpcEnv, val conf: VerifierConf)
  extends RpcEndpoint with Logging {

  // key:runnerId -> (resource, time, rpcEndpoint)
  private val runnerInfos = new ConcurrentHashMap[String, RunnerInfo]
  private val planExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("verifier-scheduler-plan-executor")
  private val timeoutChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("verifier-scheduler-timeout-checker")
  private val resultExecutor =
    ThreadUtils.newDaemonFixedThreadPool(8, "verifier-scheduler-result-executor")
  private val timeoutThreshold = VerifierConf.runnerTimeOutMs(conf)
  private val stopPlanFlag = new AtomicBoolean(true)
  private val pausePlanFlag = new AtomicBoolean(false)
  private val runningPlan = new AtomicReference[VerificationPlan](null)
  private var planExecuteTask: Future[_] = _
  private var timeoutCheckTask: ScheduledFuture[_] = _

  override def onStart(): Unit = {

    class PlanLoop extends Runnable {
      override def run(): Unit = {
        try {
          implicit val contextExecutor: ExecutionContextExecutor =
            ExecutionContext.fromExecutor(resultExecutor)
          val schedulerContext = new SchedulerContext(conf, runnerInfos)
          while (true) {
            val plan = runningPlan.get()
            if (plan == null) {
              Thread.sleep(1000)
            } else {
              val trigger = plan.trigger
              val actions = plan.actions
              val checker = plan.checker
              val actionCount = actions.size
              var actionIndex = 0
              var planLoopCount = 1
              val tendentiousActions = actions.groupBy(_.tendency)
              while (!stopPlanFlag.get()) {
                if (pausePlanFlag.get()) {
                  Thread.sleep(1000L)
                }
                val actionToExecute =
                  if (trigger.random) {
                    val actionCandidates = tendentiousActions(checker.tendency)
                      .filter(ac => checker.availableTargets.contains(ac.target))
                    actionCandidates(Random.nextInt(actionCandidates.size))
                  } else {
                    logInfo(s"Plan loop: $planLoopCount.")
                    actions(actionIndex)
                  }
                val operations = actionToExecute.generateOperations(schedulerContext)
                logDebug(
                  s"Current action: $actionToExecute, operations: ${operations.map(_.toString)}.")
                val permissionToExecute =
                  if (operations.isEmpty) {
                    false
                  } else {
                    val deducedContext = actionToExecute.deduce(schedulerContext, operations)
                    val permissionToRun = checker.validate(deducedContext)
                    logDebug(s"Current context: ${schedulerContext.toString}  => deduced context: ${deducedContext.toString}, " +
                      s"current action executes $permissionToRun.")
                    permissionToRun
                  }
                if (permissionToExecute) {
                  def executeOperationSync(operation: Operation): Unit = {
                    val executeOperationFuture =
                      runnerInfos.get(operation.actionTarget.identity).rpcEndpointRef
                        .ask[ExecuteOperationResponse](ExecuteOperation(operation))
                    val executeOperationResponse =
                      ThreadUtils.awaitReady(executeOperationFuture, Duration.Inf).value.get.get
                    if (executeOperationResponse.statusCode != StatusCode.SUCCESS) {
                      throw new PlanExecutionException("Operation failed, plan execution abort.")
                    }
                    operation.updateSchedulerContext(schedulerContext)
                    Thread.sleep(operation.interval)
                  }
                  operations.foreach(executeOperationSync)
                  Thread.sleep(actionToExecute.interval)
                }
                if (trigger.sequence) {
                  actionIndex = actionIndex + 1
                  if (actionIndex >= actionCount - 1) {
                    planLoopCount = planLoopCount + 1
                    if (planLoopCount > trigger.repeat) {
                      logInfo("Plan execution completed, cleaning verification plan.")
                      stopPlanFlag.set(true)
                      runningPlan.set(null)
                    } else {
                      actionIndex = 0
                    }
                  }
                }
                Thread.sleep(trigger.interval.getInterval)
              }
            }
          }
        } catch {
          case t: Throwable =>
            try {
              logError("Plan executed with exception, cleaning verification plan.", t)
              planExecuteTask = planExecutor.submit(new PlanLoop)
              stopPlanFlag.set(true)
              runningPlan.set(null)
            } finally {
              throw t
            }
        }
      }
    }
    planExecuteTask = planExecutor.submit(new PlanLoop)
    timeoutCheckTask = timeoutChecker.schedule(
      new Runnable {
        override def run(): Unit = {
          self.send(CheckRunnerTimeout())
        }
      },
      30 * 1000,
      TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    if (planExecuteTask != null) {
      planExecuteTask.cancel(true)
    }
    planExecutor.shutdownNow()

    if (timeoutCheckTask != null) {
      timeoutCheckTask.cancel(true)
    }
    timeoutChecker.shutdownNow()
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CheckRunnerTimeout() => handleRunnerTimeout()
    case RunnerHeartBeat(uid, resource, diff) => handleRunnerHeartBeat(uid, resource, diff)
  }

  private def handleRunnerTimeout(): Unit = {
    val outdatedItems = runnerInfos.asScala.filter(k =>
      System.currentTimeMillis() - k._2.lastContactTime > timeoutThreshold).toList
    if (outdatedItems.nonEmpty) {
      logError(s"Fatal error occurred, but got $outdatedItems.")
      stopPlanFlag.set(true)
    }
  }

  private def handleRunnerHeartBeat(uid: String, resource: NodeStatus, diff: NodeStatus): Unit = {
    if (runnerInfos.containsKey(uid)) {
      if (!VerifierConf.runnerTestMode(conf)) {
        runnerInfos.get(uid).resource.checkAndUpdate(uid, resource)
      }
      runnerInfos.get(uid).lastContactTime = System.currentTimeMillis()
    } else {
      logTrace(s"Current all runnerInfos: ${runnerInfos.keys().asScala.toList}.")
      logWarning(s"Received unknown worker $uid.")
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterRunner(uid, resource) => handleRegisterRunner(uid, resource, context)
    case SubmitPlan(plan) => handleSubmitPlan(plan, context)
    case PausePlan() => handlePausePlan(context)
    case ResumePlan() => handleResumePlan(context)
    case StopPlan() => handleStopPlan(context)
    case QueryStatus() => handleQueryStatus(context)
  }

  private def handleRegisterRunner(
      uid: String,
      resource: NodeStatus,
      context: RpcCallContext): Unit = {
    runnerInfos.synchronized {
      val runnerEp = rpcEnv.setupEndpointRef(context.senderAddress, RpcNameConstants.RUNNER_EP)
      runnerInfos.put(uid, new RunnerInfo(resource, System.currentTimeMillis(), runnerEp))
      logTrace(s"Register runner: $uid.")
      context.reply(RegisterRunnerResponse(StatusCode.SUCCESS, null))
    }
  }

  private def handleSubmitPlan(plan: VerificationPlan, context: RpcCallContext): Unit = {
    if (this.runningPlan.get() != null || !this.stopPlanFlag.get()) {
      logWarning("There is a running plan already.")
      context.reply(SubmitPlanResponse(
        StatusCode.REQUEST_FAILED,
        new IllegalStateException("There is a active plan already, stopping it firstly.")))
    } else {
      this.stopPlanFlag.set(false)
      this.runningPlan.set(plan)
      context.reply(SubmitPlanResponse(StatusCode.SUCCESS, null))
      logInfo(s"Submit plan: ${plan.toString}.")
    }
  }

  private def handlePausePlan(context: RpcCallContext): Unit = {
    this.pausePlanFlag.set(true)
    context.reply(PausePlanResponse(StatusCode.SUCCESS, null))
  }

  private def handleResumePlan(context: RpcCallContext): Unit = {
    this.pausePlanFlag.set(false)
    context.reply(ResumePlanResponse(StatusCode.SUCCESS, null))
  }

  private def handleStopPlan(context: RpcCallContext): Unit = {
    this.stopPlanFlag.set(true)
    this.runningPlan.set(null)
    Thread.sleep(1000)
    context.reply(StopPlanResponse(StatusCode.SUCCESS, null))
  }

  private def handleQueryStatus(context: RpcCallContext): Unit = {
    val lineSeparate = System.lineSeparator()
    val report = new StringBuilder
    report.append("Verifier status report:")
    report.append(lineSeparate)
    if (runningPlan.get() != null) {
      report.append(s"Current active plan: ${runningPlan.get()}.")
    } else {
      report.append(s"Idle stat.")
    }
    report.append(lineSeparate)
    report.append(s"There are ${this.runnerInfos.size()} runners online:")
    report.append(lineSeparate)
    this.runnerInfos.asScala.foreach(it => {
      report.append(s"Runner: ${it._1}, info: ${it._2.toString}")
      report.append(lineSeparate)
    })
    context.reply(QueryStatusResponse(StatusCode.SUCCESS, null, report.toString()))
  }
}

object Scheduler {

  def main(args: Array[String]): Unit = {
    val conf = new VerifierConf()
    val schedulerArgs = new SchedulerArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.SCHEDULER_SYS,
      TransportModuleConstants.RPC_APP_CLIENT_MODULE,
      schedulerArgs.host,
      schedulerArgs.port,
      conf.celebornConf,
      Role.CLIENT,
      Option.empty)
    rpcEnv.setupEndpoint(RpcNameConstants.SCHEDULER_EP, new Scheduler(rpcEnv, conf))
    rpcEnv.awaitTermination()
  }
}
