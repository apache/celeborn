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

package org.apache.celeborn.verifier.runner

import java.util.UUID
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.verifier.action.Operation
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.info.NodeStatus
import org.apache.celeborn.verifier.protocol._

class Runner(override val rpcEnv: RpcEnv, val conf: VerifierConf)
  extends RpcEndpoint with Logging {

  private val heartBeater =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("verifier-runner-heart-beater")
  private val heartBeatDelayTime = VerifierConf.runnerHeartBeatDelayMs(conf)
  private val heartBeatIntervalTime = VerifierConf.runnerHeartBeatIntervalMs(conf)
  private val runnerId: String = UUID.randomUUID().toString
  logInfo(s"Current runner node id: $runnerId.")
  private val registerRetryTime = VerifierConf.runnerRegisterRetryDelayMs(conf)
  private val currentResource = new AtomicReference[NodeStatus](NodeStatus(conf))
  private var schedulerRef: RpcEndpointRef = _
  private var heartBeatTask: ScheduledFuture[_] = _

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ExecuteOperation(operation) => handleOperation(operation, context)
  }

  private def handleOperation(operation: Operation, context: RpcCallContext): Unit = {
    logDebug(s"Execute $operation at ${System.currentTimeMillis()}.")
    operation.executeOnRunner(RunnerContext(conf, currentResource.get()))
    context.reply(ExecuteOperationResponse(
      StatusCode.SUCCESS,
      null,
      this.currentResource.get(),
      this.runnerId))
  }

  override def onStart(): Unit = {
    schedulerRef = rpcEnv.setupEndpointRef(
      VerifierConf.schedulerAddress(conf),
      RpcNameConstants.SCHEDULER_EP)
    var registerRunnerResponse = schedulerRef.askSync[RegisterRunnerResponse](RegisterRunner(
      runnerId,
      this.currentResource.get()))
    if (registerRunnerResponse.statusCode != StatusCode.SUCCESS) {
      Thread.sleep(registerRetryTime)
      registerRunnerResponse = schedulerRef
        .askSync[RegisterRunnerResponse](RegisterRunner(runnerId, this.currentResource.get()))
      if (registerRunnerResponse.statusCode != StatusCode.SUCCESS) {
        sys.error("Register runner to scheduler failed, runner abort.")
        sys.exit(1)
      }
    }
    logInfo("Register runner to scheduler successfully.")
    heartBeatTask = heartBeater.scheduleAtFixedRate(
      () => {
        Utils.tryLogNonFatalError {
          val oldResource = currentResource.getAndSet(NodeStatus(conf))
          schedulerRef.send(RunnerHeartBeat(
            runnerId,
            NodeStatus(conf),
            NodeStatus(conf).except(oldResource)))
          logDebug("Check resource and send heartbeat.")
        }
      },
      heartBeatDelayTime,
      heartBeatIntervalTime,
      TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    if (heartBeatTask != null) {
      heartBeatTask.cancel(true)
    }
    heartBeater.shutdownNow()
  }
}

object Runner {

  def main(args: Array[String]): Unit = {
    val conf = new VerifierConf()
    val runnerArgs = new RunnerArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.RUNNER_SYS,
      TransportModuleConstants.RPC_APP_CLIENT_MODULE,
      runnerArgs.host,
      runnerArgs.port,
      conf.celebornConf,
      Role.CLIENT,
      Option.empty)
    rpcEnv.setupEndpoint(RpcNameConstants.RUNNER_EP, new Runner(rpcEnv, conf))
    rpcEnv.awaitTermination()
  }
}
