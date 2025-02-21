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

package org.apache.celeborn.verifier.cli

import scala.io.Source

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.verifier.cli.Command.verifyResult
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.plan.Parser
import org.apache.celeborn.verifier.protocol._

trait Command extends Logging {

  def init(args: Array[String]): Unit

  def execute(commandContext: CommandContext): Unit
}

object Command extends Logging {

  val COMMAND_PAUSE: String = "pause"
  val COMMAND_REPORT: String = "report"
  val COMMAND_RESUME: String = "resume"
  val COMMAND_STOP: String = "stop"
  val COMMAND_SUBMIT: String = "submit"

  def verifyResult(response: ResponseMessage, commandName: String): Unit = {
    if (StatusCode.SUCCESS == response.statusCode) {
      logInfo(s"Command $commandName success.")
    } else {
      logError(s"Command $commandName failed.", response.exception)
    }
  }
}

case class CommandContext(conf: VerifierConf)

case class PausePlanCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    verifyResult(rpcEndpointRef.askSync[PausePlanResponse](PausePlan()), Command.COMMAND_PAUSE)
  }
}

case class ReportCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    val queryStatusResponse = rpcEndpointRef.askSync[QueryStatusResponse](QueryStatus())
    verifyResult(queryStatusResponse, Command.COMMAND_REPORT)
    if (queryStatusResponse.report != null) {
      logInfo(queryStatusResponse.report)
    }
  }
}

case class ResumePlanCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    verifyResult(rpcEndpointRef.askSync[ResumePlanResponse](ResumePlan()), Command.COMMAND_RESUME)
  }
}

case class StopCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    verifyResult(rpcEndpointRef.askSync[StopPlanResponse](StopPlan()), Command.COMMAND_STOP)
  }
}

case class SubmitCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  private var planFileLocation: String = _

  override def init(args: Array[String]): Unit = {
    assert(args.length >= 0)
    planFileLocation = args(0)
  }

  override def execute(commandContext: CommandContext): Unit = {
    var source: Source = null
    try {
      source = Source.fromFile(planFileLocation)
      val plan = Parser.parse(source.mkString, commandContext.conf)
      logDebug(s"Current verification plan: ${plan.toString}.")
      verifyResult(
        rpcEndpointRef.askSync[SubmitPlanResponse](SubmitPlan(plan)),
        Command.COMMAND_SUBMIT)
    } catch {
      case e: Exception =>
        logError("Parse verification plan failed.", e)
    } finally {
      if (source != null) {
        source.close()
      }
    }
  }
}
