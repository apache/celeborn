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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEnv}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.protocol.RpcNameConstants

object Cli extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logInfo(
        """
          |Usage: schedulerHost schedulerPort command args*
          |Available commands:
          |pause
          |resume
          |submit
          |report
          |""".stripMargin)
    }
    val (host, port, command) = (args(0), args(1).toInt, args(2))
    val conf = new VerifierConf()
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.CLI_SYS,
      TransportModuleConstants.RPC_APP_CLIENT_MODULE,
      Utils.localHostName(conf.celebornConf),
      0,
      conf.celebornConf,
      Role.CLIENT,
      Option.empty)
    val actualHost =
      if (host == "localhost") {
        Utils.localHostName(conf.celebornConf)
      } else {
        host
      }
    val schedulerEndpoint =
      rpcEnv.setupEndpointRef(RpcAddress(actualHost, port), RpcNameConstants.SCHEDULER_EP)
    val commandToRun = command match {
      case Command.COMMAND_PAUSE => PausePlanCommand(schedulerEndpoint)
      case Command.COMMAND_REPORT => ReportCommand(schedulerEndpoint)
      case Command.COMMAND_RESUME => ResumePlanCommand(schedulerEndpoint)
      case Command.COMMAND_STOP => StopCommand(schedulerEndpoint)
      case Command.COMMAND_SUBMIT => SubmitCommand(schedulerEndpoint)
    }
    commandToRun.init(args.slice(3, args.length))
    commandToRun.execute(CommandContext(conf))
  }
}
