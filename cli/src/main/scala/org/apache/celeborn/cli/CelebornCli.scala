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

package org.apache.celeborn.cli
import picocli.CommandLine
import picocli.CommandLine.{Command, ParameterException, Spec}
import picocli.CommandLine.Model.CommandSpec

import org.apache.celeborn.cli.common.BaseCommand
import org.apache.celeborn.cli.master.MasterSubcommandImpl
import org.apache.celeborn.cli.worker.WorkerSubcommandImpl
@Command(
  name = "celeborn-cli",
  description = Array("@|bold Scala|@ Celeborn CLI"),
  subcommands = Array(
    classOf[MasterSubcommandImpl],
    classOf[WorkerSubcommandImpl]))
class CelebornCli extends BaseCommand {
  @Spec
  private var spec: CommandSpec = _

  override def run(): Unit = {
    throw new ParameterException(
      spec.commandLine(),
      "Master or Worker subcommand needs to be provided. Please run -h to see the usage info.")
  }
}

object CelebornCli {
  def main(args: Array[String]): Unit = {
    System.exit(execute(args))
  }

  private[cli] def execute(args: Array[String]): Int = {
    val cmd = new CommandLine(new CelebornCli())
    cmd.setOptionsCaseInsensitive(false)
    cmd.setSubcommandsCaseInsensitive(false)
    cmd.execute(args: _*)
  }
}
