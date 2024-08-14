package org.apache.celeborn.cli

import picocli.CommandLine.{Command, Option, ParentCommand}

@Command(name = "master", mixinStandardHelpOptions = true)
class MasterSubcommand extends Runnable {

  @ParentCommand
  private var celebornCli: CelebornCli = _

  @Option(names = Array("--show-masters-info"), description = Array("Show master group info"))
  private var showMastersInfo: Boolean = _

  override def run(): Unit = {
    if (showMastersInfo) {
      println("showMastersInfo called!!")
    }
  }
}
