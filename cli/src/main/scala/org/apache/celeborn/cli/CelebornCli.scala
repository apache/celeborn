package org.apache.celeborn.cli
import picocli.CommandLine
import picocli.CommandLine.{Command, ScopeType}
import picocli.CommandLine.Help.Ansi.Style
import picocli.CommandLine.Help.ColorScheme
@Command(
  name = "celeborn-cli",
  version = Array("CelebornCli v1"),
  mixinStandardHelpOptions = true,
  description = Array("@|bold Scala|@ Celeborn CLI"),
  subcommands = Array(
    classOf[MasterSubcommand]))
class CelebornCli extends Runnable {
  override def run(): Unit = {
    println("subcommand needed!")
  }
}

object CelebornCli {
  def main(args: Array[String]): Unit = {
    val cmd = new CommandLine(new CelebornCli())
    cmd.setOptionsCaseInsensitive(false)
    cmd.setSubcommandsCaseInsensitive(false)
    System.exit(new CommandLine(new CelebornCli()).execute(args: _*))
  }
}
