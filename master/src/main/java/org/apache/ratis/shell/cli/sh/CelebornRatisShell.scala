package org.apache.ratis.shell.cli.sh

class CelebornRatisShell {
  def main(args: Array[String]): Unit = {
    val shell = new RatisShell(System.out)
    System.exit(shell.run(args: _*))
  }
}
