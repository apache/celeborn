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

import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.FileUtils
import picocli.CommandLine.ExitCode

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.cli.config.CliConfigManager

class CelebornCliExitCodeSuite extends CelebornFunSuite {
  private var configDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    configDir = Files.createTempDirectory("celeborn-cli-exit-code")
    sys.props.put("celeborn.testing.cli.config.file", configDir.resolve("cli.conf").toString)
  }

  override def afterAll(): Unit = {
    try {
      sys.props.remove("celeborn.testing.cli.config.file")
      if (configDir != null) {
        FileUtils.deleteDirectory(configDir.toFile)
      }
    } finally {
      super.afterAll()
    }
  }

  private def runCli(args: String*): Int = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val command = Seq(
      java,
      "-Dceleborn.testing=true",
      s"-Dceleborn.testing.cli.config.file=${CliConfigManager.cliConfigFilePath}",
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.celeborn.cli.CelebornCli") ++ args
    val process = new ProcessBuilder(command: _*)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .start()
    try {
      assert(process.waitFor(30, TimeUnit.SECONDS), "CLI process timed out")
      process.exitValue()
    } finally {
      process.destroyForcibly()
    }
  }

  test("CLI returns success for help") {
    assert(CelebornCli.execute(Array("--help")) == ExitCode.OK)
  }

  test("CLI exits with picocli's usage error status for an unknown option") {
    assert(runCli("--unknown-option") == ExitCode.USAGE)
  }

  test("CLI returns a usage error when no subcommand is provided") {
    assert(CelebornCli.execute(Array.empty[String]) == ExitCode.USAGE)
  }

  test("CLI returns an execution error when the server is unreachable") {
    assert(CelebornCli.execute(
      Array("master", "--show-workers", "--hostport", "127.0.0.1:1")) == ExitCode.SOFTWARE)
  }

  test("CLI returns an execution error when the master rejects an operation with HTTP 200") {
    val success = new AtomicBoolean(false)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val response = s"""{"success":${success.get()},"message":"Master response"}"""
            .getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length)
          try {
            exchange.getResponseBody.write(response)
          } finally {
            exchange.close()
          }
        }
      })
    server.start()
    try {
      val workerId = "localhost:1000:1001:1002:1003"
      val commands = Seq(
        Seq("--exclude-worker", "--worker-ids", workerId),
        Seq("--remove-excluded-worker", "--worker-ids", workerId),
        Seq("--remove-workers-unavailable-info", "--worker-ids", workerId),
        Seq("--send-worker-event", "RECOMMISSION", "--worker-ids", workerId),
        Seq("--update-interruption-notices", s"$workerId=123"),
        Seq("--unregister-shuffles", "--apps", "app1", "--shuffleIds", "1"))
      commands.foreach { command =>
        val args = Seq("master", "--hostport", s"127.0.0.1:${server.getAddress.getPort}") ++ command
        success.set(false)
        assert(CelebornCli.execute(args.toArray) == ExitCode.SOFTWARE, command.mkString(" "))
        success.set(true)
        assert(CelebornCli.execute(args.toArray) == ExitCode.OK, command.mkString(" "))
      }
    } finally {
      server.stop(0)
    }
  }

  test("CLI returns an execution error when cluster alias configuration cannot be saved") {
    val invalidParent = Files.createFile(configDir.resolve("regular-file"))
    val configPath = CliConfigManager.cliConfigFilePath
    sys.props.put("celeborn.testing.cli.config.file", invalidParent.resolve("cli.conf").toString)
    try {
      assert(CelebornCli.execute(Array(
        "master",
        "--add-cluster-alias",
        "test",
        "--host-list",
        "localhost:9098")) == ExitCode.SOFTWARE)
      assert(CelebornCli.execute(Array(
        "master",
        "--remove-cluster-alias",
        "test")) == ExitCode.SOFTWARE)
    } finally {
      sys.props.put("celeborn.testing.cli.config.file", configPath)
    }
  }

  test("CLI config path override is only honored in testing mode") {
    val testing = sys.props.remove("celeborn.testing")
    try {
      assert(CliConfigManager.cliConfigFilePath == s"${sys.env("HOME")}/.celeborn-cli.conf")
    } finally {
      testing.foreach(value => sys.props.put("celeborn.testing", value))
    }
    assert(CliConfigManager.cliConfigFilePath == configDir.resolve("cli.conf").toString)
  }
}
