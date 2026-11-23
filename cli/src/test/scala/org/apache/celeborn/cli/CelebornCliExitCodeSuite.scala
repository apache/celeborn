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
import org.scalatest.funsuite.AnyFunSuite
import picocli.CommandLine.ExitCode

class CelebornCliExitCodeSuite extends AnyFunSuite {
  private def runCli(args: String*): Int = runCliWithHome(None, args: _*)

  private def runCliWithHome(home: Option[Path], args: String*): Int = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val command = Seq(
      java,
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.celeborn.cli.CelebornCli") ++ args
    val builder = new ProcessBuilder(command: _*)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
    home.foreach(path => builder.environment().put("HOME", path.toString))
    val process = builder.start()
    try {
      assert(process.waitFor(30, TimeUnit.SECONDS), "CLI process timed out")
      process.exitValue()
    } finally {
      process.destroyForcibly()
    }
  }

  test("CLI exits successfully for help") {
    assert(runCli("--help") == ExitCode.OK)
  }

  test("CLI exits with picocli's usage error status for an unknown option") {
    assert(runCli("--unknown-option") == ExitCode.USAGE)
  }

  test("CLI exits with a usage error when no subcommand is provided") {
    assert(runCli() == ExitCode.USAGE)
  }

  test("CLI exits with an execution error when the server is unreachable") {
    assert(runCli("master", "--show-workers", "--hostport", "127.0.0.1:1") == ExitCode.SOFTWARE)
  }

  test("CLI exits with an execution error when the master rejects an operation with HTTP 200") {
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
        assert(runCli(args: _*) == ExitCode.SOFTWARE, command.mkString(" "))
        success.set(true)
        assert(runCli(args: _*) == ExitCode.OK, command.mkString(" "))
      }
    } finally {
      server.stop(0)
    }
  }

  test("CLI exits with an execution error when cluster alias configuration cannot be saved") {
    val home = Files.createTempFile("celeborn-cli-home", ".tmp")
    try {
      assert(runCliWithHome(
        Some(home),
        "master",
        "--add-cluster-alias",
        "test",
        "--host-list",
        "localhost:9098") == ExitCode.SOFTWARE)
      assert(runCliWithHome(
        Some(home),
        "master",
        "--remove-cluster-alias",
        "test") == ExitCode.SOFTWARE)
    } finally {
      Files.delete(home)
    }
  }
}
