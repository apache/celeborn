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

package org.apache.celeborn.server.lifecyclemanager

import scala.annotation.tailrec

import org.apache.celeborn.common.util.IntParam

private[lifecyclemanager] case class LifecycleManagerDaemonArguments(
    appId: String,
    masterEndpoints: String,
    port: Int,
    host: Option[String],
    propertiesFile: Option[String])

/**
 * Signals that argument parsing requested termination. Carries the intended
 * process exit code so callers (e.g. `main`) can exit, while unit tests can
 * assert on the code and message without the JVM actually shutting down.
 *
 * `exitCode == 0` denotes a successful, user-requested `--help`.
 */
private[lifecyclemanager] class ArgumentParseException(
    val exitCode: Int,
    message: String) extends RuntimeException(message)

private[lifecyclemanager] object LifecycleManagerDaemonArguments {

  private val MIN_USER_PORT = 1024

  /**
   * Pure parser: validates `args` and either returns the parsed arguments or
   * throws [[ArgumentParseException]]. It performs no I/O and never calls
   * `sys.exit`, so every branch (help / unknown / missing-arg / bad-port) is
   * unit-testable. Use [[parseOrExit]] for the process entry point.
   */
  def parse(args: Array[String]): LifecycleManagerDaemonArguments = {
    var appId: Option[String] = None
    var masterEndpoints: Option[String] = None
    var port: Option[Int] = None
    var host: Option[String] = None
    var propertiesFile: Option[String] = None

    @tailrec
    def doParse(remaining: List[String]): Unit = remaining match {
      case "--app-id" :: value :: tail =>
        appId = Some(value)
        doParse(tail)

      case "--master-endpoints" :: value :: tail =>
        masterEndpoints = Some(value)
        doParse(tail)

      case ("--port" | "-p") :: IntParam(value) :: tail =>
        port = Some(value)
        doParse(tail)

      // Only the long form is accepted for host: `-h` conventionally means
      // help, so reserving it for --host would be surprising.
      case "--host" :: value :: tail =>
        host = Some(value)
        doParse(tail)

      case "--properties-file" :: value :: tail =>
        propertiesFile = Some(value)
        doParse(tail)

      case ("--help" | "-h") :: _ =>
        throw new ArgumentParseException(0, usage)

      case Nil => // done

      case unknown :: _ =>
        throw new ArgumentParseException(1, s"Unknown argument: $unknown\n$usage")
    }

    doParse(args.toList)

    if (appId.isEmpty) {
      throw new ArgumentParseException(1, s"Error: --app-id is required.\n$usage")
    }
    if (masterEndpoints.isEmpty) {
      throw new ArgumentParseException(1, s"Error: --master-endpoints is required.\n$usage")
    }
    if (port.isEmpty) {
      throw new ArgumentParseException(1, s"Error: --port is required.\n$usage")
    }
    if (port.get < MIN_USER_PORT) {
      throw new ArgumentParseException(
        1,
        s"Error: --port must be >= $MIN_USER_PORT, got ${port.get}.\n$usage")
    }

    LifecycleManagerDaemonArguments(
      appId = appId.get,
      masterEndpoints = masterEndpoints.get,
      port = port.get,
      host = host,
      propertiesFile = propertiesFile)
  }

  /**
   * Process entry point wrapper around [[parse]]: prints the message carried by
   * an [[ArgumentParseException]] and exits with its code.
   */
  def parseOrExit(args: Array[String]): LifecycleManagerDaemonArguments = {
    try {
      parse(args)
    } catch {
      case e: ArgumentParseException =>
        // scalastyle:off println
        System.err.println(e.getMessage)
        // scalastyle:on println
        sys.exit(e.exitCode)
    }
  }

  val usage: String =
    """Usage: LifecycleManagerDaemon [options]
      |
      |Options:
      |  --app-id ID                  Application unique identifier (required)
      |  --master-endpoints ENDPOINTS Comma-separated master host:port list (required)
      |  -p PORT, --port PORT         Port for LifecycleManager to listen on (required, >= 1024)
      |  --host HOST                  Hostname to bind (optional, default: auto-detect)
      |  --properties-file FILE       Path to a custom Celeborn properties file,
      |                               default is conf/celeborn-defaults.conf
      |  -h, --help                   Print this help message
      |""".stripMargin
}
