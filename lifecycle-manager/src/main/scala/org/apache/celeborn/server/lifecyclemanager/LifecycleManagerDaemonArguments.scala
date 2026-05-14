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

private[lifecyclemanager] object LifecycleManagerDaemonArguments {

  private val MIN_USER_PORT = 1024

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

      case ("--host" | "-h") :: value :: tail =>
        host = Some(value)
        doParse(tail)

      case "--properties-file" :: value :: tail =>
        propertiesFile = Some(value)
        doParse(tail)

      case "--help" :: _ =>
        // scalastyle:off println
        System.err.println(usage)
        // scalastyle:on println
        sys.exit(0)

      case Nil => // done

      case unknown :: _ =>
        // scalastyle:off println
        System.err.println(s"Unknown argument: $unknown")
        System.err.println(usage)
        // scalastyle:on println
        sys.exit(1)
    }

    doParse(args.toList)

    if (appId.isEmpty) {
      // scalastyle:off println
      System.err.println("Error: --app-id is required.")
      System.err.println(usage)
      // scalastyle:on println
      sys.exit(1)
    }
    if (masterEndpoints.isEmpty) {
      // scalastyle:off println
      System.err.println("Error: --master-endpoints is required.")
      System.err.println(usage)
      // scalastyle:on println
      sys.exit(1)
    }
    if (port.isEmpty) {
      // scalastyle:off println
      System.err.println("Error: --port is required.")
      System.err.println(usage)
      // scalastyle:on println
      sys.exit(1)
    }
    if (port.get < MIN_USER_PORT) {
      // scalastyle:off println
      System.err.println(s"Error: --port must be >= $MIN_USER_PORT, got ${port.get}.")
      System.err.println(usage)
      // scalastyle:on println
      sys.exit(1)
    }

    LifecycleManagerDaemonArguments(
      appId = appId.get,
      masterEndpoints = masterEndpoints.get,
      port = port.get,
      host = host,
      propertiesFile = propertiesFile)
  }

  val usage: String =
    """Usage: LifecycleManagerDaemon [options]
      |
      |Options:
      |  --app-id ID                  Application unique identifier (required)
      |  --master-endpoints ENDPOINTS Comma-separated master host:port list (required)
      |  -p PORT, --port PORT         Port for LifecycleManager to listen on (required, >= 1024)
      |  -h HOST, --host HOST         Hostname to bind (optional, default: auto-detect)
      |  --properties-file FILE       Path to a custom Celeborn properties file,
      |                               default is conf/celeborn-defaults.conf
      |""".stripMargin
}
