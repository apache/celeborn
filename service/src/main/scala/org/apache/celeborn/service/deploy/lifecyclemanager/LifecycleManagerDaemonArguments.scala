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

package org.apache.celeborn.service.deploy.lifecyclemanager

import scala.annotation.tailrec

import org.apache.celeborn.common.util.IntParam

private[lifecyclemanager] case class LifecycleManagerDaemonArguments(
    appId: String,
    masterEndpoints: String,
    port: Int,
    host: Option[String],
    propertiesFile: Option[String])

private[lifecyclemanager] object LifecycleManagerDaemonArguments {

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

      case "--port" :: IntParam(value) :: tail =>
        if (value < 1024) {
          // scalastyle:off println
          System.err.println(s"Error: --port must be >= 1024, got $value")
          System.err.println(usage)
          // scalastyle:on println
          sys.exit(1)
        }
        port = Some(value)
        doParse(tail)

      case "--host" :: value :: tail =>
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
        System.err.println(s"Error: Unknown argument '$unknown'")
        System.err.println(usage)
        // scalastyle:on println
        sys.exit(1)
    }

    doParse(args.toList)

    if (appId.isEmpty || masterEndpoints.isEmpty || port.isEmpty) {
      // scalastyle:off println
      System.err.println("Error: --app-id, --master-endpoints, and --port are required.")
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
      |  --app-id ID              REQUIRED  Unique application id
      |  --master-endpoints EPS   REQUIRED  Comma-separated host:port of Celeborn Masters
      |  --port PORT              REQUIRED  Fixed RPC port to bind (must be >= 1024)
      |  --host HOST              OPTIONAL  Bind host (default: local hostname)
      |  --properties-file FILE   OPTIONAL  Path to celeborn-defaults.conf
      |""".stripMargin
}
