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

package org.apache.celeborn.server.common.http

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.Service

class HttpUtilsSuite extends AnyFunSuite with Logging {

  def checkParseUri(
      uri: String,
      expectPath: String,
      expectParameters: Map[String, String]): Unit = {
    val (path, parameters) = HttpUtils.parseUri(uri)
    assert(path == expectPath)
    assert(parameters == expectParameters)
  }

  test("CELEBORN-448: Support exclude worker manually") {
    checkParseUri("/exclude", "/exclude", Map.empty)
    checkParseUri(
      "/exclude?add=localhost:1001:1002:1003:1004",
      "/exclude",
      Map("ADD" -> "localhost:1001:1002:1003:1004"))
    checkParseUri(
      "/exclude?remove=localhost:1001:1002:1003:1004",
      "/exclude",
      Map("REMOVE" -> "localhost:1001:1002:1003:1004"))
    checkParseUri(
      "/exclude?add=localhost:1001:1002:1003:1004&remove=localhost:2001:2002:2003:2004",
      "/exclude",
      Map("ADD" -> "localhost:1001:1002:1003:1004", "REMOVE" -> "localhost:2001:2002:2003:2004"))
  }

  test("CELEBORN-847: Support parse HTTP Restful API parameters") {
    checkParseUri("/exit", "/exit", Map.empty)
    checkParseUri("/exit?type=decommission", "/exit", Map("TYPE" -> "decommission"))
    checkParseUri(
      "/exit?type=decommission&foo=A",
      "/exit",
      Map("TYPE" -> "decommission", "FOO" -> "A"))
  }

  test("CELEBORN-829: Improve response message of invalid HTTP request") {
    assert(HttpUtils.help(Service.MASTER) ==
      s"""Available API providers include:
         |/applications        List all running application's ids of the cluster.
         |/conf                List the conf setting of the master.
         |/exclude             Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove, which value is separated by commas.
         |/excludedWorkers     List all excluded workers of the master.
         |/help                List the available API providers of the master.
         |/hostnames           List all running application's LifecycleManager's hostnames of the cluster.
         |/listTopDiskUsedApps List the top disk usage application ids. It will return the top disk usage application ids for the cluster.
         |/lostWorkers         List all lost workers of the master.
         |/masterGroupInfo     List master group information of the service. It will list all master's LEADER, FOLLOWER information.
         |/shuffles            List all running shuffle keys of the service. It will return all running shuffle's key of the cluster.
         |/shutdownWorkers     List all shutdown workers of the master.
         |/threadDump          List the current thread dump of the master.
         |/workerInfo          List worker information of the service. It will list all registered workers 's information.
         |""".stripMargin)
    assert(HttpUtils.help(Service.WORKER) ==
      s"""Available API providers include:
         |/applications              List all running application's ids of the worker. It only return application ids running in that worker.
         |/conf                      List the conf setting of the worker.
         |/exit                      Trigger this worker to exit. Legal types are 'DECOMMISSION', 'GRACEFUL' and 'IMMEDIATELY'.
         |/help                      List the available API providers of the worker.
         |/isRegistered              Show if the worker is registered to the master success.
         |/isShutdown                Show if the worker is during the process of shutdown.
         |/listPartitionLocationInfo List all the living PartitionLocation information in that worker.
         |/listTopDiskUsedApps       List the top disk usage application ids. It only return application ids running in that worker.
         |/shuffles                  List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker.
         |/threadDump                List the current thread dump of the worker.
         |/unavailablePeers          List the unavailable peers of the worker, this always means the worker connect to the peer failed.
         |/workerInfo                List the worker information of the worker.
         |""".stripMargin)
  }
}
