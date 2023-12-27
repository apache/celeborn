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

import org.apache.celeborn.server.common.{HttpService, Service}

/**
 * HTTP endpoints of Rest API providers.
 */
trait HttpEndpoint {
  def path: String

  def description(service: String): String

  def handle(service: HttpService, parameters: Map[String, String]): String
}

case object Conf extends HttpEndpoint {
  override def path: String = "/conf"

  override def description(service: String): String = s"List the conf setting of the $service."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getConf
}

case object WorkerInfo extends HttpEndpoint {
  override def path: String = "/workerInfo"

  override def description(service: String): String = {
    if (service == Service.MASTER)
      "List worker information of the service. It will list all registered workers 's information."
    else "List the worker information of the worker."
  }

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getWorkerInfo
}

case object ThreadDump extends HttpEndpoint {
  override def path: String = "/threadDump"

  override def description(service: String): String =
    s"List the current thread dump of the $service."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getThreadDump
}

case object Shuffles extends HttpEndpoint {
  override def path: String = "/shuffles"

  override def description(service: String): String = {
    if (service == Service.MASTER)
      "List all running shuffle keys of the service. It will return all running shuffle's key of the cluster."
    else
      "List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker."
  }

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getShuffleList
}

case object Applications extends HttpEndpoint {
  override def path: String = "/applications"

  override def description(service: String): String =
    if (service == Service.MASTER)
      "List all running application's ids of the cluster."
    else
      "List all running application's ids of the worker. It only return application ids running in that worker."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getApplicationList
}

case object ListTopDiskUsedApps extends HttpEndpoint {
  override def path: String = "/listTopDiskUsedApps"

  override def description(service: String): String = {
    if (service == Service.MASTER)
      "List the top disk usage application ids. It will return the top disk usage application ids for the cluster."
    else
      "List the top disk usage application ids. It only return application ids running in that worker."
  }

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.listTopDiskUseApps
}

case object Help extends HttpEndpoint {
  override def path: String = "/help"

  override def description(service: String): String =
    s"List the available API providers of the $service."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    HttpUtils.help(service.serviceName)
}

case object Invalid extends HttpEndpoint {

  val invalid = "invalid"

  override def path: String = None.toString

  override def description(service: String): String = s"Invalid uri of the $service."

  override def handle(service: HttpService, parameters: Map[String, String]): String = invalid
}

case object MasterGroupInfo extends HttpEndpoint {
  override def path: String = "/masterGroupInfo"

  override def description(service: String): String =
    "List master group information of the service. It will list all master's LEADER, FOLLOWER information."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getMasterGroupInfo
}

case object LostWorkers extends HttpEndpoint {
  override def path: String = "/lostWorkers"

  override def description(service: String): String = "List all lost workers of the master."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getLostWorkers
}

case object ExcludedWorkers extends HttpEndpoint {
  override def path: String = "/excludedWorkers"

  override def description(service: String): String = "List all excluded workers of the master."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getExcludedWorkers
}

case object ShutdownWorkers extends HttpEndpoint {
  override def path: String = "/shutdownWorkers"

  override def description(service: String): String = "List all shutdown workers of the master."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getShutdownWorkers
}

case object Hostnames extends HttpEndpoint {
  override def path: String = "/hostnames"

  override def description(service: String): String =
    "List all running application's LifecycleManager's hostnames of the cluster."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getHostnameList
}

case object Exclude extends HttpEndpoint {
  override def path: String = "/exclude"

  override def description(service: String): String =
    "Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove, which value is separated by commas."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.exclude(parameters.getOrElse("ADD", "").trim, parameters.getOrElse("REMOVE", "").trim)
}

case object ListPartitionLocationInfo extends HttpEndpoint {
  override def path: String = "/listPartitionLocationInfo"

  override def description(service: String): String =
    "List all the living PartitionLocation information in that worker."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.listPartitionLocationInfo
}

case object UnavailablePeers extends HttpEndpoint {
  override def path: String = "/unavailablePeers"

  override def description(service: String): String =
    "List the unavailable peers of the worker, this always means the worker connect to the peer failed."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.getUnavailablePeers
}

case object IsShutdown extends HttpEndpoint {
  override def path: String = "/isShutdown"

  override def description(service: String): String =
    "Show if the worker is during the process of shutdown."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.isShutdown
}

case object IsRegistered extends HttpEndpoint {
  override def path: String = "/isRegistered"

  override def description(service: String): String =
    "Show if the worker is registered to the master success."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.isRegistered
}

case object Exit extends HttpEndpoint {
  override def path: String = "/exit"

  override def description(service: String): String =
    "Trigger this worker to exit. Legal types are 'DECOMMISSION', 'GRACEFUL' and 'IMMEDIATELY'."

  override def handle(service: HttpService, parameters: Map[String, String]): String =
    service.exit(parameters.getOrElse("TYPE", ""))
}
