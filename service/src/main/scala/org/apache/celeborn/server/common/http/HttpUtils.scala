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

import java.net.URL
import java.util.Locale

import org.apache.celeborn.server.common.{HttpService, Service}

object HttpUtils {

  private val baseEndpoints: List[HttpEndpoint] =
    List(Conf, WorkerInfo, ThreadDump, Shuffles, Applications, ListTopDiskUsedApps, Help)
  private val masterEndpoints: List[HttpEndpoint] = List(
    MasterGroupInfo,
    LostWorkers,
    ExcludedWorkers,
    ShutdownWorkers,
    Hostnames,
    Exclude) ++ baseEndpoints
  private val workerEndpoints: List[HttpEndpoint] =
    List(
      ListPartitionLocationInfo,
      UnavailablePeers,
      IsShutdown,
      IsRegistered,
      Exit) ++ baseEndpoints

  def parseUri(uri: String): (String, Map[String, String]) = {
    val url = new URL(s"https://127.0.0.1:9000$uri")
    val parameter =
      if (url.getQuery == null) {
        Map.empty[String, String]
      } else {
        url.getQuery
          .split("&")
          .map(_.split("="))
          .map(arr => arr(0).toUpperCase(Locale.ROOT) -> arr(1)).toMap
      }
    (url.getPath, parameter)
  }

  def handleRequest(
      service: HttpService,
      path: String,
      parameters: Map[String, String]): String = {
    endpoints(service.serviceName).find(endpoint => endpoint.path == path).orElse(
      Some(Invalid)).get.handle(
      service,
      parameters)
  }

  def help(service: String): String = {
    val sb = new StringBuilder
    sb.append("Available API providers include:\n")
    val httpEndpoints: List[HttpEndpoint] = endpoints(service)
    val maxLength = httpEndpoints.map(_.path.length).max
    httpEndpoints.sortBy(_.path).foreach(endpoint =>
      sb.append(
        s"${endpoint.path.padTo(maxLength, " ").mkString} ${endpoint.description(service)}\n"))
    sb.toString
  }

  private def endpoints(service: String): List[HttpEndpoint] = {
    if (service == Service.MASTER)
      masterEndpoints
    else
      workerEndpoints
  }
}
