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

package org.apache.celeborn.service.deploy.master.http.api

import java.lang.{StringBuilder => JStringBuilder}
import javax.ws.rs.{FormParam, GET, Path, POST}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.WorkerEventType
import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Tag(name = "Deprecated")
@Path("/")
class ApiMasterResource extends ApiRequestContext {

  @Path("/masterGroupInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "List master group information of the service. It will list all master's LEADER, FOLLOWER information.")
  @GET
  def masterGroupInfo: String = httpService.getMasterGroupInfo

  @Path("/lostWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all lost workers of the master.")
  @GET
  def lostWorkers: String = httpService.getLostWorkers

  @Path("/excludedWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all excluded workers of the master.")
  @GET
  def excludedWorkers: String = httpService.getExcludedWorkers

  @Path("/shutdownWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all shutdown workers of the master.")
  @GET
  def shutdownWorkers: String = httpService.getShutdownWorkers

  @Path("/decommissionWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all decommission workers of the master.")
  @GET
  def decommissionWorkers: String = httpService.getDecommissionWorkers

  @Path("/hostnames")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all running application's LifecycleManager's hostnames of the cluster.")
  @GET
  def hostnames: String = httpService.getHostnameList

  @Path("/workerEventInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all worker event infos of the master.")
  @GET
  def workerEventInfo: String = httpService.getWorkerEventInfo()

  @Path("/exclude")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_FORM_URLENCODED)),
    description =
      "Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove, which value is separated by commas.")
  @POST
  def exclude(
      @FormParam("add") addWorkers: String,
      @FormParam("remove") removeWorkers: String): String = {
    val sb = new JStringBuilder
    sb.append("============================ Add/Remove Excluded Workers  Manually =============================\n")
    val workersToAdd =
      normalizeParam(addWorkers).split(",").filter(_.nonEmpty).map(WorkerInfo.fromUniqueId).toList
    val workersToRemove =
      normalizeParam(removeWorkers).split(",").filter(_.nonEmpty).map(
        WorkerInfo.fromUniqueId).toList
    sb.append(httpService.exclude(workersToAdd, workersToRemove)._2)
    sb.toString()
  }

  @Path("/sendWorkerEvent")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_FORM_URLENCODED)),
    description =
      "For Master(Leader) can send worker event to manager workers. Legal types are 'None', 'Immediately', 'Decommission', 'DecommissionThenIdle', 'Graceful', 'Recommission', and the parameter workers is separated by commas.")
  @POST
  def sendWorkerEvent(
      @FormParam("type") eventType: String,
      @FormParam("workers") workers: String): String = {
    val sb = new JStringBuilder
    if (StringUtils.isEmpty(eventType) || StringUtils.isEmpty(workers)) {
      return sb.append(
        s"handle eventType failed as eventType: $eventType or workers: $workers has empty value").toString()
    }
    sb.append("============================ Handle Worker Event =============================\n")
    val workerList = workers.split(",").filter(_.nonEmpty).map(WorkerInfo.fromUniqueId)
    sb.append(httpService.handleWorkerEvent(
      WorkerEventType.valueOf(normalizeParam(eventType)),
      workerList)._2)
    sb.toString()
  }

}
