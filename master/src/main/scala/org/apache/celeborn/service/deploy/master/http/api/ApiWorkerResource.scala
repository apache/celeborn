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

import javax.ws.rs.{GET, Path, POST, QueryParam}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.celeborn.server.common.http.api.ApiRequestContext

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
  def masterGroupInfo: String = rs.getMasterGroupInfo

  @Path("/lostWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all lost workers of the master.")
  @GET
  def lostWorkers: String = rs.getLostWorkers

  @Path("/excludedWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all excluded workers of the master.")
  @GET
  def excludedWorkers: String = rs.getExcludedWorkers

  @Path("/shutdownWorkers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all shutdown workers of the master.")
  @GET
  def shutdownWorkers: String = rs.getShutdownWorkers

  @Path("/hostnames")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all running application's LifecycleManager's hostnames of the cluster.")
  @GET
  def hostnames: String = rs.getHostnameList

  @Path("/sendWorkerEvent")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "For Master(Leader) can send worker event to manager workers. Legal types are 'None', 'Immediately', 'Decommission', 'DecommissionThenIdle', 'Graceful', 'Recommission'")
  @POST
  def sendWorkerEvent(
      @QueryParam("TYPE") eventType: String = "",
      @QueryParam("WORKERS") workers: String = ""): String = {
    rs.handleWorkerEvent(eventType, workers)
  }

  @Path("/workerEventInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all worker event infos of the master.")
  @GET
  def workerEventInfo: String = rs.getWorkerEventInfo()

  @Path("/exclude")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all worker event infos of the master.")
  @POST
  def excludeWorkers(
      @QueryParam("ADD") addWorkers: String = "",
      @QueryParam("REMOVE") removeWorkers: String = ""): String = {
    rs.exclude(addWorkers, removeWorkers)
  }
}
