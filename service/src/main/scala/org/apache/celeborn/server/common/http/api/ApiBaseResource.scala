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

package org.apache.celeborn.server.common.http.api

import javax.ws.rs.{GET, Path, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

@Tag(name = "Deprecated")
@Path("/")
private[api] class ApiBaseResource extends ApiRequestContext {
  def service: String = httpService.serviceName

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("/conf")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List the conf setting.")
  @GET
  def conf: String = httpService.getConf

  @Path("/listDynamicConfigs")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List the dynamic configs. " +
      "The parameter level specifies the config level of dynamic configs. " +
      "The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. " +
      "The parameter name specifies the user name of TENANT_USER level. " +
      "Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level.")
  @GET
  def listDynamicConfigs(
      @QueryParam("level") level: String,
      @QueryParam("tenant") tenant: String,
      @QueryParam("name") name: String): String = {
    httpService.getDynamicConfigs(
      normalizeParam(level),
      normalizeParam(tenant),
      normalizeParam(name))
  }

  @Path("/workerInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "For MASTER: List worker information of the service. It will list all registered workers' information.\n" +
        "For WORKER: List the worker information of the worker.")
  @GET
  def workerInfo(): String = {
    httpService.getWorkerInfo
  }

  @Path("/threadDump")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List the current thread dump.")
  @GET
  def threadDump(): String = {
    httpService.getThreadDump
  }

  @Path("shuffle")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "For MASTER: List all running shuffle keys of the service. It will return all running shuffle's key of the cluster.\n" +
        "For WORKER: List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker.")
  @GET
  def shuffles(): String = {
    httpService.getShuffleList
  }

  @Path("applications")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "For MASTER: List all running application's ids of the cluster.\n" +
        "For WORKER: List all running application's ids of the worker. It only return application ids running in that worker.")
  @GET
  def applications(): String = {
    httpService.getApplicationList
  }
}
