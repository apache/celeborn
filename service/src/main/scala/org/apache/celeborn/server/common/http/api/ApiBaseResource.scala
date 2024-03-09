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

import org.apache.celeborn.server.common.Service
import org.apache.celeborn.server.common.service.config.ConfigLevel

@Path("/")
private[api] class ApiBaseResource extends ApiRequestContext {
  def service: String = rs.serviceName

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("/conf")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = s"List the conf setting of the $service.")
  @GET
  def conf: String = rs.getConf

  @Path("/listDynamicConfigs")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = s"List the dynamic configs of the $service. " +
      s"The parameter level specifies the config level of dynamic configs. " +
      s"The parameter tenant specifies the tenant id of ${ConfigLevel.TENANT.name()} or ${ConfigLevel.TENANT_USER.name()} level. " +
      s"The parameter name specifies the user name of ${ConfigLevel.TENANT_USER.name()} level. " +
      s"Meanwhile, either none or all of the parameter tenant and name are specified for ${ConfigLevel.TENANT_USER.name()} level.")
  @GET
  def listDynamicConfigs(
      @QueryParam("LEVEL") level: String = "",
      @QueryParam("TENANT") tenant: String = "",
      @QueryParam("NAME") name: String = ""): String = {
    rs.getDynamicConfigs(level.trim, tenant.trim, name.trim)
  }

  @Path("/workerInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      if (service == Service.MASTER)
        "List worker information of the service. It will list all registered workers 's information."
      else "List the worker information of the worker.")
  @GET
  def workerInfo(): String = {
    rs.getWorkerInfo
  }

  @Path("/threadDump")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = s"List the current thread dump of the $service.")
  @GET
  def threadDump(): String = {
    rs.getThreadDump
  }

  @Path("shuffle")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      if (service == Service.MASTER)
        "List all running shuffle keys of the service. It will return all running shuffle's key of the cluster."
      else
        "List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker.")
  @GET
  def shuffles(): String = {
    rs.getShuffleList
  }

  @Path("applications")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      if (service == Service.MASTER)
        "List all running application's ids of the cluster."
      else
        "List all running application's ids of the worker. It only return application ids running in that worker.")
  @GET
  def applications(): String = {
    rs.getApplicationList
  }

  @Path("listTopDiskUsedApps")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      if (service == Service.MASTER)
        "List the top disk usage application ids. It will return the top disk usage application ids for the cluster."
      else
        "List the top disk usage application ids. It only return application ids running in that worker.")
  @GET
  def listTopDiskUsedApps(): String = {
    rs.listTopDiskUseApps
  }
}
