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

package org.apache.celeborn.service.deploy.worker.http.api

import javax.ws.rs.{FormParam, GET, Path, POST}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Tag(name = "Deprecated")
@Path("/")
class ApiWorkerResource extends ApiRequestContext {

  @Path("/listPartitionLocationInfo")
  @Operation(description = "List all the living PartitionLocation information in that worker.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)))
  @GET
  def listPartitionLocationInfo: String = httpService.listPartitionLocationInfo

  @Path("/unavailablePeers")
  @Operation(description =
    "List the unavailable peers of the worker, this always means the worker connect to the peer failed.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)))
  @GET
  def unavailablePeers: String = httpService.getUnavailablePeers

  @Path("/isShutdown")
  @Operation(description = "Show if the worker is during the process of shutdown.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)))
  @GET
  def isShutdown: String = httpService.isShutdown

  @Path("/isDecommissioning")
  @Operation(description = "Show if the worker is during the process of decommission.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)))
  @GET
  def isDecommissioning: String = httpService.isDecommissioning

  @Path("/isRegistered")
  @Operation(description = "Show if the worker is registered to the master success.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)))
  @GET
  def isRegistered: String = httpService.isRegistered

  @Path("/exit")
  @Operation(description =
    "Trigger this worker to exit. Legal types are 'Decommission', 'Graceful' and 'Immediately'.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_FORM_URLENCODED)))
  @POST
  def exit(@FormParam("type") exitType: String): String = {
    httpService.exit(normalizeParam(exitType))
  }
}
