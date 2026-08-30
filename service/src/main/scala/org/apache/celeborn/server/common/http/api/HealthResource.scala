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

import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

/**
 * The response of the health check, see [[HealthResource]].
 *
 * @param service the name of the service, either `master` or `worker`.
 * @param healthy whether the service is able to serve.
 * @param reason  the reason why the service is not able to serve, empty when healthy.
 */
case class HealthCheckResponse(service: String, healthy: Boolean, reason: String)

@Tag(name = "Health")
@Path("/health")
@Produces(Array(MediaType.APPLICATION_JSON))
private[api] class HealthResource extends ApiRequestContext {

  @Operation(description =
    "Check whether the service is able to serve. Returns 200 when the service is healthy " +
      "and 503 otherwise, which is intended to be consumed by a readiness probe. " +
      "The master reports healthy once its HTTP service is available, including when it is " +
      "a follower. The worker reports healthy only when it is registered with the master and " +
      "its state is Normal, so a worker that is decommissioning, idle or exiting is reported " +
      "as not able to serve.")
  @ApiResponse(
    responseCode = "200",
    description = "The service is able to serve.",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HealthCheckResponse]))))
  @ApiResponse(
    responseCode = "503",
    description = "The service is not able to serve.",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HealthCheckResponse]))))
  @GET
  def health(): Response = {
    val (healthy, reason) = httpService.healthCheck()
    val status =
      if (healthy) Response.Status.OK else Response.Status.SERVICE_UNAVAILABLE
    Response.status(status)
      .`type`(MediaType.APPLICATION_JSON)
      .entity(HealthCheckResponse(httpService.serviceName, healthy, reason))
      .build()
  }
}
