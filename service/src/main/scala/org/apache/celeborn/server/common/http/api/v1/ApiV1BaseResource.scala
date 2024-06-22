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

package org.apache.celeborn.server.common.http.api.v1

import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.celeborn.common.util.{ThreadStackTrace, Utils}
import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Path("/api/v1")
private[v1] class ApiV1BaseResource extends ApiRequestContext {
  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

  @Path("conf")
  def conf: Class[ConfResource] = classOf[ConfResource]

  @Path("/thread_dump")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[ThreadStackTrace])))),
    description = "List the current thread dump.")
  @GET
  def threadDump(): Seq[ThreadStackTrace] = {
    Utils.getThreadDump()
  }
}
