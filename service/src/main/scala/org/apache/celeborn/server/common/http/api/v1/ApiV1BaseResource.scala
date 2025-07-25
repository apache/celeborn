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

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.{ContainerInfo, ThreadStack, ThreadStackResponse}
import org.apache.celeborn.server.common.container.ContainerInfoProvider
import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Path("/api/v1")
class ApiV1BaseResource extends ApiRequestContext {
  @Path("conf")
  def conf: Class[ConfResource] = classOf[ConfResource]

  @Path("loggers")
  def logger: Class[LoggerResource] = classOf[LoggerResource]

  @Path("/thread_dump")
  @Operation(description = "List the current thread dump.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[ThreadStackResponse]))))
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def threadDump(): ThreadStackResponse = {
    new ThreadStackResponse()
      .threadStacks(Utils.getThreadDump().map { threadStack =>
        new ThreadStack()
          .threadId(threadStack.threadId)
          .threadName(threadStack.threadName)
          .threadState(threadStack.threadState.toString)
          .stackTrace(threadStack.stackTrace.elems.asJava)
          .blockedByThreadId(
            threadStack.blockedByThreadId.getOrElse(null).asInstanceOf[java.lang.Long])
          .blockedByLock(threadStack.blockedByLock)
          .holdingLocks(threadStack.holdingLocks.asJava)
          .synchronizers(threadStack.synchronizers.asJava)
          .monitors(threadStack.monitors.asJava)
          .lockName(threadStack.lockName.orNull)
          .lockOwnerName(threadStack.lockOwnerName.orNull)
          .suspended(threadStack.suspended)
          .inNative(threadStack.inNative)
      }.asJava)
  }

  @Path("/container_info")
  @Operation(description = "List the container info.")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[ContainerInfo]))))
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  def containerInfo(): ContainerInfo =
    ContainerInfoProvider.instantiate(httpService.conf).getContainerInfo()
}
