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

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.{ThreadStack, ThreadStackResponse}
import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Path("/api/v1")
class ApiV1BaseResource extends ApiRequestContext {
  @Path("conf")
  def conf: Class[ConfResource] = classOf[ConfResource]

  @Path("/thread_dump")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[ThreadStackResponse]))),
    description = "List the current thread dump.")
  @Produces(Array(MediaType.APPLICATION_JSON))
  @GET
  def threadDump(): ThreadStackResponse = {
    new ThreadStackResponse()
      .threadStacks(Utils.getThreadDump().map { threadStack =>
        new ThreadStack()
          .threadId(threadStack.threadId)
          .threadName(threadStack.threadName)
          .threadState(threadStack.threadState.toString)
          .stackTrace(threadStack.stackTrace.elems.asJava)
          .blockedByThreadId(threadStack.blockedByThreadId.getOrElse(null.asInstanceOf[Long]): Long)
          .blockedByLock(threadStack.blockedByLock)
          .holdingLocks(threadStack.holdingLocks.asJava)
      }.asJava)
  }
}
