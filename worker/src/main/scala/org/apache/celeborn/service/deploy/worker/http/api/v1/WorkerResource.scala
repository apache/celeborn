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

package org.apache.celeborn.service.deploy.worker.http.api.v1

import javax.ws.rs.{Consumes, GET, Path, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.api.v1.ApiUtils
import org.apache.celeborn.server.common.http.api.v1.dto.{WorkerData, WorkerTimestampData}
import org.apache.celeborn.service.deploy.worker.Worker

@Tag(name = "Worker")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class WorkerResource extends ApiRequestContext {
  private def worker = httpService.asInstanceOf[Worker]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[WorkerData]))),
    description = "List the worker information.")
  @GET
  def workers(): WorkerData = {
    ApiUtils.workerData(
      worker.workerInfo,
      worker.workerStatusManager.currentWorkerStatus,
      worker.shutdown.get(),
      worker.registered.get())
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerTimestampData])))),
    description =
      "List the unavailable peers of the worker, this always means the worker connect to the peer failed.")
  @Path("/unavailable_peers")
  @GET
  def unavailablePeerWorkers(): Seq[WorkerTimestampData] = {
    worker.unavailablePeers.asScala.map { case (worker, lastTimeout) =>
      new WorkerTimestampData(ApiUtils.workerData(worker), lastTimeout)
    }.toSeq
  }
}
