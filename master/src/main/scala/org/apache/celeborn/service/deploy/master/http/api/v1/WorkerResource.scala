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

package org.apache.celeborn.service.deploy.master.http.api.v1

import javax.ws.rs.{BadRequestException, Consumes, GET, Path, POST, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.api.v1.ApiUtils
import org.apache.celeborn.server.common.http.api.v1.dto.{ExcludeWorkerRequest, HandleResponse, SendWorkerEventRequest, WorkerData, WorkerEventData, WorkerEventInfoData, WorkerTimestampData}
import org.apache.celeborn.service.deploy.master.Master

@Tag(name = "Worker")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class WorkerResource extends ApiRequestContext {
  private def statusSystem = httpService.asInstanceOf[Master].statusSystem

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerData])))),
    description =
      "List worker information of the service. It will list all registered workers' information.")
  @GET
  def workers(@QueryParam("hostname") hostname: String = ""): Seq[WorkerData] = {
    if (StringUtils.isEmpty(hostname)) {
      statusSystem.workers.asScala.map(ApiUtils.workerData).toSeq
    } else {
      statusSystem.workers.asScala.filter(_.host == hostname).map(ApiUtils.workerData).toSeq
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerTimestampData])))),
    description = "List all lost workers of the master.")
  @Path("/lost")
  @GET
  def lostWorkers(@QueryParam("hostname") hostname: String = ""): Seq[WorkerTimestampData] = {
    var workers = statusSystem.lostWorkers.asScala.toSeq.sortBy(_._2)
      .map(kv => new WorkerTimestampData(ApiUtils.workerData(kv._1), kv._2))
    if (StringUtils.isNotEmpty(hostname)) {
      workers = workers.filter(_.getWorker.getHost == hostname)
    }
    workers
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerData])))),
    description = "List all excluded workers of the master.")
  @Path("/excluded")
  @GET
  def excludedWorkers(@QueryParam("hostname") hostname: String = ""): Seq[WorkerData] = {
    var workers =
      (statusSystem.excludedWorkers.asScala ++ statusSystem.manuallyExcludedWorkers.asScala)
        .map(ApiUtils.workerData).toSeq
    if (StringUtils.isNotEmpty(hostname)) {
      workers = workers.filter(_.getHost == hostname)
    }
    workers
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerData])))),
    description = "List all shutdown workers of the master.")
  @Path("/shutdown")
  @GET
  def shutdownWorkers(@QueryParam("hostname") hostname: String = ""): Seq[WorkerData] = {
    var workers = statusSystem.shutdownWorkers.asScala.map(ApiUtils.workerData).toSeq
    if (StringUtils.isNotEmpty(hostname)) {
      workers = workers.filter(_.getHost == hostname)
    }
    workers
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerData])))),
    description = "List all decommissioned workers of the master.")
  @Path("/decommissioned")
  @GET
  def decommissionWorkers(@QueryParam("hostname") hostname: String = ""): Seq[WorkerData] = {
    var workers = statusSystem.decommissionWorkers.asScala.map(ApiUtils.workerData).toSeq
    if (StringUtils.isNotEmpty(hostname)) {
      workers = workers.filter(_.getHost == hostname)
    }
    workers
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description =
      "Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove.")
  @Path("/exclude")
  @POST
  def excludeWorker(request: ExcludeWorkerRequest): HandleResponse = {
    val (success, msg) = httpService.exclude(
      request.getAdd.asScala.map(_.toWorkerInfo).toSeq,
      request.getRemove.asScala.map(_.toWorkerInfo).toSeq)
    new HandleResponse(success, msg)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[WorkerEventData])))),
    description = "List all worker event infos of the master.")
  @Path("/events")
  @GET
  def workerEvents(@QueryParam("hostname") hostname: String = ""): Seq[WorkerEventData] = {
    var events = statusSystem.workerEventInfos.asScala.map { case (worker, event) =>
      new WorkerEventData(
        ApiUtils.workerData(worker),
        new WorkerEventInfoData(event.getEventType.toString, event.getEventStartTime))
    }.toSeq
    if (StringUtils.isNotEmpty(hostname)) {
      events = events.filter(_.getWorker.getHost == hostname)
    }
    events
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description =
      "For Master(Leader) can send worker event to manager workers. Legal types are 'None', 'Immediately', 'Decommission', 'DecommissionThenIdle', 'Graceful', 'Recommission'.")
  @Path("/events")
  @POST
  def sendWorkerEvents(request: SendWorkerEventRequest): HandleResponse = {
    if (StringUtils.isEmpty(request.getEventType) || request.getWorkers.isEmpty) {
      throw new BadRequestException(
        s"eventType(${request.getEventType}) and workers(${request.getWorkers}) are required")
    }
    val workers = request.getWorkers.asScala.map(_.toWorkerInfo).toSeq
    val (filteredWorkers, unknownWorkers) = workers.partition(statusSystem.workers.contains)
    if (filteredWorkers.isEmpty) {
      throw new BadRequestException(
        s"None of the workers are known: ${unknownWorkers.map(_.readableAddress).mkString(", ")}")
    }
    val (success, msg) = httpService.handleWorkerEvent(request.getEventType, workers)
    val finalMsg =
      if (unknownWorkers.isEmpty) {
        msg
      } else {
        s"${msg}\n(Unknown workers: ${unknownWorkers.map(_.readableAddress).mkString(", ")})"
      }
    new HandleResponse(success, finalMsg)
  }
}
