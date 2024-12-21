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

import javax.ws.rs.{BadRequestException, Consumes, GET, Path, POST, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.common.protocol.WorkerEventType
import org.apache.celeborn.rest.v1.model._
import org.apache.celeborn.rest.v1.model.SendWorkerEventRequest.EventTypeEnum
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.api.v1.ApiUtils
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils._

@Tag(name = "Worker")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class WorkerResource extends ApiRequestContext {
  private def master: Master = httpService.asInstanceOf[Master]
  private def statusSystem = master.statusSystem

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[WorkersResponse]))),
    description =
      "List worker information of the service. It will list all registered workers' information.")
  @GET
  def workers: WorkersResponse = {
    new WorkersResponse()
      .workers(statusSystem.workersMap.values().asScala.map(ApiUtils.workerData).toSeq.asJava)
      .lostWorkers(statusSystem.lostWorkers.asScala.toSeq.sortBy(_._2)
        .map(kv =>
          new WorkerTimestampData().worker(ApiUtils.workerData(kv._1)).timestamp(kv._2)).asJava)
      .excludedWorkers(
        (statusSystem.excludedWorkers.asScala ++ statusSystem.manuallyExcludedWorkers.asScala)
          .map(ApiUtils.workerData).toSeq.asJava)
      .manualExcludedWorkers(statusSystem.manuallyExcludedWorkers.asScala.map(
        ApiUtils.workerData).toSeq.asJava)
      .shutdownWorkers(statusSystem.shutdownWorkers.asScala.map(
        ApiUtils.workerData).toSeq.asJava)
      .decommissioningWorkers(statusSystem.decommissionWorkers.asScala.map(
        ApiUtils.workerData).toSeq.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description =
      "Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove.")
  @POST
  @Path("/exclude")
  def excludeWorker(request: ExcludeWorkerRequest): HandleResponse = ensureMasterIsLeader(master) {
    val (success, msg) = httpService.exclude(
      request.getAdd.asScala.map(ApiUtils.toWorkerInfo).toSeq,
      request.getRemove.asScala.map(ApiUtils.toWorkerInfo).toSeq)
    new HandleResponse().success(success).message(msg)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Remove the workers unavailable info from the master.")
  @POST
  @Path("/remove_unavailable")
  def removeWorkersUnavailableInfo(request: RemoveWorkersUnavailableInfoRequest): HandleResponse =
    ensureMasterIsLeader(master) {
      val (success, msg) = master.removeWorkersUnavailableInfo(
        request.getWorkers.asScala.map(ApiUtils.toWorkerInfo).toSeq)
      new HandleResponse().success(success).message(msg)
    }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[WorkerEventsResponse]))),
    description = "List all worker event infos of the master.")
  @GET
  @Path("/events")
  def workerEvents(): WorkerEventsResponse = {
    new WorkerEventsResponse().workerEvents(
      statusSystem.workerEventInfos.asScala.map { case (worker, event) =>
        new WorkerEventData()
          .worker(
            ApiUtils.workerData(worker)).event(
            new WorkerEventInfoData()
              .eventType(event.getEventType.toString)
              .eventTime(event.getEventStartTime))
      }.toSeq.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description =
      "For Master(Leader) can send worker event to manager workers. Legal types are 'None', 'Immediately', 'Decommission', 'DecommissionThenIdle', 'Graceful', 'Recommission'.")
  @POST
  @Path("/events")
  def sendWorkerEvents(request: SendWorkerEventRequest): HandleResponse =
    ensureMasterIsLeader(master) {
      if (request.getEventType == null || request.getWorkers.isEmpty) {
        throw new BadRequestException(
          s"eventType(${request.getEventType}) and workers(${request.getWorkers}) are required")
      }
      val workers = request.getWorkers.asScala.map(ApiUtils.toWorkerInfo).toSeq
      val (filteredWorkers, unknownWorkers) =
        workers.partition(w => statusSystem.workersMap.containsKey(w.toUniqueId))
      if (filteredWorkers.isEmpty) {
        throw new BadRequestException(
          s"None of the workers are known: ${unknownWorkers.map(_.readableAddress).mkString(", ")}")
      }
      val (success, msg) =
        httpService.handleWorkerEvent(toWorkerEventType(request.getEventType), workers)
      val finalMsg =
        if (unknownWorkers.isEmpty) {
          msg
        } else {
          s"${msg}\n(Unknown workers: ${unknownWorkers.map(_.readableAddress).mkString(", ")})"
        }
      new HandleResponse().success(success).message(finalMsg)
    }

  private def toWorkerEventType(enum: EventTypeEnum): WorkerEventType = {
    enum match {
      case EventTypeEnum.NONE => WorkerEventType.None
      case EventTypeEnum.IMMEDIATELY => WorkerEventType.Immediately
      case EventTypeEnum.DECOMMISSION => WorkerEventType.Decommission
      case EventTypeEnum.DECOMMISSIONTHENIDLE => WorkerEventType.DecommissionThenIdle
      case EventTypeEnum.GRACEFUL => WorkerEventType.Graceful
      case EventTypeEnum.RECOMMISSION => WorkerEventType.Recommission
      case _ => WorkerEventType.UNRECOGNIZED
    }
  }
}
