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

import javax.ws.rs.{Consumes, DELETE, GET, Path, POST, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.{AppDiskUsageData, AppDiskUsageSnapshotData, AppDiskUsageSnapshotsResponse, ApplicationHeartbeatData, ApplicationsHeartbeatResponse, DeleteAppsRequest, HandleResponse, HostnamesResponse, ReviseLostShufflesRequest}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master

@Tag(name = "Application")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class ApplicationResource extends ApiRequestContext {
  private def statusSystem = httpService.asInstanceOf[Master].statusSystem

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ApplicationsHeartbeatResponse]))),
    description = "List all running application's ids of the cluster.")
  @GET
  def applications(): ApplicationsHeartbeatResponse = {
    new ApplicationsHeartbeatResponse()
      .applications(
        statusSystem.appHeartbeatTime.asScala.map { case (appId, heartbeat) =>
          new ApplicationHeartbeatData()
            .appId(appId)
            .lastHeartbeatTimestamp(heartbeat)
        }.toSeq.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Delete resource of apps.")
  @DELETE
  def deleteApps(request: DeleteAppsRequest): HandleResponse = {
    val apps = request.getApps.asScala
    apps.foreach(app => statusSystem.deleteApp(app))
    new HandleResponse().success(true).message(s"deleted shuffles of app ${apps}")
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[AppDiskUsageSnapshotsResponse]))),
    description =
      "List the top disk usage application ids. It will return the top disk usage application ids for the cluster.")
  @GET
  @Path("/top_disk_usages")
  def topDiskUsedApplications(): AppDiskUsageSnapshotsResponse = {
    new AppDiskUsageSnapshotsResponse()
      .snapshots(
        statusSystem.appDiskUsageMetricManager.topSnapshots().map { snapshot =>
          new AppDiskUsageSnapshotData()
            .start(
              snapshot.startSnapShotTime)
            .end(
              snapshot.endSnapShotTime)
            .topNItems(
              snapshot.topNItems.filter(_ != null).map { usage =>
                new AppDiskUsageData()
                  .appId(usage.appId)
                  .estimatedUsage(usage.estimatedUsage)
                  .estimatedUsageStr(Utils.bytesToString(usage.estimatedUsage))
              }.toSeq.asJava)
        }.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HostnamesResponse]))),
    description =
      "List all running application's LifecycleManager's hostnames of the cluster.")
  @GET
  @Path("/hostnames")
  def hostnames(): HostnamesResponse = {
    new HostnamesResponse().hostnames(statusSystem.hostnameSet.asScala.toSeq.asJava)
  }

  @Path("/revise_lost_shuffles")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Revise lost shuffles or deleted shuffles of an application.")
  @POST
  def reviseLostShuffles(request: ReviseLostShufflesRequest): HandleResponse = {
    val appId = request.getAppId
    val shuffleIds = request.getShuffleIds
    statusSystem.reviseLostShuffles(appId, shuffleIds)
    new HandleResponse().success(true).message(s"revised app:$appId lost shuffles:$shuffleIds")
  }
}
