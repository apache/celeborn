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

import javax.ws.rs.{Consumes, GET, Path, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.common.meta.AppDiskUsageSnapShot
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.api.v1.dto.{AppDiskUsageData, AppDiskUsageSnapshotData, ApplicationHeartbeatData}
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
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[ApplicationHeartbeatData])))),
    description = "List all running application's ids of the cluster.")
  @GET
  def applications(): Seq[ApplicationHeartbeatData] = {
    statusSystem.appHeartbeatTime.asScala.map { case (appId, heartbeat) =>
      new ApplicationHeartbeatData(appId, heartbeat)
    }.toSeq
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[AppDiskUsageSnapshotData])))),
    description =
      "List the top disk usage application ids. It will return the top disk usage application ids for the cluster.")
  @Path("/top_disk_usages")
  @GET
  def topDiskUsedApplications(): Seq[AppDiskUsageSnapshotData] = {
    statusSystem.appDiskUsageMetric.topSnapshots().map { snapshot =>
      new AppDiskUsageSnapshotData(
        snapshot.startSnapShotTime,
        snapshot.endSnapShotTime,
        snapshot.topNItems.map { usage =>
          new AppDiskUsageData(usage.appId, usage.estimatedUsage)
        }.toSeq.asJava)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(
        implementation = classOf[String])))),
    description =
      "List all running application's LifecycleManager's hostnames of the cluster.")
  @Path("/hostnames")
  @GET
  def hostnames(): Seq[String] = {
    statusSystem.hostnameSet.asScala.toSeq
  }
}
