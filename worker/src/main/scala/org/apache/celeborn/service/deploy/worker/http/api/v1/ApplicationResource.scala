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

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.{AppDiskUsageData, AppDiskUsagesResponse, ApplicationsResponse}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.worker.Worker

@Tag(name = "Application")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class ApplicationResource extends ApiRequestContext {
  private def worker = httpService.asInstanceOf[Worker]
  private def storageManager = worker.storageManager

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ApplicationsResponse]))),
    description =
      "List all running application's ids of the worker. It only return application ids running in that worker.")
  @GET
  def applications(): ApplicationsResponse = {
    new ApplicationsResponse()
      .applications(worker.workerInfo.getApplicationIdSet.asScala.toSeq.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[AppDiskUsagesResponse]))),
    description =
      "List the top disk usage application ids. It will return the top disk usage application ids for the cluster.")
  @GET
  @Path("/top_disk_usages")
  def topDiskUsedApplications(): AppDiskUsagesResponse = {
    new AppDiskUsagesResponse()
      .appDiskUsages(
        storageManager.topAppDiskUsage.asScala.map { case (appId, diskUsage) =>
          new AppDiskUsageData()
            .appId(appId)
            .estimatedUsage(diskUsage)
            .estimatedUsageStr(Utils.bytesToString(diskUsage))
        }.toSeq.asJava)

  }
}
