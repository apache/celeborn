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

import org.apache.celeborn.rest.v1.model.{ShufflePartitionsResponse, ShufflesResponse}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.api.v1.ApiUtils
import org.apache.celeborn.service.deploy.worker.Worker

@Tag(name = "Shuffle")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class ShuffleResource extends ApiRequestContext {
  private def worker = httpService.asInstanceOf[Worker]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ShufflesResponse]))),
    description =
      "List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker.")
  @GET
  def shuffles(): ShufflesResponse = {
    new ShufflesResponse()
      .shuffleIds(worker.storageManager.shuffleKeySet().asScala.toSeq.asJava)

  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(
        implementation = classOf[ShufflePartitionsResponse]))),
    description = "List all the living shuffle PartitionLocation information in the worker.")
  @Path("/partitions")
  @GET
  def partitions(): ShufflePartitionsResponse = {
    new ShufflePartitionsResponse()
      .primaryPartitions(
        worker.partitionLocationInfo.primaryPartitionLocations.asScala.map { case (k, v) =>
          k -> v.asScala.map { case (id, location) =>
            id -> ApiUtils.partitionLocationData(location)
          }.asJava
        }.asJava)
      .replicaPartitions(
        worker.partitionLocationInfo.replicaPartitionLocations.asScala.map { case (k, v) =>
          k -> v.asScala.map { case (id, location) =>
            id -> ApiUtils.partitionLocationData(location)
          }.asJava
        }.asJava)
  }
}
