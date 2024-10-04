package org.apache.celeborn.service.deploy.master.http.api.v1
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

import javax.ws.rs.{Consumes, Path, POST, Produces}
import javax.ws.rs.core.{MediaType, Response}

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils.ensureMasterHAEnabled

@Tag(name = "Ratis")
@Consumes(Array(MediaType.APPLICATION_JSON))
class RatisLocalResource extends ApiRequestContext with Logging {
  private def master = httpService.asInstanceOf[Master]
  private def ratisServer = master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_OCTET_STREAM,
      schema = new Schema(implementation = classOf[Response]))),
    description = "Generate a new-raft-meta.conf file.")
  @POST
  @Path("/raft_meta_conf")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def localRaftMetaConf(): Response = ensureMasterHAEnabled(master) {
    Response.ok(ratisServer.getGroupInfo().getLogInfoProto().toByteArray)
      .header("Content-Disposition", "attachment; filename=\"new-raft-meta.conf\"")
      .build()
  }
}
