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

import javax.ws.rs.{Consumes, Path, POST, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.ratis.protocol.{LeaderElectionManagementRequest, RaftPeerId, TransferLeadershipRequest}
import org.apache.ratis.rpc.CallId

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.rest.v1.model.{HandleResponse, RatisElectionTransferRequest}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAMasterMetaManager, HARaftServer}
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils.{ensureMasterHAEnabled, ensureMasterIsLeader}

@Tag(name = "Ratis")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class RatisElectionResource extends ApiRequestContext with Logging {
  private def master = httpService.asInstanceOf[Master]
  private def ratisServer = master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Transfer the group leader to the specified server.")
  @POST
  @Path("/transfer")
  def electionTransfer(request: RatisElectionTransferRequest): HandleResponse =
    ensureMasterIsLeader(master) {
      transferLeadership(request.getPeerAddress)
    }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Make the group leader step down its leadership.")
  @POST
  @Path("/step_down")
  def electionStepDown(): HandleResponse = ensureMasterIsLeader(master) {
    transferLeadership(null)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Pause leader election at the current server." +
      " Then, the current server would not start a leader election.")
  @POST
  @Path("/pause")
  def electionPause(): HandleResponse = ensureMasterHAEnabled(master) {
    applyElectionOp(new LeaderElectionManagementRequest.Pause)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Resume leader election at the current server.")
  @POST
  @Path("/resume")
  def electionResume(): HandleResponse = ensureMasterHAEnabled(master) {
    applyElectionOp(new LeaderElectionManagementRequest.Resume)
  }

  private def transferLeadership(peerAddress: String): HandleResponse = {
    val newLeaderId = Option(peerAddress).map(getRaftPeerId).orNull
    val op =
      if (newLeaderId == null) s"step down leader ${ratisServer.getLocalAddress}"
      else s"transfer leadership from ${ratisServer.getLocalAddress} to $peerAddress"
    val request = new TransferLeadershipRequest(
      ratisServer.getClientId,
      ratisServer.getServer.getId,
      ratisServer.getGroupId,
      CallId.getAndIncrement(),
      newLeaderId,
      HARaftServer.REQUEST_TIMEOUT_MS)
    val reply = ratisServer.getServer.transferLeadership(request)
    if (reply.isSuccess) {
      new HandleResponse().success(true).message(s"Successfully $op.")
    } else {
      new HandleResponse().success(false).message(s"Failed to $op: $reply")
    }
  }

  private def applyElectionOp(op: LeaderElectionManagementRequest.Op): HandleResponse = {
    val request = new LeaderElectionManagementRequest(
      ratisServer.getClientId,
      ratisServer.getServer.getId,
      ratisServer.getGroupId,
      CallId.getAndIncrement(),
      op)
    val reply = ratisServer.getServer.leaderElectionManagement(request)
    if (reply.isSuccess) {
      new HandleResponse().success(true).message(
        s"Successfully applied election $op ${ratisServer.getLocalAddress}.")
    } else {
      new HandleResponse().success(false).message(
        s"Failed to apply election $op ${ratisServer.getLocalAddress}. $reply")
    }
  }

  private def getRaftPeerId(peerAddress: String): RaftPeerId = {
    val groupInfo =
      master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer.getGroupInfo
    groupInfo.getCommitInfos.asScala.filter(peer => peer.getServer.getAddress == peerAddress)
      .map(peer => RaftPeerId.valueOf(peer.getServer.getId)).headOption.getOrElse(
        throw new IllegalArgumentException(s"Peer $peerAddress not found in group: $groupInfo"))
  }
}
