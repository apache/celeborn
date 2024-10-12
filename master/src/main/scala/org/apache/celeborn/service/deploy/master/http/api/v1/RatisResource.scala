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

import javax.ws.rs.{BadRequestException, Consumes, Path, POST, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.ratis.protocol.{LeaderElectionManagementRequest, RaftClientReply, RaftPeer, SetConfigurationRequest, SnapshotManagementRequest, TransferLeadershipRequest}
import org.apache.ratis.rpc.CallId

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.rest.v1.model.{HandleResponse, RatisElectionTransferRequest, RatisPeerAddRequest, RatisPeerRemoveRequest, RatisPeerSetPriorityRequest}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAMasterMetaManager, HARaftServer}
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils.{ensureMasterHAEnabled, ensureMasterIsLeader}

@Tag(name = "Ratis")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class RatisResource extends ApiRequestContext with Logging {
  private def master = httpService.asInstanceOf[Master]
  private def ratisServer = master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Transfer the group leader to the specified server.")
  @POST
  @Path("/election/transfer")
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
  @Path("/election/step_down")
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
  @Path("/election/pause")
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
  @Path("/election/resume")
  def electionResume(): HandleResponse = ensureMasterHAEnabled(master) {
    applyElectionOp(new LeaderElectionManagementRequest.Resume)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Add new peers to the raft group.")
  @POST
  @Path("/peer/add")
  def peerAdd(request: RatisPeerAddRequest): HandleResponse =
    ensureLeaderElectionMemberMajorityAddEnabled(master) {
      if (request.getPeers.isEmpty) {
        throw new BadRequestException("No peers specified.")
      }

      val groupInfo = ratisServer.getGroupInfo

      val remaining = getRaftPeers()
      val adding = request.getPeers.asScala.map { peer =>
        if (remaining.exists(e =>
            e.getId.toString == peer.getId || e.getAddress == peer.getAddress)) {
          throw new IllegalArgumentException(
            s"Peer $peer with same id or address already exists in group $groupInfo.")
        }
        RaftPeer.newBuilder()
          .setId(peer.getId)
          .setAddress(peer.getAddress)
          .setPriority(0)
          .build()
      }

      val peers = (remaining ++ adding).distinct

      logInfo(s"Adding peers: $adding to group $groupInfo.")
      logInfo(s"New peers: $peers")

      val reply = setConfiguration(peers)
      if (reply.isSuccess) {
        new HandleResponse().success(true).message(
          s"Successfully added peers $adding to group $groupInfo.")
      } else {
        new HandleResponse().success(false).message(
          s"Failed to add peers $adding to group $groupInfo. $reply")
      }
    }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Remove peers from the raft group.")
  @POST
  @Path("/peer/remove")
  def peerRemove(request: RatisPeerRemoveRequest): HandleResponse =
    ensureLeaderElectionMemberMajorityAddEnabled(master) {
      if (request.getPeers.isEmpty) {
        throw new BadRequestException("No peers specified.")
      }

      val groupInfo = ratisServer.getGroupInfo

      val removing = request.getPeers.asScala.map { peer =>
        getRaftPeers().find { raftPeer =>
          raftPeer.getId.toString == peer.getId && raftPeer.getAddress == peer.getAddress
        }.getOrElse(throw new IllegalArgumentException(
          s"Peer $peer not found in group $groupInfo."))
      }
      val remaining = getRaftPeers().filterNot(removing.contains)

      logInfo(s"Removing peers:$removing from group $groupInfo.")
      logInfo(s"New peers: $remaining")

      val reply = setConfiguration(remaining)
      if (reply.isSuccess) {
        new HandleResponse().success(true).message(
          s"Successfully removed peers $removing from group $groupInfo.")
      } else {
        new HandleResponse().success(false).message(
          s"Failed to remove peers $removing from group $groupInfo. $reply")
      }
    }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Set the priority of the peers in the raft group.")
  @POST
  @Path("/peer/set_priority")
  def peerSetPriority(request: RatisPeerSetPriorityRequest): HandleResponse =
    ensureLeaderElectionMemberMajorityAddEnabled(master) {
      if (request.getAddressPriorities.isEmpty) {
        throw new BadRequestException("No peer priorities specified.")
      }

      val peers = getRaftPeers().map { peer =>
        val newPriority = request.getAddressPriorities.get(peer.getAddress)
        val priority: Int = if (newPriority != null) newPriority else peer.getPriority
        RaftPeer.newBuilder(peer).setPriority(priority).build()
      }

      val peerPriorities =
        request.getAddressPriorities.asScala.map { case (a, p) => s"$a:$p" }.mkString(", ")
      logInfo(s"Setting peer priorities: $peerPriorities.")
      logInfo(s"New peers: $peers")

      val reply = setConfiguration(peers)
      if (reply.isSuccess) {
        new HandleResponse().success(true).message(
          s"Successfully set peer priorities: $peerPriorities.")
      } else {
        new HandleResponse().success(false).message(
          s"Failed to set peer priorities: $peerPriorities. $reply")
      }
    }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Trigger the current server to take snapshot.")
  @POST
  @Path("/snapshot/create")
  def createSnapshot(): HandleResponse = ensureMasterHAEnabled(master) {
    val request = SnapshotManagementRequest.newCreate(
      ratisServer.getClientId,
      ratisServer.getServer.getId,
      ratisServer.getGroupId,
      CallId.getAndIncrement(),
      HARaftServer.REQUEST_TIMEOUT_MS)
    val reply = ratisServer.getServer.snapshotManagement(request)
    if (reply.isSuccess) {
      new HandleResponse().success(true).message(
        s"Successfully create snapshot at ${ratisServer.getLocalAddress}.")
    } else {
      new HandleResponse().success(false).message(
        s"Failed to create snapshot at ${ratisServer.getLocalAddress}. $reply")
    }
  }

  private def transferLeadership(peerAddress: String): HandleResponse = {
    val newLeaderId = Option(peerAddress).map { addr =>
      getRaftPeers().find(_.getAddress == addr).map(_.getId).getOrElse(
        throw new IllegalArgumentException(
          s"Peer $addr not found in group ${ratisServer.getGroupInfo}."))
    }.orNull
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

  private def setConfiguration(peers: Seq[RaftPeer]): RaftClientReply = {
    ratisServer.getServer.setConfiguration(new SetConfigurationRequest(
      ratisServer.getClientId,
      ratisServer.getServer.getId,
      ratisServer.getGroupId,
      CallId.getAndIncrement(),
      SetConfigurationRequest.Arguments.newBuilder.setServersInNewConf(peers.asJava).build()))
  }

  private def getRaftPeers(): Seq[RaftPeer] = {
    ratisServer.getGroupInfo.getGroup.getPeers.asScala.toSeq
  }

  private def ensureLeaderElectionMemberMajorityAddEnabled[T](master: Master)(f: => T): T = {
    ensureMasterIsLeader(master) {
      if (!master.conf.hasMasterRatisLeaderElectionMemeberMajorityAdd) {
        throw new BadRequestException(s"This operation can only be done when" +
          s" ${CelebornConf.HA_MASTER_RATIS_LEADER_ELECTION_MEMBER_MAJORITY_ADD.key} is true.")
      }
      f
    }
  }
}
