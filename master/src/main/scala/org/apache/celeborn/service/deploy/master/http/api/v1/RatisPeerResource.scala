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
import org.apache.ratis.proto.RaftProtos.RaftPeerRole
import org.apache.ratis.protocol.{RaftPeer, RaftPeerId, SetConfigurationRequest}
import org.apache.ratis.rpc.CallId

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.rest.v1.model.{HandleResponse, RatisPeerAddRequest, RatisPeerRemoveRequest, RatisPeerSetPriorityRequest}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils.ensureMasterIsLeader

@Tag(name = "Ratis")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class RatisPeerResource extends ApiRequestContext with Logging {
  private def master = httpService.asInstanceOf[Master]
  private def ratisServer = master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Add new peers to the raft group.")
  @POST
  @Path("/add")
  def peerAdd(request: RatisPeerAddRequest): HandleResponse = ensureMasterIsLeader(master) {
    val remaining = getPeersWithRole(RaftPeerRole.FOLLOWER)
    val adding = request.getPeers.asScala.map { peer =>
      RaftPeer.newBuilder()
        .setId(peer.getId)
        .setAddress(peer.getAddress)
        .setPriority(0)
        .build()
    }

    val peers = (remaining ++ adding).distinct
    val listeners = getPeersWithRole(RaftPeerRole.LISTENER)

    logInfo(
      s"Adding peers ${adding.map(_.getId).mkString(",")} to group ${ratisServer.getGroupInfo}.")
    logInfo(s"New peers: ${peers.map(_.getId).mkString(",")}")
    logInfo(s"New listeners: ${listeners.map(_.getId).mkString(",")}")

    val reply = ratisServer.getServer.setConfiguration(
      new SetConfigurationRequest(
        ratisServer.getClientId,
        ratisServer.getServer.getId,
        ratisServer.getGroupId,
        CallId.getAndIncrement(),
        peers.asJava,
        listeners.asJava))

    if (reply.isSuccess) {
      new HandleResponse().success(true).message(
        s"Successfully added peers ${peers.map(_.getId).mkString(",")} to group ${ratisServer.getGroupInfo}.")
    } else {
      new HandleResponse().success(false).message(
        s"Failed to add peers ${peers.map(_.getId).mkString(
          ",")} to group ${ratisServer.getGroupInfo}. $reply")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Remove peers from the raft group.")
  @POST
  @Path("/remove")
  def peerRemove(request: RatisPeerRemoveRequest): HandleResponse = ensureMasterIsLeader(master) {
    val removing = request.getPeerAddresses.asScala.map(getRaftPeerId)

    val peers = getPeersWithRole(RaftPeerRole.FOLLOWER)
      .filterNot(peer => removing.contains(peer.getId))
    val listeners = getPeersWithRole(RaftPeerRole.LISTENER)
      .filterNot(peer => removing.contains(peer.getId))

    logInfo(s"Removing peers ${removing.mkString(",")} to group ${ratisServer.getGroupInfo}.")
    logInfo(s"New peers: ${peers.map(_.getId).mkString(",")}")
    logInfo(s"New listeners: ${listeners.map(_.getId).mkString(",")}")

    val reply = ratisServer.getServer.setConfiguration(
      new SetConfigurationRequest(
        ratisServer.getClientId,
        ratisServer.getServer.getId,
        ratisServer.getGroupId,
        CallId.getAndIncrement(),
        peers.asJava,
        listeners.asJava))

    if (reply.isSuccess) {
      new HandleResponse().success(true).message(
        s"Successfully added peers ${peers.map(_.getId).mkString(",")} to group ${ratisServer.getGroupInfo}.")
    } else {
      new HandleResponse().success(false).message(
        s"Failed to add peers ${peers.map(_.getId).mkString(
          ",")} to group ${ratisServer.getGroupInfo}. $reply")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Set the priority of the peers in the raft group.")
  @POST
  @Path("/set_priority")
  def peerSetPriority(request: RatisPeerSetPriorityRequest): HandleResponse =
    ensureMasterIsLeader(master) {
      val peers = getPeersWithRole(RaftPeerRole.FOLLOWER).map { peer =>
        val newPriority = request.getAddressPriorities.get(peer.getAddress)
        val priority: Int = if (newPriority != null) newPriority else peer.getPriority
        RaftPeer.newBuilder(peer).setPriority(priority).build()
      }
      val listeners = getPeersWithRole(RaftPeerRole.LISTENER)

      val reply = ratisServer.getServer.setConfiguration(
        new SetConfigurationRequest(
          ratisServer.getClientId,
          ratisServer.getServer.getId,
          ratisServer.getGroupId,
          CallId.getAndIncrement(),
          peers.asJava,
          listeners.asJava))

      if (reply.isSuccess) {
        new HandleResponse().success(true).message(
          s"Successfully set priority of peers ${peers.map(_.getId).mkString(",")} to group ${ratisServer.getGroupInfo}.")
      } else {
        new HandleResponse().success(false).message(
          s"Failed to set priority of peers ${peers.map(_.getId).mkString(
            ",")} to group ${ratisServer.getGroupInfo}. $reply")
      }
    }

  private def getRaftPeerId(peerAddress: String): RaftPeerId = {
    val groupInfo =
      master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer.getGroupInfo
    groupInfo.getCommitInfos.asScala.filter(peer => peer.getServer.getAddress == peerAddress)
      .map(peer => RaftPeerId.valueOf(peer.getServer.getId)).headOption.getOrElse(
        throw new IllegalArgumentException(s"Peer $peerAddress not found in group: $groupInfo"))
  }

  private def getPeersWithRole(role: RaftPeerRole): Seq[RaftPeer] = {
    val groupInfo = ratisServer.getGroupInfo
    val conf = groupInfo.getConf.orElse(null)
    if (conf == null) return Seq.empty
    val targets = if (role == RaftPeerRole.LISTENER) conf.getListenersList else conf.getPeersList
    groupInfo.getGroup.getPeers.asScala.filter(peer => targets.contains(peer.getId)).toSeq
  }
}
