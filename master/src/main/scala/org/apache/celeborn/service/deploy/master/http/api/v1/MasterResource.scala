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

import javax.ws.rs.{BadRequestException, Consumes, GET, Produces}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.ratis.proto.RaftProtos.RaftPeerRole

import org.apache.celeborn.rest.v1.model.{MasterCommitData, MasterInfoResponse, MasterLeader, RatisLogInfo, RatisLogTermIndex}
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager

@Tag(name = "Master")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class MasterResource extends ApiRequestContext {
  private def master = httpService.asInstanceOf[Master]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[MasterInfoResponse]))),
    description =
      "List master group information of the service. It will list all master's LEADER, FOLLOWER information.")
  @GET
  def masterGroupInfo: MasterInfoResponse = {
    if (master.conf.haEnabled) {
      val groupInfo =
        master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer.getGroupInfo
      val leader = Option(groupInfo.getRoleInfoProto).map { roleInfo =>
        if (roleInfo.getRole == RaftPeerRole.LEADER) {
          roleInfo.getSelf
        } else {
          Option(roleInfo.getFollowerInfo).map(_.getLeaderInfo.getId).orNull
        }
      }.orNull
      val masterLeader = Option(leader).map { _ =>
        new MasterLeader()
          .id(leader.getId.toStringUtf8)
          .address(leader.getAddress)
      }.orNull
      val masterCommitDataList = groupInfo.getCommitInfos.asScala.map { commitInfo =>
        new MasterCommitData()
          .commitIndex(commitInfo.getCommitIndex)
          .id(commitInfo.getServer.getId.toStringUtf8)
          .address(commitInfo.getServer.getAddress)
          .clientAddress(commitInfo.getServer.getClientAddress)
          .startUpRole(commitInfo.getServer.getStartupRole.toString)
          .priority(commitInfo.getServer.getPriority)
      }
      val logInfo = new RatisLogInfo()
        .lastSnapshot(
          new RatisLogTermIndex()
            .term(groupInfo.getLogInfoProto.getLastSnapshot.getTerm)
            .index(groupInfo.getLogInfoProto.getLastSnapshot.getIndex))
        .applied(
          new RatisLogTermIndex()
            .term(groupInfo.getLogInfoProto.getApplied.getTerm)
            .index(groupInfo.getLogInfoProto.getApplied.getIndex))
        .committed(
          new RatisLogTermIndex()
            .term(groupInfo.getLogInfoProto.getCommitted.getTerm)
            .index(groupInfo.getLogInfoProto.getCommitted.getIndex))
        .lastEntry(
          new RatisLogTermIndex()
            .term(groupInfo.getLogInfoProto.getLastEntry.getTerm)
            .index(groupInfo.getLogInfoProto.getLastEntry.getIndex))
      new MasterInfoResponse()
        .groupId(groupInfo.getGroup.getGroupId.getUuid.toString)
        .leader(masterLeader)
        .masterCommitInfo(masterCommitDataList.toSeq.asJava)
        .logInfo(logInfo)
    } else {
      throw new BadRequestException("HA is not enabled")
    }
  }
}
