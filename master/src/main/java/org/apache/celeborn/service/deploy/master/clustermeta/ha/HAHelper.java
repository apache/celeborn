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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.protocol.PbMetaRequest;
import org.apache.celeborn.common.rpc.RpcCallContext;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;

public class HAHelper {

  public static boolean checkShouldProcess(
      RpcCallContext context, AbstractMetaManager masterStatusSystem, boolean bindPreferIp)
      throws IOException {
    return checkShouldProcess(
        null != context ? context::sendFailure : null, masterStatusSystem, bindPreferIp);
  }

  public static boolean checkShouldProcess(
      FailableConsumer<IOException, IOException> failureHandler,
      AbstractMetaManager masterStatusSystem,
      boolean bindPreferIp)
      throws IOException {

    HARaftServer ratisServer = getRatisServer(masterStatusSystem);
    if (ratisServer != null) {
      if (ratisServer.isLeader()) {
        return true;
      }
      sendFailure(failureHandler, ratisServer, null, bindPreferIp);
      return false;
    }
    return true;
  }

  public static void sendFailure(
      FailableConsumer<IOException, IOException> failureHandler,
      HARaftServer ratisServer,
      Throwable cause,
      boolean bindPreferIp)
      throws IOException {

    if (failureHandler != null) {
      if (ratisServer != null) {
        Optional<HARaftServer.LeaderPeerEndpoints> leaderPeer =
            ratisServer.getCachedLeaderPeerRpcEndpoint();
        if (leaderPeer.isPresent()) {
          failureHandler.accept(
              new MasterNotLeaderException(
                  ratisServer.getRpcEndpoint(),
                  leaderPeer.get().rpcEndpoints,
                  leaderPeer.get().rpcInternalEndpoints,
                  bindPreferIp,
                  cause));
        } else {
          failureHandler.accept(
              new MasterNotLeaderException(
                  ratisServer.getRpcEndpoint(),
                  MasterNotLeaderException.LEADER_NOT_PRESENTED,
                  cause));
        }
      } else {
        failureHandler.accept(new CelebornIOException(cause.getMessage(), cause));
      }
    }
  }

  public static long getWorkerTimeoutDeadline(AbstractMetaManager masterStatusSystem) {
    HARaftServer ratisServer = getRatisServer(masterStatusSystem);
    if (ratisServer != null) {
      return ratisServer.getWorkerTimeoutDeadline();
    } else {
      return -1;
    }
  }

  public static long getAppTimeoutDeadline(AbstractMetaManager masterStatusSystem) {
    HARaftServer ratisServer = getRatisServer(masterStatusSystem);
    if (ratisServer != null) {
      return ratisServer.getAppTimeoutDeadline();
    } else {
      return -1;
    }
  }

  public static HARaftServer getRatisServer(AbstractMetaManager masterStatusSystem) {
    if ((masterStatusSystem instanceof HAMasterMetaManager)) {
      HARaftServer ratisServer = ((HAMasterMetaManager) masterStatusSystem).getRatisServer();
      return ratisServer;
    }

    return null;
  }

  public static ByteString convertRequestToByteString(ResourceProtos.ResourceRequest request) {
    byte[] requestBytes = request.toByteArray();
    return ByteString.copyFrom(requestBytes);
  }

  public static ByteString convertRequestToByteString(PbMetaRequest request) {
    byte[] requestBytes = request.toByteArray();
    return ByteString.copyFrom(requestBytes);
  }

  public static PbMetaRequest convertByteStringToRequest(ByteString byteString)
      throws InvalidProtocolBufferException {
    byte[] bytes = byteString.toByteArray();
    return PbMetaRequest.parseFrom(bytes);
  }

  public static Message convertResponseToMessage(ResourceProtos.ResourceResponse response) {
    byte[] requestBytes = response.toByteArray();
    return Message.valueOf(ByteString.copyFrom(requestBytes));
  }

  /**
   * Creates a temporary snapshot file.
   *
   * @param storage the snapshot storage
   * @return the temporary snapshot file
   * @throws IOException if error occurred while creating the snapshot file
   */
  public static File createTempSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    File tempDir = storage.getTmpDir();
    if (!tempDir.isDirectory() && !tempDir.mkdir()) {
      throw new IOException(
          "Cannot create temporary snapshot directory at " + tempDir.getAbsolutePath());
    }
    return File.createTempFile(
        "raft_snapshot_" + System.currentTimeMillis() + "_", ".dat", tempDir);
  }
}
