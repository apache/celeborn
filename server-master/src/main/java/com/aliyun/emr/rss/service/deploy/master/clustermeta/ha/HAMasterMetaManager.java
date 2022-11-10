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

package com.aliyun.emr.rss.service.deploy.master.clustermeta.ha;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.rpc.RpcEnv;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.AbstractMetaManager;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.MetaUtil;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.ResourceRequest;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.Type;

public class HAMasterMetaManager extends AbstractMetaManager {
  private static final Logger LOG = LoggerFactory.getLogger(HAMasterMetaManager.class);

  protected HARaftServer ratisServer;

  public HAMasterMetaManager(RpcEnv rpcEnv, RssConf conf) {
    this.rpcEnv = rpcEnv;
    this.conf = conf;
  }

  public HARaftServer getRatisServer() {
    return ratisServer;
  }

  public void setRatisServer(HARaftServer ratisServer) {
    this.ratisServer = ratisServer;
  }

  @Override
  public void handleRequestSlots(
      String shuffleKey,
      String hostName,
      Map<WorkerInfo, Integer> workerToAllocatedSlots,
      String requestId) {
    try {
      ResourceProtos.RequestSlotsRequest.Builder builder =
        ResourceProtos.RequestSlotsRequest.newBuilder()
              .setShuffleKey(shuffleKey)
              .setHostName(hostName);
      for (WorkerInfo workerInfo : workerToAllocatedSlots.keySet()) {
        builder.addWorkerInfo(WorkerInfo.encodeToPbStr(workerInfo.host(), workerInfo.rpcPort(),
          workerInfo.pushPort(), workerInfo.fetchPort(), workerInfo.replicatePort(),
          workerToAllocatedSlots.get(workerInfo)));
      }
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.RequestSlots)
          .setRequestId(requestId)
          .setRequestSlotsRequest(builder.build())
          .build());
    } catch (ServiceException e) {
      LOG.error("Handle request slots for {} failed!", shuffleKey, e);
    }
  }

  @Override
  public void handleReleaseSlots(String shuffleKey, List<String> workerIds,
                                 List<Integer> slots, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.ReleaseSlots)
          .setRequestId(requestId)
          .setReleaseSlotsRequest(
            ResourceProtos.ReleaseSlotsRequest.newBuilder()
                          .setShuffleKey(shuffleKey)
                          .addAllWorkerIds(workerIds)
                          .addAllSlots(slots)
                          .build())
              .build());

    } catch (ServiceException e) {
      LOG.error("Handle release slots for {} failed!", shuffleKey, e);
    }
  }

  @Override
  public void handleUnRegisterShuffle(String shuffleKey, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.UnRegisterShuffle)
          .setRequestId(requestId)
          .setUnregisterShuffleRequest(
            ResourceProtos.UnregisterShuffleRequest.newBuilder()
                  .setShuffleKey(shuffleKey)
                  .build())
          .build());
    } catch (ServiceException e) {
      LOG.error("Handle unregister shuffle for {} failed!", shuffleKey, e);
    }
  }

  @Override
  public void handleAppHeartbeat(String appId, long time, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.AppHeartBeat)
          .setRequestId(requestId)
          .setAppHeartbeatRequest(
            ResourceProtos.AppHeartbeatRequest.newBuilder()
                  .setAppId(appId).setTime(time)
                  .build())
          .build());
    } catch (ServiceException e) {
      LOG.error("Handle heart beat for {} failed!", appId, e);
    }
  }

  @Override
  public void handleAppLost(String appId, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.AppLost)
          .setRequestId(requestId)
          .setAppLostRequest(
            ResourceProtos.AppLostRequest.newBuilder()
                  .setAppId(appId)
                  .build())
          .build());
    } catch (ServiceException e) {
      LOG.error("Handle app lost for {} failed!", appId, e);
    }
  }

  @Override
  public void handleWorkerLost(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
              .setCmdType(Type.WorkerLost)
              .setRequestId(requestId)
              .setWorkerLostRequest(
                ResourceProtos.WorkerLostRequest.newBuilder()
                              .setHost(host)
                              .setRpcPort(rpcPort)
                              .setPushPort(pushPort)
                              .setFetchPort(fetchPort)
                              .setReplicatePort(replicatePort)
                              .build())
              .build());
    } catch (ServiceException e) {
      LOG.error("Handle worker lost for {} failed!", host, e);
    }
  }

  @Override
  public void handleWorkerHeartBeat(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, int numSlots, long time, Map<String, Long> shuffleDiskUsage,
    String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
              .setCmdType(Type.WorkerHeartBeat)
              .setRequestId(requestId)
              .setWorkerHeartBeatRequest(
                ResourceProtos.WorkerHeartBeatRequest.newBuilder()
                              .setHost(host)
                              .setRpcPort(rpcPort)
                              .setPushPort(pushPort)
                              .setFetchPort(fetchPort)
                              .setReplicatePort(replicatePort)
                              .setNumSlots(numSlots)
                              .setTime(time)
                              .putAllShuffleDiskUsage(shuffleDiskUsage)
                              .build())
              .build());
    } catch (ServiceException e) {
      LOG.error("Handle worker heartbeat for {} failed!", host, e);
    }
  }

  @Override
  public void handleRegisterWorker(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, int numSlots, String requestId) {
    try {
      ratisServer.submitRequest(ResourceRequest.newBuilder()
          .setCmdType(Type.RegisterWorker)
          .setRequestId(requestId)
          .setRegisterWorkerRequest(
            ResourceProtos.RegisterWorkerRequest.newBuilder()
                  .setHost(host)
                  .setRpcPort(rpcPort)
                  .setPushPort(pushPort)
                  .setFetchPort(fetchPort)
                  .setReplicatePort(replicatePort)
                  .setNumSlots(numSlots)
                  .build())
          .build());
    } catch (ServiceException e) {
      LOG.error("Handle worker register for {} failed!", host, e);
    }
  }

  @Override
  public void handleReportWorkerFailure(List<WorkerInfo> failedNodes, String requestId) {
    try {
      List<ResourceProtos.WorkerAddress> addrs = failedNodes.stream()
              .map(MetaUtil::infoToAddr).collect(Collectors.toList());
      ratisServer.submitRequest(ResourceRequest.newBuilder()
              .setCmdType(Type.ReportWorkerFailure)
              .setRequestId(requestId)
              .setReportWorkerFailureRequest(
                ResourceProtos.ReportWorkerFailureRequest.newBuilder()
                              .addAllFailedWorker(addrs)
                              .build())
              .build());
    } catch (ServiceException e) {
      LOG.error("Handle report node failure for {} failed !", failedNodes);
    }
  }

}
