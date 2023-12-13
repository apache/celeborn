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

package org.apache.celeborn.service.deploy.master.clustermeta;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.AppDiskUsageMetric;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.service.deploy.master.network.CelebornRackResolver;

public class SingleMasterMetaManager extends AbstractMetaManager {
  private static final Logger LOG = LoggerFactory.getLogger(SingleMasterMetaManager.class);

  public SingleMasterMetaManager(RpcEnv rpcEnv, CelebornConf conf) {
    this.rpcEnv = rpcEnv;
    this.conf = conf;
    this.initialEstimatedPartitionSize = conf.initialEstimatedPartitionSize();
    this.estimatedPartitionSize = initialEstimatedPartitionSize;
    this.appDiskUsageMetric = new AppDiskUsageMetric(conf);
    this.rackResolver = new CelebornRackResolver(conf);
  }

  @Override
  public void handleRequestSlots(
      String shuffleKey,
      String hostName,
      Map<String, Map<String, Integer>> workerToAllocatedSlots,
      String requestId) {
    updateRequestSlotsMeta(shuffleKey, hostName, workerToAllocatedSlots);
  }

  @Override
  public void handleUnRegisterShuffle(String shuffleKey, String requestId) {
    updateUnregisterShuffleMeta(shuffleKey);
  }

  @Override
  public void handleAppHeartbeat(
      String appId, long totalWritten, long fileCount, long time, String requestId) {
    updateAppHeartbeatMeta(appId, time, totalWritten, fileCount);
  }

  @Override
  public void handleAppLost(String appId, String requestId) {
    updateAppLostMeta(appId);
  }

  @Override
  public void handleWorkerExclude(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove, String requestId) {
    updateWorkerExcludeMeta(workersToAdd, workersToRemove);
  }

  @Override
  public void handleWorkerLost(WorkerInfo lostWorker, String requestId) {
    updateWorkerLostMeta(lostWorker);
  }

  @Override
  public void handleWorkerRemove(WorkerInfo lostWorker, String requestId) {
    updateWorkerRemoveMeta(lostWorker);
  }

  @Override
  public void handleRemoveWorkersUnavailableInfo(
      List<WorkerInfo> unavailableWorkers, String requestId) {
    removeWorkersUnavailableInfoMeta(unavailableWorkers);
  }

  @Override
  public void handleWorkerHeartbeat(
      WorkerInfo workerInfo,
      Map<String, Long> estimatedAppDiskUsage,
      long time,
      boolean highWorkload,
      String requestId) {
    updateWorkerHeartbeatMeta(workerInfo, estimatedAppDiskUsage, time, highWorkload);
  }

  @Override
  public void handleRegisterWorker(WorkerInfo workerInfo, String requestId) {
    updateRegisterWorkerMeta(workerInfo);
  }

  @Override
  public void handleReportWorkerUnavailable(List<WorkerInfo> failedNodes, String requestId) {
    updateMetaByReportWorkerUnavailable(failedNodes);
  }

  @Override
  public void handleUpdatePartitionSize() {
    updatePartitionSize();
  }
}
