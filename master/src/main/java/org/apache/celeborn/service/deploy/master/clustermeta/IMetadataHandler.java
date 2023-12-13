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

import org.apache.celeborn.common.meta.WorkerInfo;

public interface IMetadataHandler {
  void handleRequestSlots(
      String shuffleKey,
      String hostName,
      Map<String, Map<String, Integer>> workerToAllocatedSlots,
      String requestId);

  void handleUnRegisterShuffle(String shuffleKey, String requestId);

  void handleAppHeartbeat(
      String appId, long totalWritten, long fileCount, long time, String requestId);

  void handleAppLost(String appId, String requestId);

  void handleWorkerExclude(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove, String requestId);

  void handleWorkerLost(WorkerInfo lostWorker, String requestId);

  void handleWorkerRemove(WorkerInfo targetWorker, String requestId);

  void handleRemoveWorkersUnavailableInfo(List<WorkerInfo> unavailableWorkers, String requestId);

  void handleWorkerHeartbeat(
      WorkerInfo workerInfo,
      Map<String, Long> estimatedAppDiskUsage,
      long time,
      boolean highWorkload,
      String requestId);

  void handleRegisterWorker(WorkerInfo workerInfo, String requestId);

  void handleReportWorkerUnavailable(List<WorkerInfo> failedNodes, String requestId);

  void handleUpdatePartitionSize();
}
