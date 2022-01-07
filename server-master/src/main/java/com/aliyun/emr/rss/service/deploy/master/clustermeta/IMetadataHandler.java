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

package com.aliyun.emr.rss.service.deploy.master.clustermeta;

import java.util.List;
import java.util.Map;

import com.aliyun.emr.rss.common.meta.WorkerInfo;

public interface IMetadataHandler {
  void handleRequestSlots(
      String shuffleKey,
      String hostName,
      Map<WorkerInfo, Integer> workerToAllocatedSlots);

  void handleReleaseSlots(
          String shuffleKey, List<String> workerIds, List<Integer> slots);

  void handleUnRegisterShuffle(String shuffleKey);

  void handleAppHeartbeat(String appId, long time);

  void handleAppLost(String appId);

  void handleWorkerLost(String host, int rpcPort, int pushPort, int fetchPort);

  void handleWorkerHeartBeat(String host, int rpcPort,
           int pushPort, int fetchPort, int numSlots, long time);

  void handleRegisterWorker(
      String host, int rpcPort, int pushPort, int fetchPort, int numSlots);

  void handleReportWorkerFailure(List<WorkerInfo> failedNodes);
}
