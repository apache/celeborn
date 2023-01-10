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

package org.apache.celeborn.client.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.Utils;

/*
 * Queue for push data,
 * it can take one special PushTask queue by partitionId,
 * and it can take one random PushTask queue.
 * workingQueuePerPartition: for PushTask queue per every partition.
 *
 * */
public class DataPushQueue {
  private static final Logger logger = LoggerFactory.getLogger(DataPushQueue.class);

  private final List<LinkedBlockingQueue<PushTask>> workingQueuePerPartition;
  private final PushState pushState;
  private final DataPusher dataPusher;
  private final int maxInFlight;

  private final String appId;
  private final int shuffleId;
  private final int numMappers;
  private final int numPartitions;
  private final ShuffleClient client;
  private int partitionIdIdx = 0;

  public DataPushQueue(
      CelebornConf conf,
      DataPusher dataPusher,
      ShuffleClient client,
      String appId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.client = client;
    this.dataPusher = dataPusher;
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    this.pushState = client.getOrRegisterPushState(mapKey);
    this.maxInFlight = conf.pushMaxReqsInFlight();
    final int capacity = conf.pushQueueCapacity();
    workingQueuePerPartition = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      workingQueuePerPartition.add(new LinkedBlockingQueue<>(capacity));
    }
  }

  public LinkedBlockingQueue<PushTask> takeAnyWorkingQueue() throws IOException {
    while (!dataPusher.terminatedOrHasException()) {
      int partitionId = nextPartitionId();
      LinkedBlockingQueue<PushTask> pushTasks;
      try {
        pushTasks = workingQueuePerPartition.get(partitionId);
      } catch (IndexOutOfBoundsException ex) {
        if (dataPusher.terminatedOrHasException()) {
          return null;
        }
        throw ex;
      }
      if (!pushTasks.isEmpty()) {
        Map<Integer, PartitionLocation> partitionLocationMap =
            client.getOrRegisterShuffle(appId, shuffleId, numMappers, numPartitions);
        PartitionLocation loc = partitionLocationMap.get(partitionId);
        boolean reachLimit = pushState.limitMaxInFlight(loc.hostAndPushPort(), maxInFlight);
        if (!reachLimit) {
          return pushTasks;
        }
      }
    }
    return null;
  }

  public LinkedBlockingQueue<PushTask> takeWorkingQueue(int partitionId) {
    return workingQueuePerPartition.get(partitionId);
  }

  public void clear() {
    workingQueuePerPartition.parallelStream().forEach(LinkedBlockingQueue::clear);
    workingQueuePerPartition.clear();
  }

  private int nextPartitionId() {
    return partitionIdIdx++ % numPartitions;
  }
}
