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

package com.aliyun.emr.rss.service.deploy.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import scala.Tuple2;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class MasterUtil {
  private static final Random rand = new Random();

  public static Map<WorkerInfo, Integer> workerToAllocatedSlots(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
    Iterator<WorkerInfo> workers = slots.keySet().iterator();
    Map<WorkerInfo, Integer> workerToSlots = new HashMap<>();
    while (workers.hasNext()) {
      WorkerInfo worker = workers.next();
      workerToSlots.put(worker,
          slots.get(worker)._1.size() + slots.get(worker)._2.size());
    }
    return workerToSlots;
  }

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlots(
      String shuffleKey,
      List<WorkerInfo> workers,
      List<Integer> reduceIds,
      boolean shouldReplicate) {
    int[] oldEpochs = new int[reduceIds.size()];
    Arrays.fill(oldEpochs, -1);
    return offerSlots(shuffleKey, workers, reduceIds, oldEpochs, shouldReplicate);
  }

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlots(
      String shuffleKey,
      List<WorkerInfo> workers,
      List<Integer> reduceIds,
      int[] oldEpochs,
      boolean shouldReplicate) {
    if (workers.size() < 2 && shouldReplicate) {
      return null;
    }

    int masterInd = rand.nextInt(workers.size());
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();
    // foreach iteration, allocate both master and slave partitions
    for(int idx = 0; idx < reduceIds.size(); idx++) {
      int nextMasterInd = masterInd;
      // try to find slot for master partition
      while (!workers.get(nextMasterInd).slotAvailable()) {
        nextMasterInd = (nextMasterInd + 1) % workers.size();
        if (nextMasterInd == masterInd) {
          return null;
        }
      }
      int nextSlaveInd = 0;
      if (shouldReplicate) {
        // try to find slot for slave partition
        nextSlaveInd = (nextMasterInd + 1) % workers.size();
        while (!workers.get(nextSlaveInd).slotAvailable()) {
          nextSlaveInd = (nextSlaveInd + 1) % workers.size();
          if (nextSlaveInd == nextMasterInd) {
            return null;
          }
        }
        if (nextSlaveInd == nextMasterInd) {
          return null;
        }
      }
      // now nextMasterInd/nextSlaveInd point to
      // available master/slave partition respectively

      int newEpoch = oldEpochs[idx] + 1;
      // new slave and master locations
      slots.putIfAbsent(workers.get(nextMasterInd),
          new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
      Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
          slots.get(workers.get(nextMasterInd));
      PartitionLocation slaveLocation = null;
      PartitionLocation masterLocation = null;
      if (shouldReplicate) {
        slaveLocation = new PartitionLocation(
            reduceIds.get(idx),
            newEpoch,
            workers.get(nextSlaveInd).host(),
            workers.get(nextSlaveInd).rpcPort(),
            workers.get(nextSlaveInd).pushPort(),
            workers.get(nextSlaveInd).fetchPort(),
            PartitionLocation.Mode.Slave
        );
      }
      masterLocation = new PartitionLocation(
          reduceIds.get(idx),
          newEpoch,
          workers.get(nextMasterInd).host(),
          workers.get(nextMasterInd).rpcPort(),
          workers.get(nextMasterInd).pushPort(),
          workers.get(nextMasterInd).fetchPort(),
          PartitionLocation.Mode.Master,
          slaveLocation
      );
      if (shouldReplicate) {
        slaveLocation.setPeer(masterLocation);
      }

      // add master location to WorkerInfo
      locations._1.add(masterLocation);

      if (shouldReplicate) {
        // add slave location to WorkerInfo
        slots.putIfAbsent(workers.get(nextSlaveInd),
            new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
        locations = slots.get(workers.get(nextSlaveInd));
        locations._2.add(slaveLocation);
      }

      // update index
      masterInd = (nextMasterInd + 1) % workers.size();
    }
    return slots;
  }
}
