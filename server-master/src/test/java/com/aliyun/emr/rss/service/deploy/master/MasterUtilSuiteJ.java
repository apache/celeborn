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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import org.junit.Test;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class MasterUtilSuiteJ {

  private List<WorkerInfo> prepareWorkers(int numSlots) {
    ArrayList<WorkerInfo> workers = new ArrayList<>(3);
    workers.add(new WorkerInfo("host1", 9, 10, 110, 113, numSlots, null));
    workers.add(new WorkerInfo("host2", 9, 11, 111, 114, numSlots, null));
    workers.add(new WorkerInfo("host3", 9, 12, 112, 115, numSlots, null));
    return workers;
  }

  @Test
  public void testAllocateSlotsForEmptyPartitionId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> partitionIds = Collections.emptyList();
    final boolean shouldReplicate = true;

    check(0, 3, workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSinglePartitionId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> partitionIds = Collections.singletonList(0);
    final boolean shouldReplicate = true;

    check(2, 1, workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSinglePartitionIdWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> partitionIds = Collections.singletonList(0);
    final boolean shouldReplicate = false;

    check(1, 2, workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForTwoPartitionIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> partitionIds = Arrays.asList(0, 1);
    final boolean shouldReplicate = false;

    check(2, 1, workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForThreePartitionIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> partitionIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = false;

    check(3, 0, workers, partitionIds, shouldReplicate, true);
  }

  private void check(
      int usedWorkers,
      int expectAvailableSlots,
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean expectSuccess) {
    String shuffleKey = "appId-1";
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        MasterUtil.offerSlots(workers, partitionIds, shouldReplicate);

    if (expectSuccess) {
      assert usedWorkers == slots.size() : "Offer slots, expect to return "
          + usedWorkers + ", but return " + slots.size() + " slots.";

      Map<WorkerInfo, Integer> workerToAllocatedSlots = MasterUtil.workerToAllocatedSlots(slots);
      assert workerToAllocatedSlots.size() == usedWorkers;
      for (Map.Entry<WorkerInfo, Integer> workerToNumSlots : workerToAllocatedSlots.entrySet()) {
        WorkerInfo worker = workerToNumSlots.getKey();
        int numSlot = workerToNumSlots.getValue();
        worker.allocateSlots(shuffleKey, numSlot);
      }
      int realAvailableSlots = 0;
      for (WorkerInfo worker : workers) {
        realAvailableSlots += worker.freeSlots();
      }
      assert realAvailableSlots == expectAvailableSlots : "Offer slots for three partitionIds," +
        "expect " + expectAvailableSlots + " available slots, but real is " + realAvailableSlots;
    } else {
      assert null == slots: "Expect to fail to offer slots, but return " + slots.size() + " slots.";
    }
  }
}
