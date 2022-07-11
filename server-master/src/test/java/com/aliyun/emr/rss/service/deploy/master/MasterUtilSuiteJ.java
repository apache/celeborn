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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import org.junit.Test;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class MasterUtilSuiteJ {

  private List<WorkerInfo> prepareWorkers(int numSlots) {
    Map<String, DiskInfo> disks = new HashMap<>();
    for (int i = 0; i < numSlots; i++) {
      disks.put("/mnt/disk1" + i, new DiskInfo("/mnt/disk1", 100 * 1024 * 1024, 1099.0, 0));
      disks.put("/mnt/disk2" + i, new DiskInfo("/mnt/disk2", 100 * 1024 * 1024, 1099.0, 0));
      disks.put("/mnt/disk3" + i, new DiskInfo("/mnt/disk3", 100 * 1024 * 1024, 1099.0, 0));
    }
    ArrayList<WorkerInfo> workers = new ArrayList<>(3);
    workers.add(new WorkerInfo("host1", 9, 10, 110, 113, disks, null));
    workers.add(new WorkerInfo("host2", 9, 11, 111, 114, disks, null));
    workers.add(new WorkerInfo("host3", 9, 12, 112, 115, disks, null));
    return workers;
  }

  @Test
  public void testAllocateSlotsForEmptyReduceId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.emptyList();
    final boolean shouldReplicate = true;

    check(0, 3, workers, reduceIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSingleReduceId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.singletonList(0);
    final boolean shouldReplicate = true;

    check(2, 1, workers, reduceIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSingleReduceIdWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.singletonList(0);
    final boolean shouldReplicate = false;

    check(1, 2, workers, reduceIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForTwoReduceIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Arrays.asList(0, 1);
    final boolean shouldReplicate = false;

    check(2, 1, workers, reduceIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForThreeReduceIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = false;

    check(3, 0, workers, reduceIds, shouldReplicate, true);
  }

  private void check(
      int usedWorkers,
      int expectAvailableSlots,
      List<WorkerInfo> workers,
      List<Integer> reduceIds,
      boolean shouldReplicate,
      boolean expectSuccess) {
    String shuffleKey = "appId-1";
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
      MasterUtil.offerSlots(workers, reduceIds, shouldReplicate, 10 * 1024 * 1024 * 1024L);

    if (expectSuccess) {
      assert usedWorkers == slots.size() : "Offer slots, expect to return "
          + usedWorkers + ", but return " + slots.size() + " slots.";

      Map<WorkerInfo, Map<String, Integer>> workerToAllocatedSlots =
        MasterUtil.workerToAllocatedSlots(slots);
      assert workerToAllocatedSlots.size() == usedWorkers;
      for (Map.Entry<WorkerInfo, Map<String, Integer>> entry : workerToAllocatedSlots.entrySet()) {
        WorkerInfo worker = entry.getKey();
        Map<String, Integer> allocationMap = entry.getValue();
        worker.allocateSlots(shuffleKey, allocationMap);
      }
      //int realAvailableSlots = 0;
      for (WorkerInfo worker : workers) {
        //realAvailableSlots += worker.freeSlots();
      }
      //assert realAvailableSlots == expectAvailableSlots :
      //  "Offer slots for three reduceIds, expect "
      //    + expectAvailableSlots + " available slots, but real is " + realAvailableSlots;
    } else {
      assert null == slots: "Expect to fail to offer slots, but return " + slots.size() + " slots.";
    }
  }
}
