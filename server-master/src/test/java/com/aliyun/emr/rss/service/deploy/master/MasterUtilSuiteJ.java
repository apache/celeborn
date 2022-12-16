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

import com.aliyun.emr.rss.common.RssConf;
import scala.Tuple2;

import org.junit.Test;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class MasterUtilSuiteJ {
  RssConf rssConf = new RssConf();

  private List<WorkerInfo> prepareWorkers(int numSlots) {
    ArrayList<WorkerInfo> workers = new ArrayList<>(3);
    workers.add(new WorkerInfo("host1", 9, 10, 110, 113, numSlots, null));
    workers.add(new WorkerInfo("host2", 9, 11, 111, 114, numSlots, null));
    workers.add(new WorkerInfo("host3", 9, 12, 112, 115, numSlots, null));
    return workers;
  }

  @Test
  public void testAllocateSlotsForEmptyReduceId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.emptyList();
    final boolean shouldReplicate = true;

    check(0, 3, workers, reduceIds, shouldReplicate, true, rssConf);
  }

  @Test
  public void testAllocateSlotsForSingleReduceId() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.singletonList(0);
    final boolean shouldReplicate = true;

    check(2, 1, workers, reduceIds, shouldReplicate, true, rssConf);
  }

  @Test
  public void testAllocateSlotsForSingleReduceIdWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Collections.singletonList(0);
    final boolean shouldReplicate = false;

    check(1, 2, workers, reduceIds, shouldReplicate, true, rssConf);
  }

  @Test
  public void testAllocateSlotsForTwoReduceIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Arrays.asList(0, 1);
    final boolean shouldReplicate = false;

    check(2, 1, workers, reduceIds, shouldReplicate, true, rssConf);
  }

  @Test
  public void testAllocateSlotsForThreeReduceIdsWithoutReplicate() {
    final int numSlots = 1;
    final List<WorkerInfo> workers = prepareWorkers(numSlots);
    final List<Integer> reduceIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = false;

    check(3, 0, workers, reduceIds, shouldReplicate, true, rssConf);
  }

  public void testAllocateSlotsForOrderByFreeSlots() {
    final List<WorkerInfo> workers = new ArrayList<>(3);
    workers.add(new WorkerInfo("host1", 9, 10, 110, 113, 1, null));
    workers.add(new WorkerInfo("host2", 9, 11, 111, 114, 2, null));
    workers.add(new WorkerInfo("host3", 9, 12, 112, 115, 3, null));
    final List<Integer> reduceIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = false;

    RssConf conf = new RssConf();
    conf.set("rss.offer.slots.orderByFreeSlots", "true");
    check(3, 3, workers, reduceIds, shouldReplicate, true, conf);
  }

  @Test
  public void testAllocateSlotsByReduceIdsWithoutReplicate() {
    RssConf conf = new RssConf();
    conf.set("rss.offer.slots.minWorkers", "3");
    conf.set("rss.offer.slots.maxWorkers", "6");
    conf.set("rss.offer.slots.minPartitionsPerWorker", "2");

    List<Integer> reduceIds = null;
    boolean shouldReplicate = false;
    int totalSlots = 10 * 3;

    List<WorkerInfo> workers = genWorkers(10, 3);
    reduceIds = genReduceIds(1);
    check(1, totalSlots - 1, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(3);
    check(3, totalSlots - 3, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(4);
    check(3, totalSlots - 4, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(6);
    check(3, totalSlots - 6, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(7);
    check(4, totalSlots - 7, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(12);
    check(6, totalSlots - 12, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(13);
    check(6, totalSlots - 13, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(18);
    check(6, totalSlots - 18, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(19);
    check(10, totalSlots - 19, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(30);
    check(10, totalSlots - 30, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(31);
    check(0, totalSlots, workers, reduceIds, shouldReplicate, false, conf);
  }

  @Test
  public void testAllocateSlotsByReduceIdsWithReplicate() {
    RssConf conf = new RssConf();
    conf.set("rss.offer.slots.minWorkers", "3");
    conf.set("rss.offer.slots.maxWorkers", "6");
    conf.set("rss.offer.slots.minPartitionsPerWorker", "2");

    List<Integer> reduceIds = null;
    boolean shouldReplicate = true;
    int totalSlots = 10 * 3;

    List<WorkerInfo> workers = genWorkers(10, 3);
    reduceIds = genReduceIds(1);
    check(2, totalSlots - 2, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(3);
    check(3, totalSlots - 6, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(4);
    check(4, totalSlots - 8, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(6);
    check(6, totalSlots - 12, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(7);
    check(6, totalSlots - 14, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(9);
    check(6, totalSlots - 18, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(10);
    check(10, totalSlots - 20, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(15);
    check(10, totalSlots - 30, workers, reduceIds, shouldReplicate, true, conf);

    workers = genWorkers(10, 3);
    reduceIds = genReduceIds(16);
    check(0, totalSlots, workers, reduceIds, shouldReplicate, false, conf);
  }

  private List<WorkerInfo> genWorkers(int num, int slotPerWorker) {
    List<WorkerInfo> workers = new ArrayList<>(num);
    for (int i = 1; i <= num; i++) {
      workers.add(new WorkerInfo("host" + i, 9, 10, 110, 113, slotPerWorker, null));
    }
    return workers;
  }

  private List<Integer> genReduceIds(int num) {
    List<Integer> res = new ArrayList(num);
    for (int i = 0; i < num; i++) {
      res.add(i);
    }
    return res;
  }

  private void check(
      int usedWorkers,
      int expectAvailableSlots,
      List<WorkerInfo> workers,
      List<Integer> reduceIds,
      boolean shouldReplicate,
      boolean expectSuccess,
      RssConf rssConf) {
    String shuffleKey = "appId-1";
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        MasterUtil.offerSlots(shuffleKey, workers, reduceIds, shouldReplicate, rssConf);

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
      assert realAvailableSlots == expectAvailableSlots : "Offer slots for three reduceIds, expect "
          + expectAvailableSlots + " available slots, but real is " + realAvailableSlots;
    } else {
      assert null == slots: "Expect to fail to offer slots, but return " + slots.size() + " slots.";
    }
  }
}
