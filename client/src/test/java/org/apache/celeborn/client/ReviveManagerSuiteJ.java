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

package org.apache.celeborn.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class ReviveManagerSuiteJ {

  private static final int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;
  private static final Map<Integer, Integer> SUCCESS =
      Collections.singletonMap(PARTITION_ID, (int) StatusCode.SUCCESS.getValue());

  private PartitionLocation loc(int epoch, String host) {
    return new PartitionLocation(
        PARTITION_ID, epoch, host, 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  }

  // A real ShuffleClientImpl (spied) so the batch scheduler can read real fields
  // (reducePartitionMap, mapperEndMap); a plain mock has null fields and the scheduler
  // thread would die on an NPE, silently cancelling all future ticks.
  private ShuffleClientImpl spyClient(PartitionLocationGroup group) {
    ShuffleClientImpl client =
        spy(
            new ShuffleClientImpl(
                "app-revive-manager-test", new CelebornConf(), new UserIdentifier("mock", "mock")));
    ConcurrentHashMap<Integer, PartitionLocationGroup> partitionMap = new ConcurrentHashMap<>();
    if (group != null) {
      partitionMap.put(PARTITION_ID, group);
    }
    client.reducePartitionMap.put(SHUFFLE_ID, partitionMap);
    return client;
  }

  private ReviveManager newManager(ShuffleClientImpl client) {
    CelebornConf conf = new CelebornConf();
    conf.set(
        CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_ENABLED().key(), "true");
    return new ReviveManager(client, conf);
  }

  @Test
  public void testBatchRetireReportsRebuiltFromOutstandingRetires() throws Exception {
    // Every queued request below is locally satisfied, so the batch carries retire reports
    // only, rebuilt from the group's outstanding set.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")));
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);

    CountDownLatch sent = new CountDownLatch(1);
    AtomicReference<List<ReviveRequest>> sentRequests = new AtomicReference<>();
    doAnswer(
            invocation -> {
              sentRequests.set(new ArrayList<>(invocation.getArgument(2)));
              sent.countDown();
              return new java.util.HashMap<>(SUCCESS);
            })
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      // Duplicate epoch-0 reports from three mappers, one epoch-1 report, and one report of
      // epoch 99 which the group has never seen (digested and evicted long ago).
      for (int mapId : Arrays.asList(1, 2, 3)) {
        manager.addRequest(
            new ReviveRequest(
                SHUFFLE_ID, mapId, 0, PARTITION_ID, 0, loc(0, "w1"), StatusCode.HARD_SPLIT));
      }
      manager.addRequest(
          new ReviveRequest(
              SHUFFLE_ID, 4, 0, PARTITION_ID, 1, loc(1, "w2"), StatusCode.HARD_SPLIT));
      manager.addRequest(
          new ReviveRequest(
              SHUFFLE_ID, 5, 0, PARTITION_ID, 99, loc(99, "wX"), StatusCode.HARD_SPLIT));

      assertTrue(sent.await(10, TimeUnit.SECONDS));
      Set<Integer> epochs =
          sentRequests.get().stream().map(r -> r.epoch).collect(Collectors.toSet());
      assertEquals(new HashSet<>(Arrays.asList(0, 1)), epochs);
      for (ReviveRequest req : sentRequests.get()) {
        assertEquals(StatusCode.HARD_SPLIT, req.cause);
      }
    } finally {
      manager.close();
    }
  }
}
