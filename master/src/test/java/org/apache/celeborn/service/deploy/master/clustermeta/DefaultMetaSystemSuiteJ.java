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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEndpointAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.rpc.netty.NettyRpcEndpointRef;

public class DefaultMetaSystemSuiteJ {

  private final RpcEnv mockRpcEnv = mock(RpcEnv.class);
  private final CelebornConf conf = new CelebornConf();
  private AbstractMetaManager statusSystem;
  private final RpcEndpointRef dummyRef =
      new NettyRpcEndpointRef(
          new CelebornConf(), RpcEndpointAddress.apply("localhost", 111, "dummy"), null);
  private final AtomicLong callerId = new AtomicLong();

  private static final String HOSTNAME1 = "host1";
  private static final int RPCPORT1 = 1111;
  private static final int PUSHPORT1 = 1112;
  private static final int FETCHPORT1 = 1113;
  private static final int REPLICATEPORT1 = 1114;
  private static final Map<String, DiskInfo> disks1 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption1 =
      new HashMap<>();

  private static final String HOSTNAME2 = "host2";
  private static final int RPCPORT2 = 2111;
  private static final int PUSHPORT2 = 2112;
  private static final int FETCHPORT2 = 2113;
  private static final int REPLICATEPORT2 = 2114;
  private static final Map<String, DiskInfo> disks2 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption2 =
      new HashMap<>();

  private static final String HOSTNAME3 = "host3";
  private static final int RPCPORT3 = 3111;
  private static final int PUSHPORT3 = 3112;
  private static final int FETCHPORT3 = 3113;
  private static final int REPLICATEPORT3 = 3114;
  private static final Map<String, DiskInfo> disks3 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption3 =
      new HashMap<>();

  @Before
  public void setUp() {
    when(mockRpcEnv.setupEndpointRef(any(), any())).thenReturn(dummyRef);
    statusSystem = new SingleMasterMetaManager(mockRpcEnv, conf);

    disks1.clear();
    disks1.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));

    disks2.clear();
    disks2.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));

    disks3.clear();
    disks3.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
  }

  @After
  public void tearDown() {}

  private String getNewReqeustId() {
    return MasterClient.encodeRequestId(UUID.randomUUID().toString(), callerId.incrementAndGet());
  }

  @Test
  public void testHandleRegisterWorker() {

    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    assertEquals(3, statusSystem.workers.size());
  }

  @Test
  public void testHandleWorkerExclude() {
    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            disks2,
            userResourceConsumption2);

    statusSystem.handleRegisterWorker(
        workerInfo1.host(),
        workerInfo1.rpcPort(),
        workerInfo1.pushPort(),
        workerInfo1.fetchPort(),
        workerInfo1.replicatePort(),
        workerInfo1.diskInfos(),
        workerInfo1.userResourceConsumption(),
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        workerInfo2.host(),
        workerInfo2.rpcPort(),
        workerInfo2.pushPort(),
        workerInfo2.fetchPort(),
        workerInfo2.replicatePort(),
        workerInfo2.diskInfos(),
        workerInfo2.userResourceConsumption(),
        getNewReqeustId());

    statusSystem.handleWorkerExclude(
        Arrays.asList(workerInfo1, workerInfo2), Collections.emptyList(), getNewReqeustId());
    assertEquals(2, statusSystem.manuallyExcludedWorkers.size());

    statusSystem.handleWorkerExclude(
        Collections.emptyList(), Collections.singletonList(workerInfo1), getNewReqeustId());
    assertEquals(1, statusSystem.manuallyExcludedWorkers.size());
  }

  @Test
  public void testHandleWorkerLost() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    statusSystem.handleWorkerLost(
        HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1, getNewReqeustId());
    assertEquals(2, statusSystem.workers.size());
  }

  private static final String APPID1 = "appId1";
  private static final int SHUFFLEID1 = 1;
  private static final String SHUFFLEKEY1 = APPID1 + "-" + SHUFFLEID1;

  @Test
  public void testHandleRequestSlots() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            disks2,
            userResourceConsumption2);
    WorkerInfo workerInfo3 =
        new WorkerInfo(
            HOSTNAME3,
            RPCPORT3,
            PUSHPORT3,
            FETCHPORT3,
            REPLICATEPORT3,
            disks3,
            userResourceConsumption3);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(0, workerInfo1.usedSlots());
    assertEquals(0, workerInfo2.usedSlots());
    assertEquals(0, workerInfo3.usedSlots());
  }

  @Test
  public void testHandleReleaseSlots() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    assertEquals(3, statusSystem.workers.size());

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(
        statusSystem.workers.stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .toUniqueId(),
        allocation);
    workersToAllocate.put(
        statusSystem.workers.stream()
            .filter(w -> w.host().equals(HOSTNAME2))
            .findFirst()
            .get()
            .toUniqueId(),
        allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    assertEquals(
        0,
        statusSystem.workers.stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
  }

  @Test
  public void testHandleAppLost() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            disks2,
            userResourceConsumption2);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(1, statusSystem.registeredShuffle.size());

    statusSystem.handleAppLost(APPID1, getNewReqeustId());

    assertTrue(statusSystem.registeredShuffle.isEmpty());
  }

  @Test
  public void testHandleUnRegisterShuffle() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            disks2,
            userResourceConsumption2);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(1, statusSystem.registeredShuffle.size());

    statusSystem.handleUnRegisterShuffle(SHUFFLEKEY1, getNewReqeustId());

    assertTrue(statusSystem.registeredShuffle.isEmpty());
  }

  @Test
  public void testHandleAppHeartbeat() {
    Long dummy = 1235L;
    statusSystem.handleAppHeartbeat(APPID1, 1, 1, dummy, getNewReqeustId());
    assertEquals(dummy, statusSystem.appHeartbeatTime.get(APPID1));

    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(appId2, 1, 1, dummy, getNewReqeustId());
    assertEquals(dummy, statusSystem.appHeartbeatTime.get(appId2));

    assertEquals(2, statusSystem.appHeartbeatTime.size());
  }

  @Test
  public void testHandleWorkerHeartbeat() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        new HashMap<>(),
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        new HashMap<>(),
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        new HashMap<>(),
        userResourceConsumption3,
        getNewReqeustId());

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        new HashMap<>(),
        userResourceConsumption1,
        new HashMap<>(),
        1,
        false,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 1);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        new HashMap<>(),
        userResourceConsumption2,
        new HashMap<>(),
        1,
        false,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 2);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        new HashMap<>(),
        1,
        false,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 2);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        new HashMap<>(),
        1,
        true,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 3);
  }

  @Test
  public void testHandleReportWorkerFailure() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    List<WorkerInfo> failedWorkers = new ArrayList<>();
    failedWorkers.add(
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            disks1,
            userResourceConsumption1));

    statusSystem.handleReportWorkerUnavailable(failedWorkers, getNewReqeustId());
    assertEquals(1, statusSystem.shutdownWorkers.size());
    assertTrue(statusSystem.excludedWorkers.isEmpty());
  }

  @Test
  public void testHandleUpdatePartitionSize() {
    statusSystem.handleUpdatePartitionSize();
  }
}
