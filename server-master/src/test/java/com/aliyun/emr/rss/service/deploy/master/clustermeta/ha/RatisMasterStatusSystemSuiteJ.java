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

package com.aliyun.emr.rss.service.deploy.master.clustermeta.ha;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.rpc.RpcEndpointAddress;
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.rss.common.rpc.RpcEnv;
import com.aliyun.emr.rss.common.rpc.netty.NettyRpcEndpointRef;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.AbstractMetaManager;

public class RatisMasterStatusSystemSuiteJ {
  protected static HARaftServer RATISSERVER1 = null;
  protected static HARaftServer RATISSERVER2 = null;
  protected static HARaftServer RATISSERVER3 = null;
  protected static HAMasterMetaManager STATUSSYSTEM1 = null;
  protected static HAMasterMetaManager STATUSSYSTEM2 = null;
  protected static HAMasterMetaManager STATUSSYSTEM3 = null;

  private static RpcEndpointRef dummyRef = new NettyRpcEndpointRef(new RssConf(),
      RpcEndpointAddress.apply("localhost", 111, "dummy"), null);

  protected static RpcEnv mockRpcEnv = Mockito.mock(RpcEnv.class);
  protected static RpcEndpointRef mockRpcEndpoint = Mockito.mock(RpcEndpointRef.class);

  private static String DEFAULT_SERVICE_ID = "RSS_DEFAULT_SERVICE_ID";

  @BeforeClass
  public static void init() throws IOException, InterruptedException {
    Mockito.when(mockRpcEnv.setupEndpointRef(Mockito.any(), Mockito.any()))
        .thenReturn(mockRpcEndpoint);
    when(mockRpcEnv.setupEndpointRef(any(), any())).thenReturn(dummyRef);

    STATUSSYSTEM1 = new HAMasterMetaManager(mockRpcEnv, new RssConf());
    STATUSSYSTEM2 = new HAMasterMetaManager(mockRpcEnv, new RssConf());
    STATUSSYSTEM3 = new HAMasterMetaManager(mockRpcEnv, new RssConf());

    MetaHandler handler1 = new MetaHandler(STATUSSYSTEM1);
    MetaHandler handler2 = new MetaHandler(STATUSSYSTEM2);
    MetaHandler handler3 = new MetaHandler(STATUSSYSTEM3);

    RssConf conf1 = new RssConf();
    File tmpDir1 = File.createTempFile("rss-ratis1", "for-test-only");
    tmpDir1.delete();
    tmpDir1.mkdirs();
    conf1.set("rss.ha.storage.dir", tmpDir1.getAbsolutePath());

    RssConf conf2 = new RssConf();
    File tmpDir2 = File.createTempFile("rss-ratis2", "for-test-only");
    tmpDir2.delete();
    tmpDir2.mkdirs();
    conf2.set("rss.ha.storage.dir", tmpDir2.getAbsolutePath());

    RssConf conf3 = new RssConf();
    File tmpDir3 = File.createTempFile("rss-ratis3", "for-test-only");
    tmpDir3.delete();
    tmpDir3.mkdirs();
    conf3.set("rss.ha.storage.dir", tmpDir3.getAbsolutePath());

    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    String id3 = UUID.randomUUID().toString();
    int ratisPort1 = 9872;
    int ratisPort2 = 9873;
    int ratisPort3 = 9874;

    ServerSocket s = new ServerSocket();
    s.bind(new InetSocketAddress(InetAddress.getByName(null), 0), 1);
    String localHost = ((InetSocketAddress) s.getLocalSocketAddress()).getHostName();

    InetSocketAddress rpcAddress1 = new InetSocketAddress(localHost, 9872);
    InetSocketAddress rpcAddress2 = new InetSocketAddress(localHost, 9873);
    InetSocketAddress rpcAddress3 = new InetSocketAddress(localHost, 9874);
    NodeDetails nodeDetails1 = new NodeDetails.Builder()
        .setRpcAddress(rpcAddress1)
        .setRatisPort(ratisPort1)
        .setNodeId(id1)
        .setServiceId(DEFAULT_SERVICE_ID)
        .build();
    NodeDetails nodeDetails2 = new NodeDetails.Builder()
        .setRpcAddress(rpcAddress2)
        .setRatisPort(ratisPort2)
        .setNodeId(id2)
        .setServiceId(DEFAULT_SERVICE_ID)
        .build();
    NodeDetails nodeDetails3 = new NodeDetails.Builder()
        .setRpcAddress(rpcAddress3)
        .setRatisPort(ratisPort3)
        .setNodeId(id3)
        .setServiceId(DEFAULT_SERVICE_ID)
        .build();

    List<NodeDetails> peersForNode1 = new ArrayList() {{
        add(nodeDetails2);
        add(nodeDetails3);
      }};
    List<NodeDetails> peersForNode2 = new ArrayList() {{
        add(nodeDetails1);
        add(nodeDetails3);
      }};
    List<NodeDetails> peersForNode3 = new ArrayList() {{
        add(nodeDetails1);
        add(nodeDetails2);
      }};

    RATISSERVER1 = HARaftServer.newMasterRatisServer(handler1, conf1, nodeDetails1, peersForNode1);
    RATISSERVER2 = HARaftServer.newMasterRatisServer(handler2, conf2, nodeDetails2, peersForNode2);
    RATISSERVER3 = HARaftServer.newMasterRatisServer(handler3, conf3, nodeDetails3, peersForNode3);

    STATUSSYSTEM1.setRatisServer(RATISSERVER1);
    STATUSSYSTEM2.setRatisServer(RATISSERVER2);
    STATUSSYSTEM3.setRatisServer(RATISSERVER3);

    RATISSERVER1.start();
    RATISSERVER2.start();
    RATISSERVER3.start();

    Thread.sleep(15000L);
  }

  @Test
  public void testLeaderAvaiable() throws InterruptedException {
    boolean hasLeader = false;
    if (RATISSERVER1.isLeader() || RATISSERVER2.isLeader() || RATISSERVER3.isLeader()) {
      hasLeader = true;
    }
    assert hasLeader;
  }

  private static String HOSTNAME1 = "host1";
  private static int RPCPORT1 = 1111;
  private static int PUSHPORT1 = 1112;
  private static int FETCHPORT1 = 1113;
  private static int NUMSLOTS1 = 10;

  private static String HOSTNAME2 = "host2";
  private static int RPCPORT2 = 2111;
  private static int PUSHPORT2 = 2112;
  private static int FETCHPORT2 = 2113;
  private static int NUMSLOTS2 = 10;

  private static String HOSTNAME3 = "host3";
  private static int RPCPORT3 = 3111;
  private static int PUSHPORT3 = 3112;
  private static int FETCHPORT3 = 3113;
  private static int NUMSLOTS3 = 10;

  private AtomicLong callerId = new AtomicLong();
  private static String APPID1 = "appId1";
  private static int SHUFFLEID1 = 1;
  private static String SHUFFLEKEY1 = APPID1 + "-" + SHUFFLEID1;

  public HAMasterMetaManager pickLeaderStatusSystem() {
    if (RATISSERVER1.isLeader()) {
      return STATUSSYSTEM1;
    }
    if (RATISSERVER2.isLeader()) {
      return STATUSSYSTEM2;
    }
    if (RATISSERVER3.isLeader()) {
      return STATUSSYSTEM3;
    }
    return null;
  }

  @Test
  public void testHandleRegisterWorker() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);
    Thread.sleep(3000L);

    Assert.assertEquals(STATUSSYSTEM1.workers.size(), 3);
    assert STATUSSYSTEM1.workers.size() == 3;
    assert STATUSSYSTEM2.workers.size() == 3;
    assert STATUSSYSTEM3.workers.size() == 3;
  }

  @Test
  public void testHandleWorkerLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    statusSystem.handleWorkerLost(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.workers.size() == 2;
    assert STATUSSYSTEM2.workers.size() == 2;
    assert STATUSSYSTEM3.workers.size() == 2;
  }

  @Test
  public void testHandleRequestSlots() {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2, dummyRef);
    WorkerInfo workerInfo3 = new WorkerInfo(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3, dummyRef);

    Map<WorkerInfo, Integer> workersToAllocate = new HashMap<>();
    workersToAllocate.put(workerInfo1, 5);
    workersToAllocate.put(workerInfo2, 5);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate);

    assert (statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME1)).findFirst()
        .get().usedSlots() == 5);
    assert (statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME2)).findFirst()
        .get().usedSlots() == 5);
    assert (statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME3)).findFirst()
        .get().usedSlots() == 0);
  }

  @Test
  public void testHandleReleaseSlots() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);
    Thread.sleep(3000L);

    assert 3 == STATUSSYSTEM1.workers.size();
    assert 3 == STATUSSYSTEM2.workers.size();
    assert 3 == STATUSSYSTEM3.workers.size();

    Map<WorkerInfo, Integer> workersToAllocate = new HashMap<>();
    workersToAllocate.put(statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
        .findFirst().get(), 5);
    workersToAllocate.put(statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME2))
        .findFirst().get(), 5);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate);
    Thread.sleep(3000L);

    List<String> workerIds = new ArrayList<>();
    workerIds.add(HOSTNAME1 + ":" + RPCPORT1 + ":" + PUSHPORT1 + ":" + FETCHPORT1);

    List<Integer> workerSlots = new ArrayList<>();
    workerSlots.add(3);

    statusSystem.handleReleaseSlots(SHUFFLEKEY1, workerIds, workerSlots);
    Thread.sleep(3000L);

    assert 2 == STATUSSYSTEM1.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
        .findFirst().get().usedSlots();
    assert 2 == STATUSSYSTEM2.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
        .findFirst().get().usedSlots();
    assert 2 == STATUSSYSTEM3.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
        .findFirst().get().usedSlots();
  }

  @Test
  public void testHandleAppLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    Thread.sleep(3000L);
    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2, dummyRef);

    Map<WorkerInfo, Integer> workersToAllocate = new HashMap<>();
    workersToAllocate.put(workerInfo1, 5);
    workersToAllocate.put(workerInfo2, 5);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.registeredShuffle.size() == 1;
    assert STATUSSYSTEM2.registeredShuffle.size() == 1;
    assert STATUSSYSTEM3.registeredShuffle.size() == 1;

    statusSystem.handleAppLost(APPID1);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.registeredShuffle.isEmpty();
    assert STATUSSYSTEM2.registeredShuffle.isEmpty();
    assert STATUSSYSTEM3.registeredShuffle.isEmpty();
  }

  @Test
  public void testHandleUnRegisterShuffle() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2, dummyRef);

    Map<WorkerInfo, Integer> workersToAllocate = new HashMap<>();
    workersToAllocate.put(workerInfo1, 5);
    workersToAllocate.put(workerInfo2, 5);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.registeredShuffle.size() == 1;
    assert STATUSSYSTEM2.registeredShuffle.size() == 1;
    assert STATUSSYSTEM3.registeredShuffle.size() == 1;

    statusSystem.handleUnRegisterShuffle(SHUFFLEKEY1);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.registeredShuffle.isEmpty();
    assert STATUSSYSTEM2.registeredShuffle.isEmpty();
    assert STATUSSYSTEM3.registeredShuffle.isEmpty();
  }

  @Test
  public void testHandleAppHeartbeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    long dummy = 1235L;
    statusSystem.handleAppHeartbeat(APPID1, dummy);
    Thread.sleep(3000L);
    assert STATUSSYSTEM1.appHeartbeatTime.get(APPID1) == dummy;
    assert STATUSSYSTEM2.appHeartbeatTime.get(APPID1) == dummy;
    assert STATUSSYSTEM3.appHeartbeatTime.get(APPID1) == dummy;

    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(appId2, dummy);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.appHeartbeatTime.get(appId2) == dummy;
    assert STATUSSYSTEM2.appHeartbeatTime.get(appId2) == dummy;
    assert STATUSSYSTEM3.appHeartbeatTime.get(appId2) == dummy;

    assert STATUSSYSTEM1.appHeartbeatTime.size() == 2;
    assert STATUSSYSTEM2.appHeartbeatTime.size() == 2;
    assert STATUSSYSTEM3.appHeartbeatTime.size() == 2;
  }

  @Test
  public void testHandleWorkerHeartBeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    statusSystem.handleWorkerHeartBeat(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        0, 1);
    Thread.sleep(3000L);

    assert STATUSSYSTEM1.blacklist.size() == 1;
    assert STATUSSYSTEM2.blacklist.size() == 1;
    assert STATUSSYSTEM3.blacklist.size() == 1;

    statusSystem.handleWorkerHeartBeat(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        0, 1);
    Thread.sleep(3000L);

    assert statusSystem.blacklist.size() == 2;
    assert STATUSSYSTEM1.blacklist.size() == 2;
    assert STATUSSYSTEM2.blacklist.size() == 2;
    assert STATUSSYSTEM3.blacklist.size() == 2;

    statusSystem.handleWorkerHeartBeat(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        1, 1);
    Thread.sleep(3000L);

    assert statusSystem.blacklist.size() == 1;
    assert STATUSSYSTEM1.blacklist.size() == 1;
    assert STATUSSYSTEM2.blacklist.size() == 1;
    assert STATUSSYSTEM3.blacklist.size() == 1;
  }

  @Before
  public void resetStatus() {
    STATUSSYSTEM1.registeredShuffle.clear();
    STATUSSYSTEM1.hostnameSet.clear();
    STATUSSYSTEM1.workers.clear();
    STATUSSYSTEM1.appHeartbeatTime.clear();
    STATUSSYSTEM1.blacklist.clear();
    STATUSSYSTEM1.workerLostEvents.clear();

    STATUSSYSTEM2.registeredShuffle.clear();
    STATUSSYSTEM2.hostnameSet.clear();
    STATUSSYSTEM2.workers.clear();
    STATUSSYSTEM2.appHeartbeatTime.clear();
    STATUSSYSTEM2.blacklist.clear();
    STATUSSYSTEM2.workerLostEvents.clear();

    STATUSSYSTEM3.registeredShuffle.clear();
    STATUSSYSTEM3.hostnameSet.clear();
    STATUSSYSTEM3.workers.clear();
    STATUSSYSTEM3.appHeartbeatTime.clear();
    STATUSSYSTEM3.blacklist.clear();
    STATUSSYSTEM3.workerLostEvents.clear();
  }

  @Test
  public void testHandleReportWorkerFailure() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    assert null != statusSystem;

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1);
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2);
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
        NUMSLOTS3);

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
        NUMSLOTS1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
        NUMSLOTS2, dummyRef);

    List<WorkerInfo> failedWorkers = new ArrayList<>();
    failedWorkers.add(workerInfo1);

    statusSystem.handleReportWorkerFailure(failedWorkers);
    Thread.sleep(3000L);
    assert 1 == STATUSSYSTEM1.blacklist.size();
    assert 1 == STATUSSYSTEM2.blacklist.size();
    assert 1 == STATUSSYSTEM3.blacklist.size();
  }
}
