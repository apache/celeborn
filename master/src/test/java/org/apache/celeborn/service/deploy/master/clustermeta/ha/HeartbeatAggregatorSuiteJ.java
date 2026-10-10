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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;

public class HeartbeatAggregatorSuiteJ {
  private HARaftServer ratisServer;
  private HAMasterMetaManager metaSystem;

  @Before
  public void init() throws Exception {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.MASTER_HA_HEARTBEAT_BATCH_ENABLED().key(), "true");
    // Use a 1s window (not shorter): on oversubscribed CI runners the handler calls below can
    // be scheduled far apart, and a short window would split the offers into several batches.
    conf.set(CelebornConf.MASTER_HA_HEARTBEAT_BATCH_INTERVAL().key(), "1s");
    metaSystem = new HAMasterMetaManager(null, conf);
    MetaHandler handler = new MetaHandler(metaSystem);
    File tmpDir = File.createTempFile("celeborn-ratis-tmp", "for-test-only");
    tmpDir.delete();
    tmpDir.mkdirs();
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), tmpDir.getAbsolutePath());
    String id = UUID.randomUUID().toString();
    MasterNode masterNode =
        new MasterNode.Builder().setNodeId(id).setHost("localhost").setRatisPort(9998).build();
    ratisServer =
        HARaftServer.newMasterRatisServer(handler, conf, masterNode, Collections.emptyList());
    metaSystem.setRatisServer(ratisServer);
    ratisServer.start();
    waitForLeader();
  }

  private void waitForLeader() throws InterruptedException {
    // Wait for isLeaderReady(), not just isLeader(): a newly elected leader rejects writes
    // with LeaderNotReadyException until its no-op entry for the new term is committed.
    for (int i = 0; i < 100; i++) {
      if (ratisServer.isLeader()) {
        try {
          if (ratisServer
              .getServer()
              .getDivision(ratisServer.getGroupId())
              .getInfo()
              .isLeaderReady()) {
            return;
          }
        } catch (IOException e) {
          // Division not available yet; keep polling.
        }
      }
      Thread.sleep(200);
    }
    Assert.fail("Raft server did not become ready leader in time.");
  }

  @After
  public void shutdown() {
    if (metaSystem.getHeartbeatAggregator() != null) {
      metaSystem.getHeartbeatAggregator().stop();
    }
    if (ratisServer != null) {
      ratisServer.stop();
    }
  }

  @Test
  public void testBatchHeartbeatAggregation() throws Exception {
    Assert.assertNotNull(metaSystem.getHeartbeatAggregator());

    metaSystem.handleRegisterWorker(
        "host1",
        1,
        2,
        3,
        4,
        5,
        "networkLocation1",
        new HashMap<>(),
        new HashMap<>(),
        UUID.randomUUID().toString() + "#1");
    Thread.sleep(2000);
    Assert.assertEquals(1, metaSystem.workersMap.size());
    long appliedIndexAfterRegister = lastAppliedIndex();

    // 4 heartbeat offers: 3 for the same worker (collapsed to the newest), 1 for an app.
    long time1 = System.currentTimeMillis();
    long time2 = time1 + 10;
    metaSystem.handleWorkerHeartbeat(
        "host1",
        1,
        2,
        3,
        4,
        new HashMap<>(),
        new HashMap<>(),
        time1,
        false,
        WorkerStatus.normalWorkerStatus(),
        UUID.randomUUID().toString() + "#2");
    metaSystem.handleWorkerHeartbeat(
        "host1",
        1,
        2,
        3,
        4,
        new HashMap<>(),
        new HashMap<>(),
        time1,
        false,
        WorkerStatus.normalWorkerStatus(),
        UUID.randomUUID().toString() + "#3");
    metaSystem.handleAppHeartbeat(
        "app-1",
        100,
        10,
        1,
        1,
        new HashMap<>(),
        new HashMap<>(),
        time2,
        UUID.randomUUID().toString() + "#4");
    metaSystem.handleWorkerHeartbeat(
        "host1",
        1,
        2,
        3,
        4,
        new HashMap<>(),
        new HashMap<>(),
        time2,
        false,
        WorkerStatus.normalWorkerStatus(),
        UUID.randomUUID().toString() + "#5");

    // Wait comfortably past one 1s flush window plus the blocking submit/apply.
    Thread.sleep(4000);

    // The 4 offers must have been flushed as (far) fewer raft log entries.
    long newEntries = lastAppliedIndex() - appliedIndexAfterRegister;
    Assert.assertTrue(
        "Expected heartbeats to be merged into few raft log entries, but got " + newEntries,
        newEntries >= 1 && newEntries <= 2);

    // The newest heartbeat wins.
    WorkerInfo workerInfo = metaSystem.workersMap.values().iterator().next();
    Assert.assertEquals("host1", workerInfo.host());
    Assert.assertEquals(time2, workerInfo.lastHeartbeat());
    Assert.assertEquals(Long.valueOf(time2), metaSystem.appHeartbeatTime.get("app-1"));
  }

  @Test
  public void testEmptyWindowProducesNoRaftLog() throws Exception {
    Thread.sleep(2000);
    long appliedIndex = lastAppliedIndex();
    Thread.sleep(500);
    Assert.assertEquals(appliedIndex, lastAppliedIndex());
  }

  @Test
  public void testDuplicateAppHeartbeatKeepsNewest() throws Exception {
    // Duplicate heartbeats of the same app within one window are collapsed to the newest one.
    java.util.Map<String, Long> fallback1 = new HashMap<>();
    fallback1.put("shuffle-1", 1L);
    java.util.Map<String, Long> fallback2 = new HashMap<>();
    fallback2.put("shuffle-1", 2L);
    long time1 = System.currentTimeMillis();
    long time2 = time1 + 10;
    metaSystem.handleAppHeartbeat(
        "app-merge",
        100,
        10,
        1,
        1,
        fallback1,
        new HashMap<>(),
        time1,
        UUID.randomUUID().toString() + "#1");
    metaSystem.handleAppHeartbeat(
        "app-merge",
        200,
        20,
        2,
        3,
        fallback2,
        new HashMap<>(),
        time2,
        UUID.randomUUID().toString() + "#2");

    Thread.sleep(4000);

    Assert.assertEquals(200, metaSystem.partitionTotalWritten.sum());
    Assert.assertEquals(20, metaSystem.partitionTotalFileCount.sum());
    Assert.assertEquals(2, metaSystem.shuffleTotalCount.sum());
    Assert.assertEquals(3, metaSystem.applicationTotalCount.sum());
    Assert.assertEquals(Long.valueOf(2), metaSystem.shuffleFallbackCounts.get("shuffle-1"));
    Assert.assertEquals(Long.valueOf(time2), metaSystem.appHeartbeatTime.get("app-merge"));
  }

  private long lastAppliedIndex() {
    return ratisServer.getMasterStateMachine().getLastAppliedTermIndex().getIndex();
  }
}
