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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.master.ApplicationMetricsSource;

public class RegisteredShuffleSetSuiteJ {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private CelebornRackResolver rackResolver;
  private ApplicationMetricsSource metricsSource;
  private AbstractMetaManager metaManager;

  @Before
  public void setUp() {
    CelebornConf conf = new CelebornConf();
    rackResolver = new CelebornRackResolver(conf);
    metricsSource = new ApplicationMetricsSource(conf);
    metaManager =
        new SingleMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
  }

  @After
  public void tearDown() throws InterruptedException {
    try {
      if (metricsSource != null) {
        metricsSource.destroy();
        assertTrue(metricsSource.metricsCleaner().awaitTermination(5, TimeUnit.SECONDS));
      }
    } finally {
      if (rackResolver != null) {
        rackResolver.stop();
      }
    }
  }

  @Test
  public void testRequestSlotsCreatesConcurrentShuffleSet() {
    registerShuffle("app-a", 1);
    Set<Integer> shuffles = metaManager.registeredAppAndShuffles.get("app-a");
    registerShuffle("app-a", 2);
    registerShuffle("app-a", 1);
    registerShuffle("app-b", 3);

    assertSame(shuffles, metaManager.registeredAppAndShuffles.get("app-a"));
    assertEquals(new HashSet<>(Arrays.asList(1, 2)), shuffles);
    assertEquals(Collections.singleton(3), metaManager.registeredAppAndShuffles.get("app-b"));
    assertConcurrentShuffleSet("app-a");
    assertConcurrentShuffleSet("app-b");
  }

  @Test
  public void testReviseLostShufflesCreatesConcurrentShuffleSet() {
    metaManager.reviseLostShuffles("app-a", Arrays.asList(1, 2, 2));
    Set<Integer> shuffles = metaManager.registeredAppAndShuffles.get("app-a");
    metaManager.reviseLostShuffles("app-a", Arrays.asList(2, 3));
    metaManager.reviseLostShuffles("app-b", Collections.singletonList(4));

    assertSame(shuffles, metaManager.registeredAppAndShuffles.get("app-a"));
    assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), shuffles);
    assertEquals(Collections.singleton(4), metaManager.registeredAppAndShuffles.get("app-b"));
    assertConcurrentShuffleSet("app-a");
    assertConcurrentShuffleSet("app-b");
  }

  @Test
  public void testSnapshotRestoresConcurrentShuffleSets() throws Exception {
    registerShuffle("app-a", 1);
    metaManager.reviseLostShuffles("app-b", Arrays.asList(2, 3));
    File snapshot = temporaryFolder.newFile("metadata.snapshot");
    metaManager.writeMetaInfoToFile(snapshot);
    metaManager.restoreMetaFromFile(snapshot);

    assertEquals(Collections.singleton(1), metaManager.registeredAppAndShuffles.get("app-a"));
    assertEquals(
        new HashSet<>(Arrays.asList(2, 3)), metaManager.registeredAppAndShuffles.get("app-b"));
    assertConcurrentShuffleSet("app-a");
    assertConcurrentShuffleSet("app-b");
  }

  @Test
  public void testSnapshotPreservesRegisteredShufflesAndAcceptsFurtherUpdates() throws Exception {
    registerShuffle("app-a", 1);
    registerShuffle("app-a", 2);
    metaManager.reviseLostShuffles("app-b", Arrays.asList(3, 4));
    Map<String, Set<Integer>> expected = new HashMap<>();
    expected.put("app-a", new HashSet<>(Arrays.asList(1, 2)));
    expected.put("app-b", new HashSet<>(Arrays.asList(3, 4)));
    File snapshot = temporaryFolder.newFile("metadata.snapshot");
    metaManager.writeMetaInfoToFile(snapshot);

    registerShuffle("app-a", 99);
    metaManager.deleteApp("app-b");
    registerShuffle("app-c", 100);
    metaManager.restoreMetaFromFile(snapshot);
    assertEquals(expected, metaManager.registeredAppAndShuffles);

    registerShuffle("app-a", 5);
    metaManager.reviseLostShuffles("app-a", Arrays.asList(2, 6));
    registerShuffle("app-b", 7);
    expected.get("app-a").addAll(Arrays.asList(5, 6));
    expected.get("app-b").add(7);
    assertEquals(expected, metaManager.registeredAppAndShuffles);
  }

  private void registerShuffle(String appId, int shuffleId) {
    metaManager.updateRequestSlotsMeta(
        Utils.makeShuffleKey(appId, shuffleId), "localhost", Collections.emptyMap());
  }

  private void assertConcurrentShuffleSet(String appId) {
    Set<Integer> shuffles = metaManager.registeredAppAndShuffles.get(appId);
    assertNotNull(shuffles);
    // Check the concurrent-set invariant deterministically instead of relying on a resize race.
    assertTrue(
        "Registered shuffles must use a concurrent key set: " + appId,
        shuffles instanceof ConcurrentHashMap.KeySetView);
  }
}
