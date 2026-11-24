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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.protocol.PbCompareAndSetScaleCheckpointRequest;
import org.apache.celeborn.common.protocol.PbMetaRequest;
import org.apache.celeborn.common.protocol.PbMetaRequestType;
import org.apache.celeborn.common.protocol.PbPendingScaleOperation;
import org.apache.celeborn.common.protocol.PbScaleCheckpoint;
import org.apache.celeborn.common.protocol.PbScaleOperation;
import org.apache.celeborn.common.protocol.PbScaleOperationRequest;
import org.apache.celeborn.common.protocol.PbScaleType;
import org.apache.celeborn.common.protocol.PbSnapshotMetaInfo;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.service.deploy.master.ApplicationMetricsSource;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HARaftServer;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.MetaHandler;

public class ScaleOperationSuiteJ {
  private CelebornConf conf;
  private CelebornRackResolver rackResolver;
  private ApplicationMetricsSource metricsSource;
  private AbstractMetaManager metaManager;

  @Before
  public void setUp() {
    conf = new CelebornConf();
    conf.set("celeborn.master.ha.heartbeat.batch.enabled", "false");
    rackResolver = new CelebornRackResolver(conf);
    metricsSource = new ApplicationMetricsSource(conf);
    metaManager =
        new SingleMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
  }

  @After
  public void tearDown() throws InterruptedException {
    try {
      metricsSource.destroy();
      assertTrue(metricsSource.metricsCleaner().awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      rackResolver.stop();
    }
  }

  @Test
  public void testSnapshotRestoresPendingAndConfirmedProgress() throws Exception {
    PbScaleCheckpoint checkpoint = checkpoint();
    assertTrue(metaManager.handleCompareAndSetScaleCheckpoint(-1L, 0L, checkpoint));
    PbScaleCheckpoint saved = metaManager.getScaleCheckpoint();
    Path snapshot = Files.createTempFile("scale-meta", ".pb");
    try {
      metaManager.writeMetaInfoToFile(snapshot.toFile());
      HAMasterMetaManager recovered =
          new HAMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
      recovered.restoreMetaFromFile(snapshot.toFile());
      assertEquals(saved, recovered.getScaleCheckpoint());
      PbScaleCheckpoint completed =
          saved.toBuilder().clearPending().setAccountedExclWorkerCnt(0).build();
      PbMetaRequest afterSnapshot =
          PbMetaRequest.newBuilder()
              .setMetaRequestType(PbMetaRequestType.CompareAndSetScaleCheckpoint)
              .setCompareAndSetScaleCheckpointRequest(
                  PbCompareAndSetScaleCheckpointRequest.newBuilder()
                      .setLeaderTerm(2L)
                      .setExpectedRevision(saved.getRevision())
                      .setCheckpoint(completed))
              .build();
      MetaHandler handler = new MetaHandler(recovered);
      assertTrue(handler.handleWriteRequest(afterSnapshot, 2L).getSuccess());
      assertFalse(handler.handleWriteRequest(afterSnapshot, 2L).getSuccess());
      assertEquals(completed.toBuilder().setRevision(2L).build(), recovered.getScaleCheckpoint());

      assertEquals("last", metaManager.lastScaleOperation().getLeft());
      assertEquals(operation(), metaManager.lastScaleOperation().getRight().toPb());
      PbScaleCheckpoint cleared = saved.toBuilder().clearLastOperation().build();
      assertFalse(metaManager.handleCompareAndSetScaleCheckpoint(-1L, 0L, cleared));
      assertTrue(metaManager.handleCompareAndSetScaleCheckpoint(-1L, saved.getRevision(), cleared));
      assertNull(metaManager.lastScaleOperation());
      assertEquals(saved.getPending(), metaManager.getScaleCheckpoint().getPending());
      assertEquals(2, metaManager.getScaleCheckpoint().getAccountedExclWorkerCnt());

      Files.write(snapshot, PbSnapshotMetaInfo.getDefaultInstance().toByteArray());
      metaManager.restoreMetaFromFile(snapshot.toFile());
      assertEquals(PbScaleCheckpoint.getDefaultInstance(), metaManager.getScaleCheckpoint());
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testCheckpointUsesBothWireModelsAndRejectsStaleWrites() throws Exception {
    assertEquals(32, PbMetaRequestType.CompareAndSetScaleCheckpoint.getNumber());
    assertEquals(28, PbMetaRequest.COMPAREANDSETSCALECHECKPOINTREQUEST_FIELD_NUMBER);
    HAMasterMetaManager replica =
        new HAMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
    HARaftServer server = mock(HARaftServer.class);
    replica.setRatisServer(server);
    MetaHandler handler = new MetaHandler(replica);
    when(server.submitMetaRequest(any(PbMetaRequest.class)))
        .thenAnswer(
            invocation -> {
              PbMetaRequest sent = invocation.getArgument(0);
              ResourceProtos.ResourceRequest resource =
                  ResourceProtos.ResourceRequest.parseFrom(sent.toByteArray());
              assertEquals(ResourceProtos.Type.CompareAndSetScaleCheckpoint, resource.getCmdType());
              ResourceProtos.ScaleCheckpoint checkpoint =
                  resource.getCompareAndSetScaleCheckpointRequest().getCheckpoint();
              assertTrue(checkpoint.getPending().hasAccountedExclWorkerCnt());
              assertEquals(0, checkpoint.getPending().getAccountedExclWorkerCnt());
              assertTrue(checkpoint.getPending().getRemovesWorkerIndices());
              assertEquals(sent, PbMetaRequest.parseFrom(resource.toByteArray()));
              return ResourceProtos.ResourceResponse.parseFrom(
                  handler.handleWriteRequest(sent, 2L).toByteArray());
            });
    PbScaleCheckpoint initial = checkpoint();
    assertTrue(replica.handleCompareAndSetScaleCheckpoint(2L, 0L, initial));
    PbScaleCheckpoint saved = replica.getScaleCheckpoint();
    assertEquals(initial.toBuilder().setRevision(1L).build(), saved);
    assertFalse(replica.handleCompareAndSetScaleCheckpoint(2L, 0L, initial));
    assertFalse(replica.handleCompareAndSetScaleCheckpoint(1L, 1L, initial));
    assertEquals(saved, replica.getScaleCheckpoint());
  }

  @Test
  public void testCheckpointControlsWorkerAdmissionAcrossSnapshotAndElection() throws Exception {
    WorkerInfo zero = registerWorker("worker-a", 0);
    WorkerInfo reserved = registerWorker("worker-b", 1);
    WorkerInfo two = registerWorker("worker-c", 2);
    WorkerInfo unmanaged = registerWorker("worker-d", -1);
    PbScaleOperation target =
        PbScaleOperation.newBuilder()
            .setExpectedWorkerReplicaNumber(2)
            .addReserveWorkerNumbers(1)
            .build();
    PbPendingScaleOperation pending =
        PbPendingScaleOperation.newBuilder()
            .setOperation(target)
            .setRemovesWorkerIndices(true)
            .build();
    PbScaleCheckpoint shrink = PbScaleCheckpoint.newBuilder().setPending(pending).build();
    assertTrue(metaManager.compareAndSetScaleCheckpoint(0, shrink));
    assertFalse(metaManager.availableWorkers.contains(reserved));
    assertTrue(metaManager.availableWorkers.containsAll(Arrays.asList(zero, two, unmanaged)));
    metaManager.invalidateWorkerStats();
    assertFalse(metaManager.availableWorkers.contains(reserved));

    Path snapshot = Files.createTempFile("scale-admission", ".pb");
    try {
      metaManager.writeMetaInfoToFile(snapshot.toFile());
      HAMasterMetaManager restored =
          new HAMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
      restored.restoreMetaFromFile(snapshot.toFile());
      assertEquals(0, restored.workersMap.get(zero.toUniqueId()).workerIndex());
      assertEquals(1, restored.workersMap.get(reserved.toUniqueId()).workerIndex());
      assertEquals(-1, restored.workersMap.get(unmanaged.toUniqueId()).workerIndex());
      assertEquals(metaManager.availableWorkers, restored.availableWorkers);
      assertTrue(restored.compareAndSetScaleCheckpoint(1, PbScaleCheckpoint.getDefaultInstance()));
      assertTrue(restored.availableWorkers.contains(reserved));

      PbSnapshotMetaInfo.Builder legacy =
          PbSnapshotMetaInfo.parseFrom(Files.readAllBytes(snapshot)).toBuilder();
      legacy.getWorkersBuilderList().forEach(worker -> worker.clearWorkerIndex());
      Files.write(snapshot, legacy.build().toByteArray());
      restored.restoreMetaFromFile(snapshot.toFile());
      assertEquals(-1, restored.workersMap.get(zero.toUniqueId()).workerIndex());
      assertEquals(-1, restored.workersMap.get(reserved.toUniqueId()).workerIndex());
      assertTrue(
          restored.availableWorkers.containsAll(Arrays.asList(zero, reserved, two, unmanaged)));
    } finally {
      Files.deleteIfExists(snapshot);
    }
  }

  @Test
  public void testEmptyScaleTargetRestrictsOnlyManagedWorkers() {
    WorkerInfo managed = registerWorker("new-pod-ip", 0);
    WorkerInfo unmanaged = registerWorker("workers-0", -1);
    PbPendingScaleOperation.Builder pending =
        PbPendingScaleOperation.newBuilder()
            .setOperation(PbScaleOperation.newBuilder().setExpectedWorkerReplicaNumber(0));
    assertTrue(
        metaManager.compareAndSetScaleCheckpoint(
            0,
            PbScaleCheckpoint.newBuilder()
                .setPending(pending.setRemovesWorkerIndices(true))
                .build()));
    assertEquals(Collections.singleton(unmanaged), metaManager.availableWorkers);
    assertFalse(
        metaManager.compareAndSetScaleCheckpoint(0, PbScaleCheckpoint.getDefaultInstance()));
    assertEquals(Collections.singleton(unmanaged), metaManager.availableWorkers);
    assertTrue(
        metaManager.compareAndSetScaleCheckpoint(
            1,
            PbScaleCheckpoint.newBuilder()
                .setPending(pending.setRemovesWorkerIndices(false))
                .build()));
    assertTrue(metaManager.availableWorkers.containsAll(Arrays.asList(managed, unmanaged)));
  }

  private WorkerInfo registerWorker(String host, int index) {
    metaManager.updateRegisterWorkerMeta(
        host, 1, 2, 3, 4, 5, "/rack", Collections.emptyMap(), index);
    return metaManager.workersMap.get(new WorkerInfo(host, 1, 2, 3, 4).toUniqueId());
  }

  private PbScaleCheckpoint checkpoint() {
    return PbScaleCheckpoint.newBuilder()
        .setWorkloadUid("workload")
        .setAccountedExclWorkerCnt(2)
        .setLastOperation(
            PbScaleOperationRequest.newBuilder()
                .setOperationId("last")
                .setScaleOperation(operation()))
        .setPending(
            PbPendingScaleOperation.newBuilder()
                .setOperation(operation().toBuilder().setIsFinished(false))
                .setAccountedExclWorkerCnt(0)
                .setRemovesWorkerIndices(true)
                .setPlatformVersion("version"))
        .build();
  }

  private PbScaleOperation operation() {
    return PbScaleOperation.newBuilder()
        .setScaleType(PbScaleType.SCALE_DOWN)
        .setExpectedWorkerReplicaNumber(8)
        .addAllReserveWorkerNumbers(Arrays.asList(3, 7))
        .setLastScaleTime(1234L)
        .setIsFinished(true)
        .build();
  }
}
