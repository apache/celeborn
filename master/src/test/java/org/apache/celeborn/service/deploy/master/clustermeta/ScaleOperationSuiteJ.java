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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.protocol.PbMetaRequest;
import org.apache.celeborn.common.protocol.PbMetaRequestType;
import org.apache.celeborn.common.protocol.PbScaleOperation;
import org.apache.celeborn.common.protocol.PbScaleType;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.service.deploy.master.ApplicationMetricsSource;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HARaftServer;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.MetaHandler;
import org.apache.celeborn.service.deploy.master.scale.ScaleOperation;
import org.apache.celeborn.service.deploy.master.scale.ScaleOperation$;

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
  public void testSingleMasterUpdatesAndRemovesMatchingOperation() {
    assertNull(metaManager.lastScaleOperation);
    ScaleOperation operation = operation();
    metaManager.handleUpdateScaleOperation("operation-1", operation);
    assertEquals(operation, metaManager.lastScaleOperation.getRight());
    metaManager.handleRemoveScaleOperation("operation-2");
    assertEquals("operation-1", metaManager.lastScaleOperation.getLeft());
    metaManager.handleRemoveScaleOperation("operation-1");
    assertNull(metaManager.lastScaleOperation);
  }

  @Test
  public void testScaleOperationIsAppliedThroughBothMetadataModels() throws Exception {
    HAMasterMetaManager replica =
        new HAMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
    HARaftServer ratisServer = mock(HARaftServer.class);
    replica.setRatisServer(ratisServer);
    ScaleOperation operation = operation();

    replica.handleUpdateScaleOperation("scale-operation", operation);

    assertNull(replica.lastScaleOperation);
    ArgumentCaptor<ResourceProtos.ResourceRequest> requests =
        ArgumentCaptor.forClass(ResourceProtos.ResourceRequest.class);
    verify(ratisServer).submitRequest(requests.capture());
    ResourceProtos.ResourceRequest resourceRequest = requests.getValue();
    PbMetaRequest request = PbMetaRequest.parseFrom(resourceRequest.toByteArray());
    assertEquals(PbMetaRequestType.DoScaleOperation, request.getMetaRequestType());
    assertEquals("scale-operation", request.getScaleOperationRequest().getOperationId());
    assertEquals(resourceRequest, ResourceProtos.ResourceRequest.parseFrom(request.toByteArray()));
    assertTrue(new MetaHandler(replica).handleWriteRequest(request).getSuccess());
    assertEquals("scale-operation", replica.lastScaleOperation.getLeft());
    assertEquals(operation, replica.lastScaleOperation.getRight());
    assertEquals(PbScaleType.SCALE_DOWN, replica.lastScaleOperation.getRight().scaleType());
    assertEquals(1234L, replica.lastScaleOperation.getRight().lastScaleTime());
    assertTrue(replica.lastScaleOperation.getRight().isFinished());
  }

  @Test
  public void testRemoveOperationIsAppliedThroughBothMetadataModels() throws Exception {
    HAMasterMetaManager replica =
        new HAMasterMetaManager(mock(RpcEnv.class), conf, rackResolver, metricsSource);
    HARaftServer ratisServer = mock(HARaftServer.class);
    replica.setRatisServer(ratisServer);
    replica.updateScaleOperation("scale-operation", operation());

    replica.handleRemoveScaleOperation("scale-operation");

    ArgumentCaptor<ResourceProtos.ResourceRequest> requests =
        ArgumentCaptor.forClass(ResourceProtos.ResourceRequest.class);
    verify(ratisServer).submitRequest(requests.capture());
    ResourceProtos.ResourceRequest resourceRequest = requests.getValue();
    PbMetaRequest request = PbMetaRequest.parseFrom(resourceRequest.toByteArray());
    assertEquals(PbMetaRequestType.RemoveScaleOperation, request.getMetaRequestType());
    assertEquals(resourceRequest, ResourceProtos.ResourceRequest.parseFrom(request.toByteArray()));
    assertTrue(new MetaHandler(replica).handleWriteRequest(request).getSuccess());
    assertNull(replica.lastScaleOperation);
  }

  private ScaleOperation operation() {
    return ScaleOperation$.MODULE$.fromPb(
        PbScaleOperation.newBuilder()
            .setScaleType(PbScaleType.SCALE_DOWN)
            .setExpectedWorkerReplicaNumber(8)
            .addAllReserveWorkerNumbers(Arrays.asList(3, 7))
            .setLastScaleTime(1234L)
            .setIsFinished(true)
            .build());
  }
}
