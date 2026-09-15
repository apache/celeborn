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

package org.apache.celeborn.common.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class PartitionLocationSuiteJ {

  private final int partitionId = 0;
  private final int epoch = 0;
  private final String host = "localhost";
  private final int rpcPort = 3;
  private final int pushPort = 1;
  private final int fetchPort = 2;
  private final int replicatePort = 4;
  private final PartitionLocation.Mode mode = PartitionLocation.Mode.PRIMARY;
  private final PartitionLocation peer =
      new PartitionLocation(
          partitionId,
          epoch,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          PartitionLocation.Mode.REPLICA);

  @Test
  public void testGetCorrectMode() {
    byte primaryMode = 0;
    byte replicaMode = 1;

    assertEquals(PartitionLocation.Mode.PRIMARY, PartitionLocation.getMode(primaryMode));
    assertEquals(PartitionLocation.Mode.REPLICA, PartitionLocation.getMode(replicaMode));

    for (int i = 2; i < 255; ++i) {
      byte otherMode = (byte) i;
      // Should we return replica mode when the parameter passed in is neither 0 nor 1?
      assertEquals(PartitionLocation.Mode.REPLICA, PartitionLocation.getMode(otherMode));
    }
  }

  @Test
  public void testPartitionIdNotEqualMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId + 1, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, false);
  }

  @Test
  public void testEpochNotEqualMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch + 1, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, false);
  }

  @Test
  public void testHostNotEqualMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId,
            epoch,
            "remoteHost",
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            mode,
            peer);
    checkEqual(location1, location2, false);
  }

  @Test
  public void testPushPortNotEqualMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort + 1, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, false);
  }

  @Test
  public void testFetchPortNotEqualMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort + 1, replicatePort, mode, peer);
    checkEqual(location1, location2, false);
  }

  @Test
  public void testModeNotEqualNeverMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId,
            epoch,
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            PartitionLocation.Mode.REPLICA,
            peer);
    PartitionLocation location3 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, true);
    checkEqual(location1, location3, true);
    checkEqual(location2, location3, true);
  }

  @Test
  public void testPeerNotEqualNeverMakePartitionLocationDifferent() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, location1);
    PartitionLocation location3 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, true);
    checkEqual(location1, location3, true);
    checkEqual(location2, location3, true);
  }

  @Test
  public void testAllFieldEqualMakePartitionLocationEqual() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    checkEqual(location1, location2, true);
  }

  @Test
  public void testSetPeerMaintainsPeerReference() {
    PartitionLocation primary =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);
    PartitionLocation replica =
        new PartitionLocation(
            partitionId,
            epoch,
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            PartitionLocation.Mode.REPLICA);

    primary.setPeer(replica);

    assertEquals(true, primary.hasPeer());
    assertSame(replica, primary.getPeer());

    primary.setPeer(null);
    assertEquals(false, primary.hasPeer());
  }

  @Test
  public void testCopyPartitionLocationKeepsSharedFields() {
    PartitionLocation primary =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);
    PartitionLocation replica =
        new PartitionLocation(
            partitionId,
            epoch,
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            PartitionLocation.Mode.REPLICA);
    primary.setPeer(replica);
    StorageInfo storageInfo = primary.getStorageInfo();
    RoaringBitmap bitmap = primary.getMapIdBitMapOrCreate();

    PartitionLocation copy = new PartitionLocation(primary);

    assertSame(replica, copy.getPeer());
    assertSame(storageInfo, copy.getStorageInfo());
    assertSame(bitmap, copy.getMapIdBitMap());
  }

  @Test
  public void testLazyDefaultStorageInfoIsNotShared() {
    PartitionLocation first =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);
    PartitionLocation second =
        new PartitionLocation(
            partitionId + 1, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);

    StorageInfo storageInfo = first.getStorageInfo();
    storageInfo.availableStorageTypes = StorageInfo.HDFS_MASK;
    storageInfo.setMountPoint("/mnt/disk1");

    assertSame(storageInfo, first.getStorageInfoOrCreate());
    assertNotSame(first.getStorageInfo(), second.getStorageInfo());
    assertEquals(StorageInfo.HDFS_MASK, first.getStorageInfo().availableStorageTypes);
    assertEquals("/mnt/disk1", first.getStorageInfo().getMountPoint());
    assertEquals(
        StorageInfo.ALL_TYPES_AVAILABLE_MASK, second.getStorageInfo().availableStorageTypes);
    assertEquals("", second.getStorageInfo().getMountPoint());
  }

  @Test
  public void testLazyMapIdBitmapKeepsCompatibleGetter() {
    PartitionLocation location =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);

    assertNull(location.getMapIdBitMapIfPresent());
    RoaringBitmap bitmap = location.getMapIdBitMapOrCreate();
    bitmap.add(1);

    assertSame(bitmap, location.getMapIdBitMap());
    assertEquals(1, location.getMapIdBitMap().getCardinality());
  }

  @Test
  public void testWorkerEndpointIsInterned() {
    WorkerEndpoint first = WorkerEndpoint.apply(host, rpcPort, pushPort, fetchPort, replicatePort);
    WorkerEndpoint second = WorkerEndpoint.apply(host, rpcPort, pushPort, fetchPort, replicatePort);
    WorkerEndpoint different =
        WorkerEndpoint.apply(host, rpcPort, pushPort + 1, fetchPort, replicatePort);

    assertSame(first, second);
    assertNotSame(first, different);
  }

  @Test
  public void testSameVersionJavaSerializationReinternsWorkerEndpoint() throws Exception {
    WorkerEndpoint endpoint =
        WorkerEndpoint.apply(host, rpcPort, pushPort, fetchPort, replicatePort);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(endpoint);
    }

    WorkerEndpoint deserialized;
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      deserialized = (WorkerEndpoint) input.readObject();
    }

    assertSame(endpoint, deserialized);
    assertEquals("localhost:1", deserialized.hostAndPushPort());
    assertEquals("localhost:2", deserialized.hostAndFetchPort());
  }

  @Test
  public void testHostPortCachesAreInvalidatedBySetters() {
    PartitionLocation location =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);

    assertEquals("localhost:1", location.hostAndPushPort());
    assertEquals("localhost:2", location.hostAndFetchPort());

    location.setHost("remoteHost");
    location.setRpcPort(13);
    location.setPushPort(11);
    location.setFetchPort(12);
    location.setReplicatePort(14);

    assertEquals("remoteHost:11", location.hostAndPushPort());
    assertEquals("remoteHost:12", location.hostAndFetchPort());
    assertEquals(13, location.getRpcPort());
    assertEquals(14, location.getReplicatePort());
  }

  @Test
  public void testWorkerEndpointKeepsWorkerInfoCompatible() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId + 1, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);

    assertEquals(location1.getWorker(), location2.getWorker());
    assertNotNull(location1.hostAndPorts());
  }

  @Test
  public void testToStringOutput() {
    PartitionLocation location1 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode);
    PartitionLocation location2 =
        new PartitionLocation(
            partitionId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer);
    StorageInfo storageInfo =
        new StorageInfo(
            "/mnt/disk/0", StorageInfo.Type.MEMORY, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1);
    bitmap.add(2);
    bitmap.add(3);

    int partitionId = 1000;
    PartitionLocation location3 =
        new PartitionLocation(
            partitionId,
            epoch,
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            mode,
            peer,
            storageInfo,
            bitmap);

    String exp1 =
        "PartitionLocation[\n"
            + "  id-epoch:0-0\n"
            + "  host-rpcPort-pushPort-fetchPort-replicatePort:localhost-3-1-2-4\n"
            + "  mode:PRIMARY\n"
            + "  peer:(empty)\n"
            + "  storage hint:null\n"
            + "  mapIdBitMap:{}]";
    String exp2 =
        "PartitionLocation[\n"
            + "  id-epoch:0-0\n"
            + "  host-rpcPort-pushPort-fetchPort-replicatePort:localhost-3-1-2-4\n"
            + "  mode:PRIMARY\n"
            + "  peer:(host-rpcPort-pushPort-fetchPort-replicatePort:localhost-3-1-2-4)\n"
            + "  storage hint:null\n"
            + "  mapIdBitMap:{}]";
    String exp3 =
        "PartitionLocation[\n"
            + "  id-epoch:1000-0\n"
            + "  host-rpcPort-pushPort-fetchPort-replicatePort:localhost-3-1-2-4\n"
            + "  mode:PRIMARY\n"
            + "  peer:(host-rpcPort-pushPort-fetchPort-replicatePort:localhost-3-1-2-4)\n"
            + "  storage hint:StorageInfo{type=MEMORY, mountPoint='/mnt/disk/0', "
            + "finalResult=false, filePath=null, fileSize=0, chunkOffsets=null}\n"
            + "  mapIdBitMap:{1,2,3}]";
    assertEquals(exp1, location1.toString());
    assertEquals(exp2, location2.toString());
    assertEquals(exp3, location3.toString());
  }

  private void checkEqual(
      PartitionLocation location1, PartitionLocation location2, boolean shouldEqual) {
    String errorMessage =
        "Need location1 "
            + location1
            + " and location2 "
            + location2
            + " are "
            + (shouldEqual ? "" : "not ")
            + "equal, but "
            + (shouldEqual ? "not " : "")
            + "equal.";
    assertEquals(errorMessage, shouldEqual, location1.equals(location2));
  }
}
