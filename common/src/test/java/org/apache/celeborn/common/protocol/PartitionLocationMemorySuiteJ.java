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

import java.lang.reflect.Field;

import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;
import org.roaringbitmap.RoaringBitmap;

@Ignore("Manual JOL benchmark; run main to print retained-size comparisons.")
public class PartitionLocationMemorySuiteJ {

  private static final int ENDPOINT_COUNT = 2000;
  private static final int DEFAULT_PAIR_COUNT = 10_000;
  private static final String ALLOCATOR_SCENARIO = "allocator";
  private static final String PACKED_DESERIALIZED_PAIR_SCENARIO = "packed-deserialized-pair";

  public static void main(String[] args) throws Exception {
    String scenario = args.length > 0 ? args[0] : ALLOCATOR_SCENARIO;
    int pairCount = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PAIR_COUNT;
    if (pairCount <= 0) {
      throw new IllegalArgumentException("pairCount must be positive: " + pairCount);
    }
    if (!ALLOCATOR_SCENARIO.equals(scenario)
        && !PACKED_DESERIALIZED_PAIR_SCENARIO.equals(scenario)) {
      throw new IllegalArgumentException(
          "Unknown scenario: " + scenario + ". Expected allocator or packed-deserialized-pair.");
    }
    new PartitionLocationMemorySuiteJ().printPeerPairFootprint(scenario, pairCount, ENDPOINT_COUNT);
  }

  @Test
  public void printPartitionLocationFootprint() throws Exception {
    printPeerPairFootprint(ALLOCATOR_SCENARIO, DEFAULT_PAIR_COUNT, ENDPOINT_COUNT);
  }

  private void printPeerPairFootprint(String scenario, int pairCount, int endpointCount)
      throws Exception {
    String[] endpointHosts = endpointHosts(endpointCount);
    boolean allocatorShaped = ALLOCATOR_SCENARIO.equals(scenario);
    compare(
        scenario + "-shaped, " + pairCount + " peer pairs across " + endpointCount + " endpoints",
        newOldLocationPairArray(pairCount, endpointHosts, allocatorShaped),
        newLocationPairArray(pairCount, endpointHosts, allocatorShaped));
  }

  private String[] endpointHosts(int endpointCount) {
    String[] hosts = new String[endpointCount];
    for (int i = 0; i < endpointCount; i++) {
      hosts[i] = "localhost-" + i;
    }
    return hosts;
  }

  private PartitionLocation[] newLocationPairArray(
      int size, String[] endpointHosts, boolean allocatorShaped) {
    PartitionLocation[] locations = new PartitionLocation[size * 2];
    for (int i = 0; i < size; i++) {
      int primaryEndpointIndex = i % endpointHosts.length;
      int replicaEndpointIndex = (primaryEndpointIndex + 1) % endpointHosts.length;
      PartitionLocation primary =
          newLocation(
              i,
              primaryEndpointIndex,
              endpointHosts,
              allocatorShaped,
              PartitionLocation.Mode.PRIMARY);
      PartitionLocation replica =
          newLocation(
              i,
              replicaEndpointIndex,
              endpointHosts,
              allocatorShaped,
              PartitionLocation.Mode.REPLICA);
      primary.setPeer(replica);
      replica.setPeer(primary);
      locations[i * 2] = primary;
      locations[i * 2 + 1] = replica;
    }
    return locations;
  }

  private PartitionLocation newLocation(
      int id,
      int endpointIndex,
      String[] endpointHosts,
      boolean allocatorShaped,
      PartitionLocation.Mode mode) {
    return new PartitionLocation(
        id,
        0,
        locationHost(endpointHosts[endpointIndex], allocatorShaped),
        1001 + endpointIndex,
        1002 + endpointIndex,
        1003 + endpointIndex,
        1004 + endpointIndex,
        mode,
        null,
        newStorageInfo(),
        null);
  }

  private PartitionLocationOld[] newOldLocationPairArray(
      int size, String[] endpointHosts, boolean allocatorShaped) {
    PartitionLocationOld[] locations = new PartitionLocationOld[size * 2];
    for (int i = 0; i < size; i++) {
      int primaryEndpointIndex = i % endpointHosts.length;
      int replicaEndpointIndex = (primaryEndpointIndex + 1) % endpointHosts.length;
      PartitionLocationOld primary =
          newOldLocation(
              i,
              primaryEndpointIndex,
              endpointHosts,
              allocatorShaped,
              PartitionLocation.Mode.PRIMARY);
      PartitionLocationOld replica =
          newOldLocation(
              i,
              replicaEndpointIndex,
              endpointHosts,
              allocatorShaped,
              PartitionLocation.Mode.REPLICA);
      primary.setPeer(replica);
      replica.setPeer(primary);
      locations[i * 2] = primary;
      locations[i * 2 + 1] = replica;
    }
    return locations;
  }

  private PartitionLocationOld newOldLocation(
      int id,
      int endpointIndex,
      String[] endpointHosts,
      boolean allocatorShaped,
      PartitionLocation.Mode mode) {
    return new PartitionLocationOld(
        id,
        0,
        locationHost(endpointHosts[endpointIndex], allocatorShaped),
        1001 + endpointIndex,
        1002 + endpointIndex,
        1003 + endpointIndex,
        1004 + endpointIndex,
        mode,
        null,
        newStorageInfo(),
        allocatorShaped ? new RoaringBitmap() : null);
  }

  private String locationHost(String endpointHost, boolean allocatorShaped) {
    // Allocators reuse WorkerInfo.host. Packed protobuf decoding creates a new host string per
    // location while splitting the encoded worker ID. This scenario measures one decoded pair's
    // retained object shape, not an entire WorkerResource response.
    return allocatorShaped ? endpointHost : new String(endpointHost.toCharArray());
  }

  private StorageInfo newStorageInfo() {
    return new StorageInfo("", StorageInfo.Type.MEMORY, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
  }

  private void compare(String label, Object oldValue, Object newValue) throws Exception {
    long oldSize = GraphLayout.parseInstance(oldValue).totalSize();
    long newLocationSize = GraphLayout.parseInstance(newValue).totalSize();
    long newSizeIncludingInterner =
        GraphLayout.parseInstance(newValue, endpointInterner()).totalSize();
    long internerOverhead = newSizeIncludingInterner - newLocationSize;
    long saved = oldSize - newSizeIncludingInterner;
    double savedPercentage = oldSize == 0 ? 0 : saved * 100.0 / oldSize;
    int locationCount = java.lang.reflect.Array.getLength(oldValue);
    System.out.printf(
        "PartitionLocation footprint [%s]: old=%d bytes, "
            + "newLocations=%d bytes, weakInternerOverhead=%d bytes, "
            + "newIncludingLiveInterner=%d bytes, oldBytesPerLocation=%.2f, "
            + "newBytesPerLocation=%.2f, savedIncludingLiveInterner=%d bytes (%.2f%%)%n",
        label,
        oldSize,
        newLocationSize,
        internerOverhead,
        newSizeIncludingInterner,
        oldSize / (double) locationCount,
        newSizeIncludingInterner / (double) locationCount,
        saved,
        savedPercentage);
    if (newSizeIncludingInterner >= oldSize) {
      throw new AssertionError(
          "Optimized PartitionLocation retained size including its weak interner must be smaller for "
              + label);
    }
  }

  private Object endpointInterner() throws Exception {
    Field field = WorkerEndpoint.class.getDeclaredField("INTERNED_ENDPOINTS");
    field.setAccessible(true);
    return field.get(null);
  }
}
