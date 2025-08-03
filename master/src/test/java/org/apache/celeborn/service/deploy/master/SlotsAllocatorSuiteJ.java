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

package org.apache.celeborn.service.deploy.master;

import static org.junit.Assert.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class SlotsAllocatorSuiteJ {
  private List<WorkerInfo> prepareWorkers(boolean hasDisks) {
    long assumedPartitionSize = 64 * 1024 * 1024;
    Random random = new Random();
    return basePrepareWorkers(
        3,
        hasDisks,
        ImmutableMap.of(
            "/mnt/disk1", random.nextInt() + 100 * 1024 * 1024 * 1024L,
            "/mnt/disk2", random.nextInt() + 95 * 1024 * 1024 * 1024L,
            "/mnt/disk3", random.nextInt() + 90 * 1024 * 1024 * 1024L),
        assumedPartitionSize,
        3,
        false,
        random);
  }

  @Test
  public void testAllocateSlotsForEmptyPartitionId() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Collections.emptyList();
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSinglePartitionId() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Collections.singletonList(0);
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForSinglePartitionIdWithoutReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Collections.singletonList(0);
    final boolean shouldReplicate = false;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForTwoPartitionIdsWithoutReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Arrays.asList(0, 1);
    final boolean shouldReplicate = false;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForThreePartitionIdsWithoutReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = false;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocateSlotsForThreeReduceIdsWithReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = Arrays.asList(0, 1, 2);
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocate3000ReduceIdsWithReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocate3000ReduceIdsWithoutReplicate() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testAllocate3000ReduceIdsWithReplicateOnRoundRobin() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;

    check(workers, partitionIds, shouldReplicate, true, true, false, 0);
  }

  private void check(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean expectSuccess) {
    check(workers, partitionIds, shouldReplicate, expectSuccess, false, false, 0);
  }

  private Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> check(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean expectSuccess,
      boolean roundrobin,
      boolean interruptionAware,
      int interruptionAwareThreshold) {
    String shuffleKey = "appId-1";
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "2");
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_GRADIENT().key(), "1");
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots;
    if (roundrobin) {
      slots =
          SlotsAllocator.offerSlotsRoundRobin(
              workers,
              partitionIds,
              shouldReplicate,
              false,
              StorageInfo.ALL_TYPES_AVAILABLE_MASK,
              interruptionAware,
              interruptionAwareThreshold);
    } else {
      slots =
          SlotsAllocator.offerSlotsLoadAware(
              workers,
              partitionIds,
              shouldReplicate,
              false,
              conf.masterSlotAssignLoadAwareDiskGroupNum(),
              conf.masterSlotAssignLoadAwareDiskGroupGradient(),
              conf.masterSlotAssignLoadAwareFlushTimeWeight(),
              conf.masterSlotAssignLoadAwareFetchTimeWeight(),
              StorageInfo.ALL_TYPES_AVAILABLE_MASK,
              interruptionAware,
              interruptionAwareThreshold);
    }
    if (expectSuccess) {
      if (shouldReplicate) {
        slots.forEach(
            (k, v) -> {
              Set<String> locationDuplicationSet = new HashSet<>();
              v._1.forEach(
                  i -> {
                    String uniqueId = i.getUniqueId();
                    assertFalse(locationDuplicationSet.contains(uniqueId));
                    locationDuplicationSet.add(uniqueId);
                    for (PartitionLocation location : v._1) {
                      assertTrue(
                          location.getHost().equals(k.host())
                              && location.getRpcPort() == k.rpcPort()
                              && location.getPushPort() == k.pushPort()
                              && location.getFetchPort() == k.fetchPort());
                    }
                  });
            });
      }
      Map<WorkerInfo, Map<String, Integer>> workerToAllocatedSlots =
          SlotsAllocator.slotsToDiskAllocations(slots);
      int unknownDiskSlots = 0;
      for (Map.Entry<WorkerInfo, Map<String, Integer>> entry : workerToAllocatedSlots.entrySet()) {
        WorkerInfo worker = entry.getKey();
        Map<String, Integer> allocationMap = entry.getValue();
        worker.allocateSlots(shuffleKey, allocationMap);
        if (allocationMap.containsKey("UNKNOWN_DISK")) {
          unknownDiskSlots += allocationMap.get("UNKNOWN_DISK");
        }
        if (roundrobin && !allocationMap.isEmpty()) {
          int maxSlots = Collections.max(allocationMap.values());
          int minSlots = Collections.min(allocationMap.values());
          assertTrue(
              "Worker "
                  + worker.host()
                  + " has unbalanced slot allocation. "
                  + "Max: "
                  + maxSlots
                  + ", Min: "
                  + minSlots,
              maxSlots - minSlots <= 1);
        }
      }
      int allocateToDiskSlots = 0;
      for (WorkerInfo worker : workers) {
        allocateToDiskSlots = (int) (allocateToDiskSlots + worker.usedSlots());
      }
      if (shouldReplicate) {
        assertTrue(partitionIds.size() * 2 >= unknownDiskSlots + allocateToDiskSlots);
      } else {
        assertTrue(partitionIds.size() >= unknownDiskSlots + allocateToDiskSlots);
      }
      assertEquals(0, unknownDiskSlots);
    } else {
      assertTrue(
          "Expect to fail to offer slots, but return " + slots.size() + " slots.", slots.isEmpty());
    }
    return slots;
  }

  private void checkSlotsOnDFS(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean expectSuccess,
      boolean roundRobin,
      boolean enableS3) {
    checkSlotsOnDFS(
        workers, partitionIds, shouldReplicate, expectSuccess, roundRobin, enableS3, false);
  }

  private void checkSlotsOnDFS(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean expectSuccess,
      boolean roundRobin,
      boolean enableS3,
      boolean enableOss) {
    CelebornConf conf = new CelebornConf();
    int availableStorageTypes;
    if (enableS3) {
      conf.set("celeborn.active.storage.levels", "S3");
      availableStorageTypes = StorageInfo.S3_MASK;
    } else if (enableOss) {
      conf.set("celeborn.active.storage.levels", "OSS");
      availableStorageTypes = StorageInfo.OSS_MASK;
    } else {
      conf.set("celeborn.active.storage.levels", "HDFS");
      availableStorageTypes = StorageInfo.HDFS_MASK;
    }
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots;
    if (roundRobin) {
      slots =
          SlotsAllocator.offerSlotsRoundRobin(
              workers, partitionIds, shouldReplicate, false, availableStorageTypes, false, 0);
    } else {
      slots =
          SlotsAllocator.offerSlotsLoadAware(
              workers,
              partitionIds,
              shouldReplicate,
              false,
              3,
              0.1,
              0,
              1,
              StorageInfo.LOCAL_DISK_MASK | availableStorageTypes,
              false,
              0);
    }
    int allocatedPartitionCount = 0;
    for (Map.Entry<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
        workerToPartitions : slots.entrySet()) {
      List<PartitionLocation> primaryLocs = workerToPartitions.getValue()._1;
      List<PartitionLocation> replicaLocs = workerToPartitions.getValue()._2();
      allocatedPartitionCount += primaryLocs.size();
      allocatedPartitionCount += replicaLocs.size();
    }
    assertEquals(expectSuccess, !slots.isEmpty());
    assertEquals(
        shouldReplicate ? partitionIds.size() * 2 : partitionIds.size(), allocatedPartitionCount);
  }

  @Test
  public void testHDFSOnly() {
    final List<WorkerInfo> workers = prepareWorkers(false);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, false);
  }

  @Test
  public void testLocalDisksAndHDFSOnRoundRobin() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    DiskInfo hdfs1 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    DiskInfo hdfs2 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    DiskInfo hdfs3 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    hdfs1.maxSlots_$eq(Long.MAX_VALUE);
    hdfs2.maxSlots_$eq(Long.MAX_VALUE);
    hdfs3.maxSlots_$eq(Long.MAX_VALUE);
    workers.get(0).diskInfos().put("HDFS", hdfs1);
    workers.get(1).diskInfos().put("HDFS", hdfs2);
    workers.get(2).diskInfos().put("HDFS", hdfs3);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, false);
  }

  @Test
  public void testLocalDisksAndHDFSOnLoadAware() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    DiskInfo hdfs1 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    DiskInfo hdfs2 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    //    DiskInfo hdfs3 = new DiskInfo("HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE,
    // StorageInfo.Type.HDFS);
    hdfs1.maxSlots_$eq(Long.MAX_VALUE);
    hdfs2.maxSlots_$eq(Long.MAX_VALUE);
    //    hdfs3.maxSlots_$eq(Long.MAX_VALUE);
    workers.get(0).diskInfos().put("HDFS", hdfs1);
    workers.get(1).diskInfos().put("HDFS", hdfs2);
    //    workers.get(2).diskInfos().put("HDFS", hdfs3);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, false, false);
  }

  @Test
  public void testLocalDisksAndHDFSOnLoadAwareWithInsufficientSlots() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    DiskInfo hdfs1 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    DiskInfo hdfs2 =
        new DiskInfo(
            "HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.HDFS);
    //    DiskInfo hdfs3 = new DiskInfo("HDFS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE,
    // StorageInfo.Type.HDFS);
    hdfs1.maxSlots_$eq(Long.MAX_VALUE);
    hdfs2.maxSlots_$eq(Long.MAX_VALUE);
    //    hdfs3.maxSlots_$eq(Long.MAX_VALUE);
    workers.get(0).diskInfos().put("HDFS", hdfs1);
    workers.get(1).diskInfos().put("HDFS", hdfs2);
    for (Map.Entry<String, DiskInfo> diskEntry : workers.get(2).diskInfos().entrySet()) {
      diskEntry.getValue().maxSlots_$eq(100);
    }
    //    workers.get(2).diskInfos().put("HDFS", hdfs3);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, false, false);
  }

  @Test
  public void testAllocateSlotsWithNoAvailableSlots() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    // Simulates no available slots behavior with greatly changed estimatedPartitionSize for workers
    // with usable disks.
    workers.forEach(workerInfo -> workerInfo.updateDiskSlots(Long.MAX_VALUE));
    final List<Integer> partitionIds = Collections.singletonList(0);
    final boolean shouldReplicate = false;

    check(workers, partitionIds, shouldReplicate, true);
  }

  @Test
  public void testS3Only() {
    final List<WorkerInfo> workers = prepareWorkers(false);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, true);
  }

  @Test
  public void testLocalDisksAndS3() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    DiskInfo s3DiskInfo1 =
        new DiskInfo("S3", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.S3);
    DiskInfo s3DiskInfo2 =
        new DiskInfo("S3", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.S3);
    s3DiskInfo1.maxSlots_$eq(Long.MAX_VALUE);
    s3DiskInfo2.maxSlots_$eq(Long.MAX_VALUE);
    workers.get(0).diskInfos().put("S3", s3DiskInfo1);
    workers.get(1).diskInfos().put("S3", s3DiskInfo2);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, true);
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, false, true);
  }

  @Test
  public void testOssOnly() {
    final List<WorkerInfo> workers = prepareWorkers(false);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, false, true);
  }

  @Test
  public void testLocalDisksAndOss() {
    final List<WorkerInfo> workers = prepareWorkers(true);
    DiskInfo ossDiskInfo1 =
        new DiskInfo(
            "OSS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.OSS);
    DiskInfo ossDiskInfo2 =
        new DiskInfo(
            "OSS", Long.MAX_VALUE, 999999, 999999, Integer.MAX_VALUE, StorageInfo.Type.OSS);
    ossDiskInfo1.maxSlots_$eq(Long.MAX_VALUE);
    ossDiskInfo2.maxSlots_$eq(Long.MAX_VALUE);
    workers.get(0).diskInfos().put("OSS", ossDiskInfo1);
    workers.get(1).diskInfos().put("OSS", ossDiskInfo2);
    final List<Integer> partitionIds = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      partitionIds.add(i);
    }
    final boolean shouldReplicate = true;
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, true, false, true);
    checkSlotsOnDFS(workers, partitionIds, shouldReplicate, true, false, false, true);
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, false", "false, true"})
  public void testInterruptionAwareSlotSelection(boolean shouldReplicate, boolean shouldRackAware) {
    long assumedPartitionSize = 64 * 1024 * 1024;
    double interruptionAwarePercentileThreshold = 50;
    Map<String, Long> diskPartitionToSize = new HashMap<>();
    diskPartitionToSize.put("/mnt/disk", 512 * 1024 * 1024L); // 0.5gb disk space
    // Cluster usable space is 50g, with 25g that will not be interrupted.
    List<WorkerInfo> workers =
        basePrepareWorkers(
            100, true, diskPartitionToSize, assumedPartitionSize, 20, true, new Random());
    Map<String, WorkerInfo> workersMap =
        workers.stream().collect(Collectors.toMap(WorkerInfo::host, worker -> worker));
    Tuple3<List<WorkerInfo>, List<WorkerInfo>, List<WorkerInfo>> prioritization =
        SlotsAllocator.prioritizeWorkersBasedOnInterruptionNotice(
            workers, shouldReplicate, shouldRackAware, interruptionAwarePercentileThreshold);
    List<WorkerInfo> workersWithoutInterruptions = prioritization._1();
    List<WorkerInfo> workersWithLateInterruptions = prioritization._2();
    List<WorkerInfo> workersWithEarlyInterruptions = prioritization._3();
    List<String> workersWithoutInterruptionsHosts = extractHosts(workersWithoutInterruptions);
    List<String> workersWithLateInterruptionsHosts = extractHosts(workersWithLateInterruptions);
    List<String> workersWithEarlyInterruptionsHosts = extractHosts(workersWithEarlyInterruptions);

    assertEquals(50, workersWithoutInterruptionsHosts.size());
    assertEquals(25, workersWithLateInterruptionsHosts.size());
    assertEquals(25, workersWithEarlyInterruptionsHosts.size());
    IntStream.range(0, 100)
        .forEach(
            i -> {
              String host = "host" + i;
              if (i % 2 == 0) {
                assertTrue(workersWithoutInterruptionsHosts.contains(host));
              } else if (i >= 51) {
                assertTrue(workersWithLateInterruptionsHosts.contains(host));
              } else {
                assertTrue(workersWithEarlyInterruptionsHosts.contains(host));
              }
            });

    // With replication enabled: 150 partitions * 128mb (64 primary, 64 replica) is roughly 19gb.
    // Both primaries and replicas should fit into workersWithoutInterruptions, since 19gb < 25gb
    // uninterrupted capacity.
    //
    // With replication disabled: 150 partitions * 64mb is roughly 9gb.
    // Similar to the above case, all primaries should fit into workersWithoutInterruptions since
    // 9gb < 25gb uninterrupted capacity.
    List<Integer> bestCasePartitionIds =
        IntStream.range(0, 150).boxed().collect(Collectors.toList());
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
        slotsFromBestCasePartitionIds =
            SlotsAllocator.offerSlotsRoundRobin(
                workers,
                bestCasePartitionIds,
                shouldReplicate,
                shouldRackAware,
                StorageInfo.ALL_TYPES_AVAILABLE_MASK,
                true,
                (int) interruptionAwarePercentileThreshold);
    slotsFromBestCasePartitionIds
        .values()
        .forEach(
            primaryReplicaSlots -> {
              List<PartitionLocation> primarySlots = primaryReplicaSlots._1;
              List<PartitionLocation> replicaSlots = primaryReplicaSlots._2;
              assertTrue(
                  primarySlots.stream()
                      .map(PartitionLocation::getHost)
                      .allMatch(workersWithoutInterruptionsHosts::contains));
              if (shouldReplicate) {
                assertTrue(
                    replicaSlots.stream()
                        .map(PartitionLocation::getHost)
                        .allMatch(workersWithoutInterruptionsHosts::contains));
                if (shouldRackAware) {
                  primarySlots.forEach(
                      slot -> {
                        WorkerInfo primary = workersMap.get(slot.getHost());
                        WorkerInfo replica = workersMap.get(slot.getPeer().getHost());
                        assertNotSame(primary.networkLocation(), replica.networkLocation());
                      });
                }
              }
            });

    List<WorkerInfo> primaryWorkerCandidates =
        combineWorkers(workersWithoutInterruptions, workersWithLateInterruptions);
    List<String> primaryWorkerCandidatesHosts = extractHosts(primaryWorkerCandidates);

    // With replication enabled: 300 partitions * 128mb (64 primary, 64 replica) is roughly 38gb.
    // In this case, primaries should be in workersWithoutInterruptions +
    // workersWithLateInterruptions, while
    // replicas can spill over into workersWithEarlyInterruptions.
    //
    // With replication disabled, we increase partitions to 600 to force this case:
    // 600 partitions * 64mb is roughly 38gb.
    // Similar to the above case, all primaries should be in workersWithoutInterruptions +
    // workersWithLateInterruptions.
    List<Integer> spillOverCasePartitionIds;
    if (shouldReplicate) {
      spillOverCasePartitionIds = IntStream.range(0, 300).boxed().collect(Collectors.toList());
    } else {
      spillOverCasePartitionIds = IntStream.range(0, 600).boxed().collect(Collectors.toList());
    }
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
        slotsFromSpillOverCasePartitionIds =
            SlotsAllocator.offerSlotsRoundRobin(
                workers,
                spillOverCasePartitionIds,
                shouldReplicate,
                shouldRackAware,
                StorageInfo.ALL_TYPES_AVAILABLE_MASK,
                true,
                (int) interruptionAwarePercentileThreshold);
    slotsFromSpillOverCasePartitionIds
        .values()
        .forEach(
            primaryReplicaSlots -> {
              List<PartitionLocation> primarySlots = primaryReplicaSlots._1;
              List<PartitionLocation> replicaSlots = primaryReplicaSlots._2;
              assertTrue(
                  primarySlots.stream()
                      .map(PartitionLocation::getHost)
                      .allMatch(primaryWorkerCandidatesHosts::contains));
              assertTrue(
                  primarySlots.stream()
                      .map(PartitionLocation::getHost)
                      .noneMatch(workersWithEarlyInterruptionsHosts::contains));
              if (shouldReplicate) {
                assertTrue(
                    replicaSlots.stream()
                        .map(PartitionLocation::getHost)
                        .allMatch(
                            host ->
                                primaryWorkerCandidatesHosts.contains(host)
                                    || workersWithEarlyInterruptionsHosts.contains(host)));
                if (shouldRackAware) {
                  primarySlots.forEach(
                      slot -> {
                        WorkerInfo primary = workersMap.get(slot.getHost());
                        WorkerInfo replica = workersMap.get(slot.getPeer().getHost());
                        assertNotSame(primary.networkLocation(), replica.networkLocation());
                      });
                }
              }
            });
    // With the slot restrictions in place for LoadAware, we expect to spill replicas into
    // workersWithEarlyInterruptionsHosts.
    // But primaries should be in workersWithoutInterruptions + workersWithLateInterruptions.
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
        loadAwareBestCasePartitionIdsSlots =
            check(
                workers,
                spillOverCasePartitionIds,
                shouldReplicate,
                true,
                false,
                true,
                (int) interruptionAwarePercentileThreshold);
    loadAwareBestCasePartitionIdsSlots
        .values()
        .forEach(
            primaryReplicaSlots -> {
              List<PartitionLocation> primarySlots = primaryReplicaSlots._1;
              List<PartitionLocation> replicaSlots = primaryReplicaSlots._2;
              assertTrue(
                  primarySlots.stream()
                      .map(PartitionLocation::getHost)
                      .allMatch(primaryWorkerCandidatesHosts::contains));
              assertTrue(
                  primarySlots.stream()
                      .map(PartitionLocation::getHost)
                      .noneMatch(workersWithEarlyInterruptionsHosts::contains));
              if (shouldReplicate) {
                assertTrue(
                    replicaSlots.stream()
                        .map(PartitionLocation::getHost)
                        .allMatch(
                            host ->
                                primaryWorkerCandidatesHosts.contains(host)
                                    || workersWithEarlyInterruptionsHosts.contains(host)));
                if (shouldRackAware) {
                  primarySlots.forEach(
                      slot -> {
                        WorkerInfo primary = workersMap.get(slot.getHost());
                        WorkerInfo replica = workersMap.get(slot.getPeer().getHost());
                        assertNotSame(primary.networkLocation(), replica.networkLocation());
                      });
                }
              }
            });
  }

  @Test
  public void testInterruptionAwareSlotSelectionWithNoInterruptions() {
    long assumedPartitionSize = 64 * 1024 * 1024;
    Map<String, Long> diskPartitionToSize = new HashMap<>();
    diskPartitionToSize.put("/mnt/disk", 512 * 1024 * 1024L); // 0.5gb disk space
    // Cluster usable space is 50g, with 25g that will not be interrupted.
    List<WorkerInfo> workers =
        basePrepareWorkers(
            100, true, diskPartitionToSize, assumedPartitionSize, 20, false, new Random());
    List<Integer> partitionIds = IntStream.range(0, 600).boxed().collect(Collectors.toList());
    check(workers, partitionIds, true, true, false, true, 50);
  }

  private List<String> extractHosts(List<WorkerInfo> workers) {
    return workers.stream().map(WorkerInfo::host).collect(Collectors.toList());
  }

  private List<WorkerInfo> combineWorkers(List<WorkerInfo>... workerLists) {
    return Arrays.stream(workerLists).flatMap(List::stream).collect(Collectors.toList());
  }

  static List<WorkerInfo> basePrepareWorkers(
      int numWorkers,
      boolean hasDisks,
      Map<String, Long> diskPartitionToSize,
      long assumedPartitionSize,
      int numNetworkLocations,
      boolean hasInterruptions,
      Random random) {
    return IntStream.range(0, numWorkers)
        .mapToObj(
            i -> {
              Map<String, DiskInfo> disks = new HashMap<>();
              if (hasDisks) {
                diskPartitionToSize.forEach(
                    (diskMountPoint, diskSize) -> {
                      DiskInfo diskInfo =
                          new DiskInfo(
                              diskMountPoint,
                              diskSize,
                              random.nextInt(1000),
                              random.nextInt(1000),
                              0);
                      diskInfo.availableSlots_$eq(
                          diskInfo.actualUsableSpace() / assumedPartitionSize);
                      disks.put(diskMountPoint, diskInfo);
                    });
              }
              WorkerInfo worker = new WorkerInfo("host" + i, i, i, i, i, i, disks, null);
              if (hasInterruptions) {
                if (i % 2 == 0) {
                  worker.nextInterruptionNotice_$eq(Long.MAX_VALUE);
                } else {
                  worker.nextInterruptionNotice_$eq(i);
                }
              }
              worker.networkLocation_$eq(String.valueOf(i % numNetworkLocations));
              return worker;
            })
        .collect(Collectors.toList());
  }
}
