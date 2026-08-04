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

package org.apache.celeborn.service.deploy.master.slotsalloc;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;

/**
 * Unit tests for {@link SlotsAllocator#buildStorageInfo}.
 *
 * <p>The method has two main paths:
 *
 * <ol>
 *   <li><b>With budgets</b> – a budget map is provided, and a disk is selected from the worker's
 *       {@link UsableDiskInfo} list.
 *   <li><b>Without budgets</b> – the budget map is {@code null}, and storage type is derived from
 *       the {@code availableStorageTypes} bitmask.
 * </ol>
 */
public class BuildStorageInfoSuiteJ {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private DiskInfo makeDiskInfo(String mountPoint, StorageInfo.Type storageType) {
    return new DiskInfo(mountPoint, 10L * 1024 * 1024 * 1024, 100, 100, 0, storageType);
  }

  private WorkerInfo makeWorker(String host, Map<String, DiskInfo> disks) {
    return new WorkerInfo(host, 9001, 9002, 9003, 9004, 9005, disks, null);
  }

  private Map<WorkerInfo, List<UsableDiskInfo>> budgetsFor(
      WorkerInfo worker, List<UsableDiskInfo> diskList) {
    Map<WorkerInfo, List<UsableDiskInfo>> budgets = new HashMap<>();
    budgets.put(worker, diskList);
    return budgets;
  }

  // ---------------------------------------------------------------------------
  // Tests: budgets != null
  // ---------------------------------------------------------------------------

  /** An HDD disk in the budget list should produce a StorageInfo with the disk mount point. */
  @Test
  public void testWithBudgets_HDDDisk() {
    DiskInfo disk = makeDiskInfo("/mnt/hdd1", StorageInfo.Type.HDD);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("/mnt/hdd1", disk));

    UsableDiskInfo usable = new UsableDiskInfo(disk, 10);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(usable));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(
            worker, budgets, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    assertEquals(StorageInfo.Type.HDD, result.getType());
    assertEquals("/mnt/hdd1", result.getMountPoint());
    assertEquals(StorageInfo.ALL_TYPES_AVAILABLE_MASK, result.availableStorageTypes);
    assertEquals(9, usable.usableSlots); // consumed one slot
  }

  /** An SSD disk in the budget list should produce a StorageInfo with the disk mount point. */
  @Test
  public void testWithBudgets_SSDDisk() {
    DiskInfo disk = makeDiskInfo("/mnt/ssd1", StorageInfo.Type.SSD);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("/mnt/ssd1", disk));

    UsableDiskInfo usable = new UsableDiskInfo(disk, 5);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(usable));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(
            worker, budgets, workerDiskIndex, StorageInfo.LOCAL_DISK_MASK);

    assertEquals(StorageInfo.Type.SSD, result.getType());
    assertEquals("/mnt/ssd1", result.getMountPoint());
    assertEquals(StorageInfo.LOCAL_DISK_MASK, result.availableStorageTypes);
    assertEquals(4, usable.usableSlots);
  }

  /**
   * An HDFS disk in the budget list should produce a StorageInfo with an empty mount point and HDFS
   * type, regardless of the actual mount-point string stored in DiskInfo.
   */
  @Test
  public void testWithBudgets_HDFSDisk_emptyMountPoint() {
    DiskInfo disk = makeDiskInfo("HDFS", StorageInfo.Type.HDFS);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("HDFS", disk));

    UsableDiskInfo usable = new UsableDiskInfo(disk, 50);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(usable));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, budgets, workerDiskIndex, StorageInfo.HDFS_MASK);

    assertEquals(StorageInfo.Type.HDFS, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.HDFS_MASK, result.availableStorageTypes);
    assertEquals(49, usable.usableSlots);
  }

  /**
   * An S3 disk in the budget list should produce a StorageInfo with an empty mount point and S3
   * type.
   */
  @Test
  public void testWithBudgets_S3Disk_emptyMountPoint() {
    DiskInfo disk = makeDiskInfo("S3", StorageInfo.Type.S3);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("S3", disk));

    UsableDiskInfo usable = new UsableDiskInfo(disk, 20);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(usable));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, budgets, workerDiskIndex, StorageInfo.S3_MASK);

    assertEquals(StorageInfo.Type.S3, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.S3_MASK, result.availableStorageTypes);
    assertEquals(19, usable.usableSlots);
  }

  /**
   * An OSS disk in the budget list should produce a StorageInfo with an empty mount point and OSS
   * type.
   */
  @Test
  public void testWithBudgets_OSSDisk_emptyMountPoint() {
    DiskInfo disk = makeDiskInfo("OSS", StorageInfo.Type.OSS);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("OSS", disk));

    UsableDiskInfo usable = new UsableDiskInfo(disk, 30);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(usable));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, budgets, workerDiskIndex, StorageInfo.OSS_MASK);

    assertEquals(StorageInfo.Type.OSS, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.OSS_MASK, result.availableStorageTypes);
    assertEquals(29, usable.usableSlots);
  }

  /**
   * When the first disk in the budget list has zero usable slots, the method must skip it and
   * select the next disk with available capacity.
   */
  @Test
  public void testWithBudgets_skipExhaustedDisk() {
    DiskInfo disk1 = makeDiskInfo("/mnt/disk1", StorageInfo.Type.HDD);
    DiskInfo disk2 = makeDiskInfo("/mnt/disk2", StorageInfo.Type.HDD);
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    UsableDiskInfo exhausted = new UsableDiskInfo(disk1, 0);
    UsableDiskInfo active = new UsableDiskInfo(disk2, 8);
    List<UsableDiskInfo> diskList = new ArrayList<>();
    diskList.add(exhausted);
    diskList.add(active);

    Map<WorkerInfo, List<UsableDiskInfo>> budgets = budgetsFor(worker, diskList);
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0); // start at the exhausted disk

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(
            worker, budgets, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    assertEquals(StorageInfo.Type.HDD, result.getType());
    assertEquals("/mnt/disk2", result.getMountPoint()); // disk2 was chosen
    assertEquals(0, exhausted.usableSlots); // disk1 untouched
    assertEquals(7, active.usableSlots); // disk2 consumed one slot
  }

  /**
   * After assigning a slot on a local disk, the disk index stored in {@code workerDiskIndex} must
   * advance to the next disk (round-robin).
   */
  @Test
  public void testWithBudgets_localDiskAdvancesDiskIndex() {
    DiskInfo disk1 = makeDiskInfo("/mnt/disk1", StorageInfo.Type.HDD);
    DiskInfo disk2 = makeDiskInfo("/mnt/disk2", StorageInfo.Type.HDD);
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    List<UsableDiskInfo> diskList = new ArrayList<>();
    diskList.add(new UsableDiskInfo(disk1, 10));
    diskList.add(new UsableDiskInfo(disk2, 10));

    Map<WorkerInfo, List<UsableDiskInfo>> budgets = budgetsFor(worker, diskList);
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    SlotsAllocator.buildStorageInfo(
        worker, budgets, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    assertEquals(Integer.valueOf(1), workerDiskIndex.get(worker));
  }

  /**
   * For DFS storage types (HDFS, S3, OSS), the disk index in {@code workerDiskIndex} must NOT be
   * advanced after slot assignment, because there is only a single logical endpoint.
   */
  @Test
  public void testWithBudgets_HDFSDiskDoesNotAdvanceDiskIndex() {
    DiskInfo disk = makeDiskInfo("HDFS", StorageInfo.Type.HDFS);
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        budgetsFor(worker, Collections.singletonList(new UsableDiskInfo(disk, 10)));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    SlotsAllocator.buildStorageInfo(worker, budgets, workerDiskIndex, StorageInfo.HDFS_MASK);

    assertEquals(Integer.valueOf(0), workerDiskIndex.get(worker));
  }

  /**
   * Consecutive calls with two local disks in the budget list must cycle through the disks in
   * order, demonstrating round-robin selection.
   */
  @Test
  public void testWithBudgets_roundRobinAcrossLocalDisks() {
    DiskInfo disk1 = makeDiskInfo("/mnt/disk1", StorageInfo.Type.HDD);
    DiskInfo disk2 = makeDiskInfo("/mnt/disk2", StorageInfo.Type.HDD);
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    UsableDiskInfo usable1 = new UsableDiskInfo(disk1, 10);
    UsableDiskInfo usable2 = new UsableDiskInfo(disk2, 10);
    List<UsableDiskInfo> diskList = new ArrayList<>();
    diskList.add(usable1);
    diskList.add(usable2);

    Map<WorkerInfo, List<UsableDiskInfo>> budgets = budgetsFor(worker, diskList);
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);
    // First call: disk at index 0
    StorageInfo result1 =
        SlotsAllocator.buildStorageInfo(
            worker, budgets, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
    assertEquals("/mnt/disk1", result1.getMountPoint());
    assertEquals(9, usable1.usableSlots);
    assertEquals(Integer.valueOf(1), workerDiskIndex.get(worker));

    // Second call: disk at index 1
    StorageInfo result2 =
        SlotsAllocator.buildStorageInfo(
            worker, budgets, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
    assertEquals("/mnt/disk2", result2.getMountPoint());
    assertEquals(9, usable2.usableSlots);
    assertEquals(Integer.valueOf(0), workerDiskIndex.get(worker)); // wrapped around
  }

  // ---------------------------------------------------------------------------
  // Tests: budgets == null
  // ---------------------------------------------------------------------------

  /**
   * When budgets are {@code null} and all storage types are available (mask = 0), the method must
   * pick a local disk from the worker's disk map.
   */
  @Test
  public void testWithoutBudgets_allTypesAvailable_picksLocalDisk() {
    DiskInfo disk = makeDiskInfo("/mnt/hdd1", StorageInfo.Type.HDD);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("/mnt/hdd1", disk));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(
            worker, null, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    assertEquals(StorageInfo.Type.HDD, result.getType());
    assertEquals("/mnt/hdd1", result.getMountPoint());
    assertEquals(StorageInfo.ALL_TYPES_AVAILABLE_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and only LOCAL_DISK_MASK is set, the method must pick an SSD disk
   * from the worker and record its mount point.
   */
  @Test
  public void testWithoutBudgets_localDiskMask() {
    DiskInfo disk = makeDiskInfo("/mnt/ssd1", StorageInfo.Type.SSD);
    WorkerInfo worker = makeWorker("host1", Collections.singletonMap("/mnt/ssd1", disk));
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0);

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, null, workerDiskIndex, StorageInfo.LOCAL_DISK_MASK);

    assertEquals(StorageInfo.Type.SSD, result.getType());
    assertEquals("/mnt/ssd1", result.getMountPoint());
    assertEquals(StorageInfo.LOCAL_DISK_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and only S3_MASK is set, the method must return a StorageInfo
   * with empty mount point and S3 type without touching any worker disks.
   */
  @Test
  public void testWithoutBudgets_S3Only() {
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, null, new HashMap<>(), StorageInfo.S3_MASK);

    assertEquals(StorageInfo.Type.S3, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.S3_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and only OSS_MASK is set, the method must return a StorageInfo
   * with empty mount point and OSS type.
   */
  @Test
  public void testWithoutBudgets_OSSOnly() {
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, null, new HashMap<>(), StorageInfo.OSS_MASK);

    assertEquals(StorageInfo.Type.OSS, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.OSS_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and only HDFS_MASK is set, the method must return a StorageInfo
   * with empty mount point and HDFS type.
   */
  @Test
  public void testWithoutBudgets_HDFSOnly() {
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, null, new HashMap<>(), StorageInfo.HDFS_MASK);

    assertEquals(StorageInfo.Type.HDFS, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.HDFS_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and only MEMORY_MASK is set, the method must return a StorageInfo
   * with empty mount point and MEMORY type.
   */
  @Test
  public void testWithoutBudgets_memoryOnly() {
    WorkerInfo worker = makeWorker("host1", new HashMap<>());

    StorageInfo result =
        SlotsAllocator.buildStorageInfo(worker, null, new HashMap<>(), StorageInfo.MEMORY_MASK);

    assertEquals(StorageInfo.Type.MEMORY, result.getType());
    assertEquals("", result.getMountPoint());
    assertEquals(StorageInfo.MEMORY_MASK, result.availableStorageTypes);
  }

  /**
   * When budgets are {@code null} and the bitmask does not correspond to any known storage type,
   * the method must throw {@link IllegalStateException}.
   */
  @Test(expected = IllegalStateException.class)
  public void testWithoutBudgets_noValidStorageType_throwsIllegalState() {
    WorkerInfo worker = makeWorker("host1", new HashMap<>());
    // 0b100000 = 32 has none of the bits used by known storage types
    int unknownMask = 0b100000;
    SlotsAllocator.buildStorageInfo(worker, null, new HashMap<>(), unknownMask);
  }

  /**
   * When budgets are {@code null} and a local disk is selected, the disk index in {@code
   * workerDiskIndex} must advance so that the next call picks the following disk (round-robin).
   */
  @Test
  public void testWithoutBudgets_localDiskAdvancesDiskIndex() {
    DiskInfo disk1 = makeDiskInfo("/mnt/disk1", StorageInfo.Type.HDD);
    DiskInfo disk2 = makeDiskInfo("/mnt/disk2", StorageInfo.Type.HDD);

    // Use a LinkedHashMap so that disk1 comes before disk2 during stream iteration.
    Map<String, DiskInfo> disks = new LinkedHashMap<>();
    disks.put("/mnt/disk1", disk1);
    disks.put("/mnt/disk2", disk2);
    WorkerInfo worker = makeWorker("host1", disks);

    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    workerDiskIndex.put(worker, 0); // force first disk
    // First call selects disk1 and advances the index to 1
    StorageInfo result1 =
        SlotsAllocator.buildStorageInfo(
            worker, null, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
    assertEquals("/mnt/disk1", result1.getMountPoint());
    assertEquals(Integer.valueOf(1), workerDiskIndex.get(worker));

    // Second call selects disk2 and wraps the index back to 0
    StorageInfo result2 =
        SlotsAllocator.buildStorageInfo(
            worker, null, workerDiskIndex, StorageInfo.ALL_TYPES_AVAILABLE_MASK);
    assertEquals("/mnt/disk2", result2.getMountPoint());
    assertEquals(Integer.valueOf(0), workerDiskIndex.get(worker));
  }
}
