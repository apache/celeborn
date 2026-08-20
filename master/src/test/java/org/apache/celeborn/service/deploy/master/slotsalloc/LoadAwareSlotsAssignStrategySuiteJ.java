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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;

public class LoadAwareSlotsAssignStrategySuiteJ {

  @Test
  public void testProviderCreatesIndependentStrategiesFromConfiguration() {
    List<WorkerInfo> workers = createWorkers(3);
    List<Integer> partitionIds = createPartitionIds(70);

    SlotsAssignStrategy evenStrategy = createStrategy(0);
    Map<WorkerInfo, List<UsableDiskInfo>> evenBudgets =
        evenStrategy.computeSlotBudgets(workers, partitionIds, false, StorageInfo.LOCAL_DISK_MASK);
    assertEquals(24, allocatedSlots(evenBudgets, workers.get(0)));
    assertEquals(24, allocatedSlots(evenBudgets, workers.get(1)));
    assertEquals(22, allocatedSlots(evenBudgets, workers.get(2)));

    SlotsAssignStrategy weightedStrategy = createStrategy(1);
    Map<WorkerInfo, List<UsableDiskInfo>> weightedBudgets =
        weightedStrategy.computeSlotBudgets(
            workers, partitionIds, false, StorageInfo.LOCAL_DISK_MASK);
    assertEquals(40, allocatedSlots(weightedBudgets, workers.get(0)));
    assertEquals(20, allocatedSlots(weightedBudgets, workers.get(1)));
    assertEquals(10, allocatedSlots(weightedBudgets, workers.get(2)));
  }

  @Test
  public void testProviderRejectsInvalidConfiguration() {
    assertInvalidConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "0");
    assertInvalidConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "-1");

    assertInvalidDoubleConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_GRADIENT().key());
    assertInvalidDoubleConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_FLUSHTIME_WEIGHT().key());
    assertInvalidDoubleConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_FETCHTIME_WEIGHT().key());
    assertInvalidDoubleConfiguration(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_ACTIVE_SLOTS_WEIGHT().key());
  }

  private static SlotsAssignStrategy createStrategy(double diskGroupGradient) {
    CelebornConf conf = validLoadAwareConf();
    conf.set(
        CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_GRADIENT().key(),
        String.valueOf(diskGroupGradient));
    return new LoadAwareSlotsAssignStrategyProvider().create(conf);
  }

  private static CelebornConf validLoadAwareConf() {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "3");
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_FLUSHTIME_WEIGHT().key(), "1");
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_FETCHTIME_WEIGHT().key(), "10");
    conf.set(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_ACTIVE_SLOTS_WEIGHT().key(), "100");
    return conf;
  }

  private static void assertInvalidDoubleConfiguration(String key) {
    assertInvalidConfiguration(key, "-1");
    assertInvalidConfiguration(key, "NaN");
    assertInvalidConfiguration(key, "Infinity");
  }

  private static void assertInvalidConfiguration(String key, String value) {
    CelebornConf conf = validLoadAwareConf().set(key, value);

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LoadAwareSlotsAssignStrategyProvider().create(conf));

    assertTrue(error.getMessage().contains(key));
  }

  private static List<WorkerInfo> createWorkers(int workerCount) {
    List<WorkerInfo> workers = new ArrayList<>();
    for (int i = 0; i < workerCount; i++) {
      String mountPoint = "/mnt/disk" + i;
      DiskInfo disk =
          new DiskInfo(mountPoint, 1024, i == 0 ? 1 : 0, i == 1 ? 1 : 0, i == 2 ? 1 : 0);
      disk.availableSlots_$eq(100);
      Map<String, DiskInfo> disks = new HashMap<>();
      disks.put(mountPoint, disk);
      workers.add(new WorkerInfo("host" + i, i, i, i, i, i, disks, null));
    }
    return workers;
  }

  private static List<Integer> createPartitionIds(int partitionCount) {
    return IntStream.range(0, partitionCount).boxed().collect(Collectors.toList());
  }

  private static long allocatedSlots(
      Map<WorkerInfo, List<UsableDiskInfo>> budgets, WorkerInfo worker) {
    return budgets.get(worker).stream().mapToLong(info -> info.usableSlots).sum();
  }
}
