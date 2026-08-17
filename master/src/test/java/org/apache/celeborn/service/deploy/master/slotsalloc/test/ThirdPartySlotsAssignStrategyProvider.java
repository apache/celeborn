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

package org.apache.celeborn.service.deploy.master.slotsalloc.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategy;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategyProvider;
import org.apache.celeborn.service.deploy.master.slotsalloc.UsableDiskInfo;

/** Simulates a third-party provider packaged outside Celeborn's slots-allocation package. */
public final class ThirdPartySlotsAssignStrategyProvider implements SlotsAssignStrategyProvider {

  public static final String NAME = "THIRD_PARTY_LIMITED";
  public static final String MAX_SLOTS_PER_DISK_KEY =
      "celeborn.master.slot.assign.thirdParty.maxSlotsPerDisk";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SlotsAssignStrategy create(CelebornConf conf) {
    if (!conf.contains(MAX_SLOTS_PER_DISK_KEY)) {
      throw new IllegalArgumentException(
          "Missing third-party configuration: " + MAX_SLOTS_PER_DISK_KEY);
    }

    long maxSlotsPerDisk = conf.getLong(MAX_SLOTS_PER_DISK_KEY, -1L);
    if (maxSlotsPerDisk <= 0) {
      throw new IllegalArgumentException(MAX_SLOTS_PER_DISK_KEY + " must be positive");
    }
    return new LimitedSlotsAssignStrategy(maxSlotsPerDisk);
  }

  private static final class LimitedSlotsAssignStrategy implements SlotsAssignStrategy {
    private final long maxSlotsPerDisk;

    private LimitedSlotsAssignStrategy(long maxSlotsPerDisk) {
      this.maxSlotsPerDisk = maxSlotsPerDisk;
    }

    @Override
    public Map<WorkerInfo, List<UsableDiskInfo>> computeSlotBudgets(
        List<WorkerInfo> workers,
        List<Integer> partitionIds,
        boolean shouldReplicate,
        int availableStorageTypes) {
      Map<WorkerInfo, List<UsableDiskInfo>> budgets = new HashMap<>();
      for (WorkerInfo worker : workers) {
        for (DiskInfo disk : worker.diskInfos().values()) {
          if (DiskStatus.HEALTHY.equals(disk.status())
              && StorageInfo.isAvailable(disk.storageType(), availableStorageTypes)
              && disk.getAvailableSlots() > 0) {
            budgets
                .computeIfAbsent(worker, ignored -> new ArrayList<>())
                .add(new UsableDiskInfo(disk, Math.min(maxSlotsPerDisk, disk.getAvailableSlots())));
          }
        }
      }
      return budgets;
    }
  }
}
