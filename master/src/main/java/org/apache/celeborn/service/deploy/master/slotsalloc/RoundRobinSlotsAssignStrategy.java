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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;

public class RoundRobinSlotsAssignStrategy implements SlotsAssignStrategy {

  @Override
  public Map<WorkerInfo, List<UsableDiskInfo>> computeSlotBudgets(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      int availableStorageTypes) {
    Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets = new HashMap<>();
    for (WorkerInfo worker : workers) {
      List<UsableDiskInfo> usableDisks =
          slotBudgets.computeIfAbsent(worker, v -> new ArrayList<>());
      for (DiskInfo diskInfo : worker.diskInfos().values()) {
        if (DiskStatus.HEALTHY.equals(diskInfo.status())
            && StorageInfo.isAvailable(diskInfo.storageType(), availableStorageTypes)) {
          usableDisks.add(new UsableDiskInfo(diskInfo));
        }
      }
    }
    return slotBudgets;
  }
}
