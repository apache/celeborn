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

import java.util.List;
import java.util.Map;

import org.apache.celeborn.common.meta.WorkerInfo;

/**
 * Computes per-disk slot budgets for {@link SlotsAllocator}. The allocator owns worker placement,
 * replication, rack awareness, interruption awareness, and best-effort placement after the budgets
 * are exhausted.
 */
public interface SlotsAssignStrategy {

  /**
   * Returns a fresh, mutable budget map for one allocation request. The allocator consumes the
   * returned budgets while placing partitions.
   */
  Map<WorkerInfo, List<UsableDiskInfo>> computeSlotBudgets(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      int availableStorageTypes);
}
