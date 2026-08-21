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

import static org.apache.celeborn.common.protocol.StorageInfo.Type.*;

import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class SlotsAllocator {
  private static final Logger logger = LoggerFactory.getLogger(SlotsAllocator.class);
  private static final Random rand = new Random();

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      offerSlots(
          List<WorkerInfo> workers,
          List<Integer> partitionIds,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes,
          boolean interruptionAware,
          int interruptionAwareThreshold,
          SlotsAssignStrategy strategy) {
    if (partitionIds.isEmpty()) {
      return new HashMap<>();
    }
    if (workers.size() < 2 && shouldReplicate) {
      return new HashMap<>();
    }
    Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets =
        strategy.computeSlotBudgets(workers, partitionIds, shouldReplicate, availableStorageTypes);
    return locateSlots(
        partitionIds,
        workers,
        slotBudgets,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        interruptionAware,
        interruptionAwareThreshold);
  }

  static StorageInfo buildStorageInfo(
      WorkerInfo selectedWorker,
      Map<WorkerInfo, List<UsableDiskInfo>> budgets,
      Map<WorkerInfo, Integer> workerDiskIndex,
      int availableStorageTypes) {
    StorageInfo storageInfo;
    if (budgets != null) {
      List<UsableDiskInfo> usableDiskInfos = budgets.get(selectedWorker);
      int diskIndex =
          workerDiskIndex.computeIfAbsent(
              selectedWorker, v -> rand.nextInt(usableDiskInfos.size()));
      while (usableDiskInfos.get(diskIndex).usableSlots <= 0) {
        diskIndex = (diskIndex + 1) % usableDiskInfos.size();
      }
      usableDiskInfos.get(diskIndex).usableSlots--;
      DiskInfo selectedDiskInfo = usableDiskInfos.get(diskIndex).diskInfo;
      if (selectedDiskInfo.storageType().isDFS()) {
        storageInfo = new StorageInfo("", selectedDiskInfo.storageType(), availableStorageTypes);
      } else {
        storageInfo =
            new StorageInfo(
                selectedDiskInfo.mountPoint(),
                selectedDiskInfo.storageType(),
                availableStorageTypes);
        workerDiskIndex.put(selectedWorker, (diskIndex + 1) % usableDiskInfos.size());
      }
    } else {
      if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
        DiskInfo[] diskInfos =
            selectedWorker.diskInfos().values().stream()
                .filter(p -> !p.storageType().isDFS())
                .collect(Collectors.toList())
                .toArray(new DiskInfo[0]);
        int diskIndex =
            workerDiskIndex.computeIfAbsent(selectedWorker, v -> rand.nextInt(diskInfos.length));
        storageInfo =
            new StorageInfo(
                diskInfos[diskIndex].mountPoint(),
                diskInfos[diskIndex].storageType(),
                availableStorageTypes);
        workerDiskIndex.put(selectedWorker, (diskIndex + 1) % diskInfos.length);
      } else if (StorageInfo.S3Available(availableStorageTypes)) {
        storageInfo = new StorageInfo("", S3, availableStorageTypes);
      } else if (StorageInfo.OSSAvailable(availableStorageTypes)) {
        storageInfo = new StorageInfo("", OSS, availableStorageTypes);
      } else if (StorageInfo.HDFSAvailable(availableStorageTypes)) {
        storageInfo = new StorageInfo("", HDFS, availableStorageTypes);
      } else if (StorageInfo.memoryAvailable(availableStorageTypes)) {
        storageInfo = new StorageInfo("", StorageInfo.Type.MEMORY, availableStorageTypes);
      } else {
        throw new IllegalStateException("no storage type available");
      }
    }
    return storageInfo;
  }

  /**
   * If interruptionAware = true, select workers based on 2 main criteria: <br>
   * 1. Workers that have no nextInterruptionNotice are the first priority and are included in the
   * 1st pass for slot selection. <br>
   * 2. Workers that have a later interruption notice are a little less deprioritized, and are
   * included in the 2nd pass for slot selection. This is determined by nextInterruptionNotice above
   * a certain percentage threshold.<br>
   * All other workers are considered least priority, and are only included for slot selection in
   * the worst case. <br>
   */
  static Tuple3<List<WorkerInfo>, List<WorkerInfo>, List<WorkerInfo>>
      prioritizeWorkersBasedOnInterruptionNotice(
          List<WorkerInfo> workers,
          boolean shouldReplicate,
          boolean shouldRackAware,
          double percentileThreshold) {
    Map<Boolean, List<WorkerInfo>> partitioned =
        workers.stream().collect(Collectors.partitioningBy(WorkerInfo::hasInterruptionNotice));
    List<WorkerInfo> workersWithInterruptions = partitioned.get(true);
    List<WorkerInfo> workersWithoutInterruptions = partitioned.get(false);
    // Timestamps towards the boundary of `percentileThreshold` might be the same. Given this
    // is a stable sort, it makes sense to randomize these hosts so that the same hosts are not
    // consistently selected.
    Collections.shuffle(workersWithInterruptions);
    workersWithInterruptions.sort(
        Comparator.comparingLong(WorkerInfo::nextInterruptionNotice).reversed());
    int requiredNodes =
        (int) Math.floor((percentileThreshold * workersWithInterruptions.size()) / 100.0);

    List<WorkerInfo> workersWithLateInterruptions =
        new ArrayList<>(workersWithInterruptions.subList(0, requiredNodes));
    List<WorkerInfo> workersWithEarlyInterruptions =
        new ArrayList<>(
            workersWithInterruptions.subList(requiredNodes, workersWithInterruptions.size()));
    if (shouldReplicate && shouldRackAware) {
      return Tuple3.apply(
          generateRackAwareWorkers(workersWithoutInterruptions),
          Collections.unmodifiableList(workersWithLateInterruptions),
          generateRackAwareWorkers(workersWithEarlyInterruptions));
    }
    return Tuple3.apply(
        Collections.unmodifiableList(workersWithoutInterruptions),
        Collections.unmodifiableList(workersWithLateInterruptions),
        Collections.unmodifiableList(workersWithEarlyInterruptions));
  }

  /**
   * Allocates slots by progressively relaxing soft constraints: strategy budgets, interruption
   * preference, and finally rack awareness.
   */
  private static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      locateSlots(
          List<Integer> partitionIds,
          List<WorkerInfo> workersList,
          Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes,
          boolean interruptionAware,
          int interruptionAwareThreshold) {

    List<WorkerInfo> budgetWorkers = new ArrayList<>(slotBudgets.keySet());
    List<WorkerInfo> allWorkers = workersList;
    if (shouldReplicate && shouldRackAware) {
      budgetWorkers = generateRackAwareWorkers(budgetWorkers);
      allWorkers = generateRackAwareWorkers(allWorkers);
    }

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();
    List<WorkerInfo> preferredWorkers;
    List<WorkerInfo> workersWithLateInterruptions = Collections.emptyList();
    List<WorkerInfo> workersWithEarlyInterruptions = Collections.emptyList();
    if (interruptionAware) {
      Tuple3<List<WorkerInfo>, List<WorkerInfo>, List<WorkerInfo>>
          workersBasedOnInterruptionNotice =
              prioritizeWorkersBasedOnInterruptionNotice(
                  budgetWorkers, shouldReplicate, shouldRackAware, interruptionAwareThreshold);
      preferredWorkers = workersBasedOnInterruptionNotice._1();
      workersWithLateInterruptions = workersBasedOnInterruptionNotice._2();
      workersWithEarlyInterruptions = workersBasedOnInterruptionNotice._3();
    } else {
      preferredWorkers = budgetWorkers;
    }

    // First honor the budgets computed by the selected strategy.
    List<Integer> remainingPartitionIds =
        tryAllocateWithinBudgets(
            slots,
            partitionIds,
            preferredWorkers,
            slotBudgets,
            shouldReplicate,
            shouldRackAware,
            availableStorageTypes);
    logger.debug(
        "Remaining number of partitionIds after budget-constrained allocation: {}",
        remainingPartitionIds.size());

    // Relax strategy budgets while retaining the interruption-aware worker preference.
    if (interruptionAware && !remainingPartitionIds.isEmpty()) {
      remainingPartitionIds =
          tryAllocateInterruptionAware(
              slots,
              remainingPartitionIds,
              preferredWorkers,
              workersWithLateInterruptions,
              workersWithEarlyInterruptions,
              shouldReplicate,
              shouldRackAware,
              availableStorageTypes);
      logger.debug(
          "Remaining number of partitionIds after interruption-aware fallback: {}",
          remainingPartitionIds.size());
    }

    // Budgets and interruption preference are soft constraints. Use every worker if necessary.
    if (!remainingPartitionIds.isEmpty()) {
      remainingPartitionIds =
          tryAllocateBestEffort(
              slots,
              remainingPartitionIds,
              allWorkers,
              shouldReplicate,
              shouldRackAware,
              availableStorageTypes);
      logger.debug(
          "Remaining number of partitionIds after best-effort allocation: {}",
          remainingPartitionIds.size());
    }

    // Rack awareness is also a soft constraint and is relaxed only as the final fallback.
    if (shouldReplicate && shouldRackAware && !remainingPartitionIds.isEmpty()) {
      remainingPartitionIds =
          tryAllocateBestEffort(
              slots,
              remainingPartitionIds,
              allWorkers,
              shouldReplicate,
              false,
              availableStorageTypes);
      logger.debug(
          "Remaining number of partitionIds after disabling rack-aware placement: {}",
          remainingPartitionIds.size());
    }
    return slots;
  }

  private static List<Integer> tryAllocateWithinBudgets(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> workers,
      Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes) {
    return tryAllocateSlots(
        slots,
        partitionIds,
        workers,
        workers,
        slotBudgets,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        true);
  }

  private static List<Integer> tryAllocateInterruptionAware(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> preferredWorkers,
      List<WorkerInfo> workersWithLateInterruptions,
      List<WorkerInfo> workersWithEarlyInterruptions,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes) {
    List<WorkerInfo> primaryWorkerCandidates = new ArrayList<>(preferredWorkers);
    primaryWorkerCandidates.addAll(workersWithLateInterruptions);
    if (shouldReplicate && shouldRackAware) {
      primaryWorkerCandidates = generateRackAwareWorkers(primaryWorkerCandidates);
    }
    return tryAllocateSlots(
        slots,
        partitionIds,
        primaryWorkerCandidates,
        workersWithEarlyInterruptions,
        null,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        false);
  }

  private static List<Integer> tryAllocateBestEffort(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> workers,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes) {
    return tryAllocateSlots(
        slots,
        partitionIds,
        workers,
        workers,
        null,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        true);
  }

  /**
   * The rack distribution of the input workers list is essentially random, and in degenerate cases
   * the rack aware slot selection ends up skipping sub list's of hosts (in same rack) - which
   * results in uneven distribution of replica selection. For example given worker list: [h1r1,
   * h2r1, h3r1, h4r2, h5r2, h6r2] if primary is h3r1 and replica index is pointing to h1r1, it will
   * skip both h2r1 and h3r1 in order to pick h4r2; and for the next slot, primary will be h4r2 and
   * will skip all the r2 hosts in order to pick h1r1. This ends up being suboptimal where some
   * hosts are picked a lot more than others (due to the worker and worker/rack distribution). In
   * order to mitigate this, we reorder the worker list by redistributing the workers based on rack
   * to increase the rack diversity between adjoining workers, so that we minimize skipping over
   * consecutive hosts.
   */
  static List<WorkerInfo> generateRackAwareWorkers(List<WorkerInfo> workers) {

    List<Map.Entry<String, LinkedList<WorkerInfo>>> sortedRackToHosts;
    {
      Map<String, LinkedList<WorkerInfo>> map = new HashMap<>();
      for (WorkerInfo worker : workers) {
        map.computeIfAbsent(worker.networkLocation(), key -> new LinkedList<>()).add(worker);
      }
      sortedRackToHosts = new ArrayList<>(map.entrySet());
      // reverse sort by number of hosts per rack
      sortedRackToHosts.sort(
          (o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()));
    }

    ArrayList<WorkerInfo> result = new ArrayList<>(workers.size());
    int count = 0;
    final int numWorkers = workers.size();
    while (count < numWorkers) {
      Iterator<Map.Entry<String, LinkedList<WorkerInfo>>> iter = sortedRackToHosts.iterator();
      while (iter.hasNext()) {
        LinkedList<WorkerInfo> workerList = iter.next().getValue();
        result.add(workerList.removeFirst());
        count++;
        if (workerList.isEmpty()) {
          iter.remove();
        }
      }
    }

    return Collections.unmodifiableList(result);
  }

  /**
   * Attempts to allocate the given partitions by scanning primary and replica worker candidates in
   * round-robin order.
   *
   * @param slots the slots that have been assigned for each partitionId
   * @param partitionIds the partitionIds that require slot selection still
   * @param primaryWorkers list of worker candidates that can be used for primary workers.
   * @param replicaWorkers list of worker candidates that can be used for replica workers.
   * @param slotBudgets budgets for each available slot based on worker characteristics
   * @param shouldReplicate if replication is enabled within the cluster
   * @param shouldRackAware if rack-aware replication is enabled within the cluster.
   * @param availableStorageTypes available storage types coming from the offer slots request.
   * @param sameWorkerCandidates whether primary and replica indexes refer to the same worker
   *     candidate list, in which case equal indexes identify the same worker
   * @return the partitionIds that were not able to be assigned slots in this iteration with the
   *     current primary and replica worker candidates and slot budgets.
   */
  private static List<Integer> tryAllocateSlots(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> primaryWorkers,
      List<WorkerInfo> replicaWorkers,
      Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes,
      boolean sameWorkerCandidates) {
    if (primaryWorkers.isEmpty() || (shouldReplicate && replicaWorkers.isEmpty())) {
      return partitionIds;
    }
    // Tracks the next disk to try for each worker during this allocation pass.
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    List<Integer> remainingPartitionIds = new LinkedList<>(partitionIds);

    int primaryIndex = rand.nextInt(primaryWorkers.size());
    int replicaIndex = shouldReplicate ? rand.nextInt(replicaWorkers.size()) : -1;

    // Preserve the original allocation order while keeping removal from the linked list O(1).
    ListIterator<Integer> partitionIterator =
        remainingPartitionIds.listIterator(remainingPartitionIds.size());
    while (partitionIterator.hasPrevious()) {
      int partitionId = partitionIterator.previous();
      int selectedPrimaryIndex =
          findNextWorkerIndex(
              primaryWorkers.size(),
              primaryIndex,
              index -> canAssign(slotBudgets, primaryWorkers.get(index), availableStorageTypes));
      if (selectedPrimaryIndex < 0) {
        break;
      }
      WorkerInfo primaryWorker = primaryWorkers.get(selectedPrimaryIndex);

      StorageInfo primaryStorageInfo =
          buildStorageInfo(primaryWorker, slotBudgets, workerDiskIndex, availableStorageTypes);
      PartitionLocation primaryPartition =
          createLocation(
              partitionId, primaryWorker, null, primaryStorageInfo, PartitionLocation.Mode.PRIMARY);

      if (shouldReplicate) {
        int selectedReplicaIndex;
        if (slotBudgets != null) {
          selectedReplicaIndex =
              findNextWorkerIndex(
                  replicaWorkers.size(),
                  replicaIndex,
                  index ->
                      !(sameWorkerCandidates && index == selectedPrimaryIndex)
                          && haveUsableSlots(slotBudgets, replicaWorkers.get(index))
                          && satisfyRackAware(
                              shouldRackAware, primaryWorker, replicaWorkers.get(index)));
        } else if (shouldRackAware) {
          selectedReplicaIndex =
              findNextWorkerIndex(
                  replicaWorkers.size(),
                  replicaIndex,
                  index ->
                      !(sameWorkerCandidates && index == selectedPrimaryIndex)
                          && canAssign(null, replicaWorkers.get(index), availableStorageTypes)
                          && satisfyRackAware(true, primaryWorker, replicaWorkers.get(index)));
        } else if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
          selectedReplicaIndex =
              findNextWorkerIndex(
                  replicaWorkers.size(),
                  replicaIndex,
                  index ->
                      !(sameWorkerCandidates && index == selectedPrimaryIndex)
                          && replicaWorkers.get(index).haveDisk());
        } else {
          selectedReplicaIndex = replicaIndex;
        }
        if (selectedReplicaIndex < 0) {
          break;
        }
        WorkerInfo replicaWorker = replicaWorkers.get(selectedReplicaIndex);

        StorageInfo replicaStorageInfo =
            buildStorageInfo(replicaWorker, slotBudgets, workerDiskIndex, availableStorageTypes);
        PartitionLocation replicaPartition =
            createLocation(
                partitionId,
                replicaWorker,
                primaryPartition,
                replicaStorageInfo,
                PartitionLocation.Mode.REPLICA);
        primaryPartition.setPeer(replicaPartition);
        addLocation(slots, replicaWorker, replicaPartition);
        replicaIndex = (selectedReplicaIndex + 1) % replicaWorkers.size();
      }

      addLocation(slots, primaryWorker, primaryPartition);
      primaryIndex = (selectedPrimaryIndex + 1) % primaryWorkers.size();
      partitionIterator.remove();
    }
    return remainingPartitionIds;
  }

  private static int findNextWorkerIndex(int workerCount, int startIndex, IntPredicate isEligible) {
    int workerIndex = startIndex;
    do {
      if (isEligible.test(workerIndex)) {
        return workerIndex;
      }
      workerIndex = (workerIndex + 1) % workerCount;
    } while (workerIndex != startIndex);
    return -1;
  }

  private static boolean canAssign(
      Map<WorkerInfo, List<UsableDiskInfo>> slotBudgets,
      WorkerInfo worker,
      int availableStorageTypes) {
    if (slotBudgets != null) {
      return haveUsableSlots(slotBudgets, worker);
    }
    return !StorageInfo.localDiskAvailable(availableStorageTypes) || worker.haveDisk();
  }

  private static boolean haveUsableSlots(
      Map<WorkerInfo, List<UsableDiskInfo>> budgets, WorkerInfo worker) {
    List<UsableDiskInfo> usableDiskInfos = budgets.get(worker);
    return usableDiskInfos != null
        && usableDiskInfos.stream().anyMatch(disk -> disk.usableSlots > 0);
  }

  private static boolean satisfyRackAware(
      boolean shouldRackAware, WorkerInfo primaryWorker, WorkerInfo replicaWorker) {
    return !shouldRackAware
        || !Objects.equals(primaryWorker.networkLocation(), replicaWorker.networkLocation());
  }

  private static PartitionLocation createLocation(
      int partitionIndex,
      WorkerInfo workerInfo,
      PartitionLocation peer,
      StorageInfo storageInfo,
      PartitionLocation.Mode mode) {
    return new PartitionLocation(
        partitionIndex,
        0,
        workerInfo.host(),
        workerInfo.rpcPort(),
        workerInfo.pushPort(),
        workerInfo.fetchPort(),
        workerInfo.replicatePort(),
        mode,
        peer,
        storageInfo,
        new RoaringBitmap());
  }

  private static void addLocation(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      WorkerInfo worker,
      PartitionLocation location) {
    Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
        slots.computeIfAbsent(
            worker, ignored -> new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
    if (location.getMode() == PartitionLocation.Mode.PRIMARY) {
      locations._1.add(location);
    } else {
      locations._2.add(location);
    }
  }

  public static Map<WorkerInfo, Map<String, Integer>> slotsToDiskAllocations(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
    Iterator<WorkerInfo> workers = slots.keySet().iterator();
    Map<WorkerInfo, Map<String, Integer>> workerToSlots = new HashMap<>();
    while (workers.hasNext()) {
      WorkerInfo worker = workers.next();
      Map<String, Integer> slotsPerDisk =
          workerToSlots.computeIfAbsent(worker, v -> new HashMap<>());
      List<PartitionLocation> jointLocations = new ArrayList<>();
      jointLocations.addAll(slots.get(worker)._1);
      jointLocations.addAll(slots.get(worker)._2);
      for (PartitionLocation location : jointLocations) {
        String mountPoint = location.getStorageInfo().getMountPoint();
        // skip non local disks slots
        if (!mountPoint.isEmpty()) {
          if (slotsPerDisk.containsKey(mountPoint)) {
            slotsPerDisk.put(mountPoint, slotsPerDisk.get(mountPoint) + 1);
          } else {
            slotsPerDisk.put(mountPoint, 1);
          }
        }
      }
    }
    return workerToSlots;
  }
}
