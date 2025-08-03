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

import java.util.*;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class SlotsAllocator {
  static class UsableDiskInfo {
    DiskInfo diskInfo;
    long usableSlots;

    UsableDiskInfo(DiskInfo diskInfo, long usableSlots) {
      this.diskInfo = diskInfo;
      this.usableSlots = usableSlots;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(SlotsAllocator.class);
  private static final Random rand = new Random();
  private static boolean initialized = false;
  private static double[] taskAllocationRatio = null;

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      offerSlotsRoundRobin(
          List<WorkerInfo> workers,
          List<Integer> partitionIds,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes,
          boolean interruptionAware,
          int interruptionAwareThreshold) {
    if (partitionIds.isEmpty()) {
      return new HashMap<>();
    }
    if (workers.size() < 2 && shouldReplicate) {
      return new HashMap<>();
    }
    Map<WorkerInfo, List<UsableDiskInfo>> slotsRestrictions = new HashMap<>();
    for (WorkerInfo worker : workers) {
      List<UsableDiskInfo> usableDisks =
          slotsRestrictions.computeIfAbsent(worker, v -> new ArrayList<>());
      for (Map.Entry<String, DiskInfo> diskInfoEntry : worker.diskInfos().entrySet()) {
        if (diskInfoEntry.getValue().status().equals(DiskStatus.HEALTHY)) {
          if (StorageInfo.localDiskAvailable(availableStorageTypes)
              && diskInfoEntry.getValue().storageType() != StorageInfo.Type.HDFS
              && diskInfoEntry.getValue().storageType() != StorageInfo.Type.S3
              && diskInfoEntry.getValue().storageType() != StorageInfo.Type.OSS) {
            usableDisks.add(
                new UsableDiskInfo(
                    diskInfoEntry.getValue(), diskInfoEntry.getValue().getAvailableSlots()));
          } else if (StorageInfo.HDFSAvailable(availableStorageTypes)
              && diskInfoEntry.getValue().storageType() == StorageInfo.Type.HDFS) {
            usableDisks.add(
                new UsableDiskInfo(
                    diskInfoEntry.getValue(), diskInfoEntry.getValue().getAvailableSlots()));
          } else if (StorageInfo.S3Available(availableStorageTypes)
              && diskInfoEntry.getValue().storageType() == StorageInfo.Type.S3) {
            usableDisks.add(
                new UsableDiskInfo(
                    diskInfoEntry.getValue(), diskInfoEntry.getValue().getAvailableSlots()));
          } else if (StorageInfo.OSSAvailable(availableStorageTypes)
              && diskInfoEntry.getValue().storageType() == StorageInfo.Type.OSS) {
            usableDisks.add(
                new UsableDiskInfo(
                    diskInfoEntry.getValue(), diskInfoEntry.getValue().availableSlots()));
          }
        }
      }
    }
    return locateSlots(
        partitionIds,
        workers,
        slotsRestrictions,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        interruptionAware,
        interruptionAwareThreshold);
  }

  /**
   * It assumes that all disks whose available space is greater than the minimum space are divided
   * into multiple groups. A faster group will allocate more allocations than a slower group by
   * diskGroupGradient.
   */
  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      offerSlotsLoadAware(
          List<WorkerInfo> workers,
          List<Integer> partitionIds,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int diskGroupCount,
          double diskGroupGradient,
          double flushTimeWeight,
          double fetchTimeWeight,
          int availableStorageTypes,
          boolean interruptionAware,
          int interruptionAwareThreshold) {
    if (partitionIds.isEmpty()) {
      return new HashMap<>();
    }
    if (workers.size() < 2 && shouldReplicate) {
      return new HashMap<>();
    }
    if (StorageInfo.HDFSOnly(availableStorageTypes)
        || StorageInfo.S3Only(availableStorageTypes)
        || StorageInfo.OSSOnly(availableStorageTypes)) {
      return offerSlotsRoundRobin(
          workers,
          partitionIds,
          shouldReplicate,
          shouldRackAware,
          availableStorageTypes,
          interruptionAware,
          interruptionAwareThreshold);
    }

    List<DiskInfo> usableDisks = new ArrayList<>();
    Map<DiskInfo, WorkerInfo> diskToWorkerMap = new HashMap<>();

    workers.forEach(
        i ->
            i.diskInfos()
                .forEach(
                    (key, diskInfo) -> {
                      diskToWorkerMap.put(diskInfo, i);
                      if (diskInfo.actualUsableSpace() > 0
                          && diskInfo.status().equals(DiskStatus.HEALTHY)
                          && diskInfo.storageType() != StorageInfo.Type.HDFS
                          && diskInfo.storageType() != StorageInfo.Type.S3
                          && diskInfo.storageType() != StorageInfo.Type.OSS) {
                        usableDisks.add(diskInfo);
                      }
                    }));

    boolean noUsableDisks =
        usableDisks.isEmpty()
            || (shouldReplicate
                && (usableDisks.size() == 1
                    || usableDisks.stream().map(diskToWorkerMap::get).distinct().count() <= 1));
    boolean noAvailableSlots =
        usableDisks.stream().mapToLong(DiskInfo::getAvailableSlots).sum() <= 0;

    if (noUsableDisks || noAvailableSlots) {
      logger.warn(
          "offer slots for {} fallback to roundrobin because there is no {}",
          StringUtils.join(partitionIds, ','),
          noUsableDisks ? "usable disks" : "available slots");
      return offerSlotsRoundRobin(
          workers,
          partitionIds,
          shouldReplicate,
          shouldRackAware,
          availableStorageTypes,
          interruptionAware,
          interruptionAwareThreshold);
    }

    if (!initialized) {
      initLoadAwareAlgorithm(diskGroupCount, diskGroupGradient);
    }

    Map<WorkerInfo, List<UsableDiskInfo>> slotsRestrictions =
        getSlotsRestrictionsByLoadAwareAlgorithm(
            placeDisksToGroups(usableDisks, diskGroupCount, flushTimeWeight, fetchTimeWeight),
            diskToWorkerMap,
            shouldReplicate ? partitionIds.size() * 2 : partitionIds.size());
    return locateSlots(
        partitionIds,
        workers,
        slotsRestrictions,
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes,
        interruptionAware,
        interruptionAwareThreshold);
  }

  private static StorageInfo getStorageInfo(
      List<WorkerInfo> workers,
      int workerIndex,
      Map<WorkerInfo, List<UsableDiskInfo>> restrictions,
      Map<WorkerInfo, Integer> workerDiskIndex,
      int availableStorageTypes) {
    WorkerInfo selectedWorker = workers.get(workerIndex);
    StorageInfo storageInfo;
    if (restrictions != null) {
      List<UsableDiskInfo> usableDiskInfos = restrictions.get(selectedWorker);
      int diskIndex =
          workerDiskIndex.computeIfAbsent(
              selectedWorker, v -> rand.nextInt(usableDiskInfos.size()));
      while (usableDiskInfos.get(diskIndex).usableSlots <= 0) {
        diskIndex = (diskIndex + 1) % usableDiskInfos.size();
      }
      usableDiskInfos.get(diskIndex).usableSlots--;
      DiskInfo selectedDiskInfo = usableDiskInfos.get(diskIndex).diskInfo;
      if (selectedDiskInfo.storageType() == StorageInfo.Type.HDFS) {
        storageInfo = new StorageInfo("", StorageInfo.Type.HDFS, availableStorageTypes);
      } else if (selectedDiskInfo.storageType() == StorageInfo.Type.S3) {
        storageInfo = new StorageInfo("", StorageInfo.Type.S3, availableStorageTypes);
      } else if (selectedDiskInfo.storageType() == StorageInfo.Type.OSS) {
        storageInfo = new StorageInfo("", StorageInfo.Type.OSS, availableStorageTypes);
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
                .filter(p -> p.storageType() != StorageInfo.Type.HDFS)
                .filter(p -> p.storageType() != StorageInfo.Type.S3)
                .filter(p -> p.storageType() != StorageInfo.Type.OSS)
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
        storageInfo = new StorageInfo("", StorageInfo.Type.S3, availableStorageTypes);
      } else if (StorageInfo.OSSAvailable(availableStorageTypes)) {
        storageInfo = new StorageInfo("", StorageInfo.Type.OSS, availableStorageTypes);
      } else {
        storageInfo = new StorageInfo("", StorageInfo.Type.HDFS, availableStorageTypes);
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
   * Progressive locate slots for all partitions <br>
   * 1. try to allocate for all partitions under restrictions, on workers with no interruption
   * notice if interruptionAware = true. <br>
   * 2. try to allocate for all partitions, and attempt the replica selection to be
   * interruptionAware if interruptionAware = true <br>
   * 3. allocate remain partitions to all workers <br>
   * 4. allocate remain partitions to all workers again without considering rack aware <br>
   */
  private static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      locateSlots(
          List<Integer> partitionIds,
          List<WorkerInfo> workersList,
          Map<WorkerInfo, List<UsableDiskInfo>> slotRestrictions,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes,
          boolean interruptionAware,
          int interruptionAwareThreshold) {

    List<WorkerInfo> workersFromSlotRestrictions = new ArrayList<>(slotRestrictions.keySet());
    List<WorkerInfo> workers = workersList;
    if (shouldReplicate && shouldRackAware) {
      workersFromSlotRestrictions = generateRackAwareWorkers(workersFromSlotRestrictions);
      workers = generateRackAwareWorkers(workers);
    }

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();
    List<WorkerInfo> workersWithoutInterruptions;
    List<WorkerInfo> workersWithLateInterruptions;
    List<WorkerInfo> workersWithEarlyInterruptions;
    if (interruptionAware) {
      Tuple3<List<WorkerInfo>, List<WorkerInfo>, List<WorkerInfo>>
          workersBasedOnInterruptionNotice =
              prioritizeWorkersBasedOnInterruptionNotice(
                  workersFromSlotRestrictions,
                  shouldReplicate,
                  shouldRackAware,
                  interruptionAwareThreshold);
      workersWithoutInterruptions = workersBasedOnInterruptionNotice._1();
      workersWithLateInterruptions = workersBasedOnInterruptionNotice._2();
      workersWithEarlyInterruptions = workersBasedOnInterruptionNotice._3();
    } else {
      workersWithoutInterruptions = workersFromSlotRestrictions;
      workersWithLateInterruptions = null;
      workersWithEarlyInterruptions = null;
    }
    // In the first pass, we try to place all partitions (primary and replica) from
    // `workersWithoutInterruptions`.
    List<Integer> remain =
        roundRobin(
            slots,
            partitionIds,
            workersWithoutInterruptions,
            workersWithoutInterruptions,
            slotRestrictions,
            shouldReplicate,
            shouldRackAware,
            availableStorageTypes,
            true);
    logger.debug(
        "Remaining number of partitionIds after 1st pass slot selection: {}", remain.size());
    // Do an extra pass for partition placement if interruptionAware = true, to see if we can
    // assign the remaining partitions with slot restriction still set in place. The goal during
    // this pass
    // is to see if we can place primary from `workersWithoutInterruptions +
    // workersWithLateInterruptions`, while replica can be in
    // `workersWithEarlyInterruptions`.
    // This is to avoid the degenerate case in which both primary and replica may end up in
    // `workersWithEarlyInterruptions`.
    if (interruptionAware && !remain.isEmpty()) {
      List<WorkerInfo> primaryWorkerCandidates = new ArrayList<>(workersWithoutInterruptions);
      primaryWorkerCandidates.addAll(workersWithLateInterruptions);
      if (shouldReplicate && shouldRackAware) {
        primaryWorkerCandidates = generateRackAwareWorkers(primaryWorkerCandidates);
      }
      remain =
          roundRobin(
              slots,
              remain,
              primaryWorkerCandidates,
              workersWithEarlyInterruptions,
              null,
              shouldReplicate,
              shouldRackAware,
              availableStorageTypes,
              false);
      logger.debug(
          "Remaining number of partitionIds after 2nd pass slot selection: {}", remain.size());
    }
    // If partitions are remaining from this point on, and interruptionAware = true, then
    // this becomes the degenerate case where both primary and replica are likely chosen on
    // workers with interruptions that are sooner.
    if (!remain.isEmpty()) {
      remain =
          roundRobin(
              slots,
              remain,
              workers,
              workers,
              null,
              shouldReplicate,
              shouldRackAware,
              availableStorageTypes,
              true);
      logger.debug(
          "Remaining number of partitionIds after 3rd pass slot selection: {}", remain.size());
    }
    if (!remain.isEmpty()) {
      roundRobin(
          slots,
          remain,
          workers,
          workers,
          null,
          shouldReplicate,
          false,
          availableStorageTypes,
          true);
    }
    return slots;
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
   * Assigns slots in a roundrobin fashion given lists of primary and replica worker candidates and
   * other restrictions.
   *
   * @param slots the slots that have been assigned for each partitionId
   * @param partitionIds the partitionIds that require slot selection still
   * @param primaryWorkers list of worker candidates that can be used for primary workers.
   * @param replicaWorkers list of worker candidates that can be used for replica workers.
   * @param slotsRestrictions restrictions for each available slot based on worker characteristics
   * @param shouldReplicate if replication is enabled within the cluster
   * @param shouldRackAware if rack-aware replication is enabled within the cluster.
   * @param availableStorageTypes available storage types coming from the offer slots request.
   * @param skipLocationsOnSameWorkerCheck if the worker candidates list for primaries and replicas
   *     is the same. This is to prevent index mismatch while assigning slots across both lists.
   * @return the partitionIds that were not able to be assigned slots in this iteration with the
   *     current primary and replica worker candidates and slot restrictions.
   */
  private static List<Integer> roundRobin(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> primaryWorkers,
      List<WorkerInfo> replicaWorkers,
      Map<WorkerInfo, List<UsableDiskInfo>> slotsRestrictions,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes,
      boolean skipLocationsOnSameWorkerCheck) {
    if (primaryWorkers.isEmpty() || (shouldReplicate && replicaWorkers.isEmpty())) {
      return partitionIds;
    }
    // workerInfo -> (diskIndexForPrimaryAndReplica)
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    List<Integer> partitionIdList = new LinkedList<>(partitionIds);

    final int primaryWorkersSize = primaryWorkers.size();
    final int replicaWorkersSize = replicaWorkers.size();
    final IntUnaryOperator primaryWorkersIncrementIndex = v -> (v + 1) % primaryWorkersSize;
    int primaryIndex = rand.nextInt(primaryWorkersSize);
    final IntUnaryOperator replicaWorkersIncrementIndex;
    int replicaIndex;
    if (shouldReplicate) {
      replicaWorkersIncrementIndex = v -> (v + 1) % replicaWorkersSize;
      replicaIndex = rand.nextInt(replicaWorkersSize);
    } else {
      replicaWorkersIncrementIndex = null;
      replicaIndex = -1;
    }

    ListIterator<Integer> iter = partitionIdList.listIterator(partitionIdList.size());
    // Iterate from the end to preserve O(1) removal of processed partitions.
    // This is important when we have a high number of concurrent apps that have a
    // high number of partitions.
    outer:
    while (iter.hasPrevious()) {
      int nextPrimaryInd = primaryIndex;

      int partitionId = iter.previous();
      StorageInfo storageInfo;
      if (slotsRestrictions != null && !slotsRestrictions.isEmpty()) {
        // this means that we'll select a mount point
        while (!haveUsableSlots(slotsRestrictions, primaryWorkers, nextPrimaryInd)) {
          nextPrimaryInd = primaryWorkersIncrementIndex.applyAsInt(nextPrimaryInd);
          if (nextPrimaryInd == primaryIndex) {
            break outer;
          }
        }
        storageInfo =
            getStorageInfo(
                primaryWorkers,
                nextPrimaryInd,
                slotsRestrictions,
                workerDiskIndex,
                availableStorageTypes);
      } else {
        if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
          while (!primaryWorkers.get(nextPrimaryInd).haveDisk()) {
            nextPrimaryInd = primaryWorkersIncrementIndex.applyAsInt(nextPrimaryInd);
            if (nextPrimaryInd == primaryIndex) {
              break outer;
            }
          }
        }
        storageInfo =
            getStorageInfo(
                primaryWorkers, nextPrimaryInd, null, workerDiskIndex, availableStorageTypes);
      }
      PartitionLocation primaryPartition =
          createLocation(partitionId, primaryWorkers.get(nextPrimaryInd), null, storageInfo, true);

      if (shouldReplicate) {
        int nextReplicaInd = replicaIndex;
        if (slotsRestrictions != null) {
          while ((nextReplicaInd == nextPrimaryInd && skipLocationsOnSameWorkerCheck)
              || !haveUsableSlots(slotsRestrictions, replicaWorkers, nextReplicaInd)
              || !satisfyRackAware(
                  shouldRackAware,
                  primaryWorkers,
                  nextPrimaryInd,
                  replicaWorkers,
                  nextReplicaInd)) {
            nextReplicaInd = replicaWorkersIncrementIndex.applyAsInt(nextReplicaInd);
            if (nextReplicaInd == replicaIndex) {
              break outer;
            }
          }
          storageInfo =
              getStorageInfo(
                  replicaWorkers,
                  nextReplicaInd,
                  slotsRestrictions,
                  workerDiskIndex,
                  availableStorageTypes);
        } else if (shouldRackAware) {
          while ((nextReplicaInd == nextPrimaryInd && skipLocationsOnSameWorkerCheck)
              || !satisfyRackAware(
                  true, primaryWorkers, nextPrimaryInd, replicaWorkers, nextReplicaInd)) {
            nextReplicaInd = replicaWorkersIncrementIndex.applyAsInt(nextReplicaInd);
            if (nextReplicaInd == replicaIndex) {
              break outer;
            }
          }
        } else {
          if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
            while ((nextReplicaInd == nextPrimaryInd && skipLocationsOnSameWorkerCheck)
                || !replicaWorkers.get(nextReplicaInd).haveDisk()) {
              nextReplicaInd = replicaWorkersIncrementIndex.applyAsInt(nextReplicaInd);
              if (nextReplicaInd == replicaIndex) {
                break outer;
              }
            }
          }
          storageInfo =
              getStorageInfo(
                  replicaWorkers, nextReplicaInd, null, workerDiskIndex, availableStorageTypes);
        }
        PartitionLocation replicaPartition =
            createLocation(
                partitionId,
                replicaWorkers.get(nextReplicaInd),
                primaryPartition,
                storageInfo,
                false);
        primaryPartition.setPeer(replicaPartition);
        Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
            slots.computeIfAbsent(
                replicaWorkers.get(nextReplicaInd),
                v -> new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
        locations._2.add(replicaPartition);
        replicaIndex = replicaWorkersIncrementIndex.applyAsInt(nextReplicaInd);
      }

      Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
          slots.computeIfAbsent(
              primaryWorkers.get(nextPrimaryInd),
              v -> new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
      locations._1.add(primaryPartition);
      primaryIndex = primaryWorkersIncrementIndex.applyAsInt(nextPrimaryInd);
      iter.remove();
    }
    return partitionIdList;
  }

  private static boolean haveUsableSlots(
      Map<WorkerInfo, List<UsableDiskInfo>> restrictions, List<WorkerInfo> workers, int index) {
    return restrictions.get(workers.get(index)).stream().mapToLong(i -> i.usableSlots).sum() > 0;
  }

  private static boolean satisfyRackAware(
      boolean shouldRackAware,
      List<WorkerInfo> primaryWorkers,
      int primaryIndex,
      List<WorkerInfo> replicaWorkers,
      int nextReplicaInd) {
    return !shouldRackAware
        || !Objects.equals(
            primaryWorkers.get(primaryIndex).networkLocation(),
            replicaWorkers.get(nextReplicaInd).networkLocation());
  }

  private static void initLoadAwareAlgorithm(int diskGroups, double diskGroupGradient) {
    taskAllocationRatio = new double[diskGroups];
    double totalAllocations = 0;

    for (int i = 0; i < diskGroups; i++) {
      totalAllocations += Math.pow(1 + diskGroupGradient, diskGroups - 1 - i);
    }
    for (int i = 0; i < diskGroups; i++) {
      taskAllocationRatio[i] =
          Math.pow(1 + diskGroupGradient, diskGroups - 1 - i) / totalAllocations;
    }
    logger.info(
        "load-aware offer slots algorithm init with taskAllocationRatio {}",
        StringUtils.join(taskAllocationRatio, ','));
    initialized = true;
  }

  private static List<List<DiskInfo>> placeDisksToGroups(
      List<DiskInfo> usableDisks,
      int diskGroupCount,
      double flushTimeWeight,
      double fetchTimeWeight) {
    List<List<DiskInfo>> diskGroups = new ArrayList<>();
    usableDisks.sort(
        (o1, o2) -> {
          double delta =
              (o1.avgFlushTime() * flushTimeWeight + o1.avgFetchTime() * fetchTimeWeight)
                  - (o2.avgFlushTime() * flushTimeWeight + o2.avgFetchTime() * fetchTimeWeight);
          return delta < 0 ? -1 : (delta > 0 ? 1 : 0);
        });
    int diskCount = usableDisks.size();
    int startIndex = 0;
    int groupSizeSize = (int) Math.ceil(usableDisks.size() / (double) diskGroupCount);
    for (int i = 0; i < diskGroupCount; i++) {
      List<DiskInfo> diskList = new ArrayList<>();
      if (startIndex >= usableDisks.size()) {
        continue;
      }
      if (startIndex + groupSizeSize <= diskCount) {
        diskList.addAll(usableDisks.subList(startIndex, startIndex + groupSizeSize));
        startIndex += groupSizeSize;
      } else {
        diskList.addAll(usableDisks.subList(startIndex, usableDisks.size()));
        startIndex = usableDisks.size();
      }
      diskGroups.add(diskList);
    }
    return diskGroups;
  }

  /**
   * This method implement the load aware slots allocation algorithm. See details at
   * /docs/developers/slotsallocation.md
   */
  private static Map<WorkerInfo, List<UsableDiskInfo>> getSlotsRestrictionsByLoadAwareAlgorithm(
      List<List<DiskInfo>> groups, Map<DiskInfo, WorkerInfo> diskWorkerMap, int partitionCnt) {
    int groupSize = groups.size();
    long[] groupAllocations = new long[groupSize];
    Map<WorkerInfo, List<UsableDiskInfo>> restrictions = new HashMap<>();
    long[] groupAvailableSlots = new long[groupSize];
    for (int i = 0; i < groupSize; i++) {
      for (DiskInfo disk : groups.get(i)) {
        groupAvailableSlots[i] += disk.getAvailableSlots();
      }
    }
    double[] currentAllocation = new double[groupSize];
    double currentAllocationSum = 0;
    for (int i = 0; i < groupSize; i++) {
      if (!groups.get(i).isEmpty()) {
        currentAllocationSum += taskAllocationRatio[i];
      }
    }
    for (int i = 0; i < groupSize; i++) {
      if (!groups.get(i).isEmpty()) {
        currentAllocation[i] = taskAllocationRatio[i] / currentAllocationSum;
      }
    }
    long toNextGroup = 0;
    long left = partitionCnt;
    for (int i = 0; i < groupSize; i++) {
      if (left <= 0) {
        break;
      }
      long estimateAllocation = (int) Math.ceil(partitionCnt * currentAllocation[i]);
      if (estimateAllocation > left) {
        estimateAllocation = left;
      }
      if (estimateAllocation + toNextGroup > groupAvailableSlots[i]) {
        groupAllocations[i] = groupAvailableSlots[i];
        toNextGroup = estimateAllocation - groupAvailableSlots[i] + toNextGroup;
      } else {
        groupAllocations[i] = estimateAllocation + toNextGroup;
      }
      left -= groupAllocations[i];
    }

    long groupLeft = 0;
    for (int i = 0; i < groups.size(); i++) {
      int disksInsideGroup = groups.get(i).size();
      long groupRequired = groupAllocations[i] + groupLeft;
      for (DiskInfo disk : groups.get(i)) {
        if (groupRequired <= 0) {
          break;
        }
        List<UsableDiskInfo> diskAllocation =
            restrictions.computeIfAbsent(diskWorkerMap.get(disk), v -> new ArrayList<>());
        long allocated =
            (int) Math.ceil((groupAllocations[i] + groupLeft) / (double) disksInsideGroup);
        if (allocated > disk.getAvailableSlots()) {
          allocated = disk.getAvailableSlots();
        }
        if (allocated > groupRequired) {
          allocated = groupRequired;
        }
        diskAllocation.add(new UsableDiskInfo(disk, Math.toIntExact(allocated)));
        groupRequired -= allocated;
      }
      groupLeft = groupRequired;
    }

    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < groups.size(); i++) {
        sb.append("| group ").append(i).append(" ");
        for (DiskInfo diskInfo : groups.get(i)) {
          WorkerInfo workerInfo = diskWorkerMap.get(diskInfo);
          String workerHost = workerInfo.host();
          long allocation = 0;
          if (restrictions.get(workerInfo) != null) {
            for (UsableDiskInfo usableInfo : restrictions.get(workerInfo)) {
              if (usableInfo.diskInfo.equals(diskInfo)) {
                allocation = usableInfo.usableSlots;
              }
            }
          }
          sb.append(workerHost)
              .append("-")
              .append(diskInfo.mountPoint())
              .append(" flushtime:")
              .append(diskInfo.avgFlushTime())
              .append(" fetchtime:")
              .append(diskInfo.avgFetchTime())
              .append(" allocation: ")
              .append(allocation)
              .append(" ");
        }
        sb.append(" | ");
      }
      logger.debug(
          "total {} allocate with group {} with allocations {}",
          partitionCnt,
          StringUtils.join(groupAllocations, ','),
          sb);
    }
    return restrictions;
  }

  private static PartitionLocation createLocation(
      int partitionIndex,
      WorkerInfo workerInfo,
      PartitionLocation peer,
      StorageInfo storageInfo,
      boolean isPrimary) {
    return new PartitionLocation(
        partitionIndex,
        0,
        workerInfo.host(),
        workerInfo.rpcPort(),
        workerInfo.pushPort(),
        workerInfo.fetchPort(),
        workerInfo.replicatePort(),
        isPrimary ? PartitionLocation.Mode.PRIMARY : PartitionLocation.Mode.REPLICA,
        peer,
        storageInfo,
        new RoaringBitmap());
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
