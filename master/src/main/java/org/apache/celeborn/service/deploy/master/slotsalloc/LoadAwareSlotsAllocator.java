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

import java.util.*;

import scala.Tuple2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class LoadAwareSlotsAllocator extends SlotsAllocator {
  private static final Logger logger = LoggerFactory.getLogger(LoadAwareSlotsAllocator.class);

  private final int diskGroupNum;
  private final double diskGroupGradient;
  private final double flushTimeWeight;
  private final double fetchTimeWeight;

  private static final RoundRobinSlotsAllocator fallbackSlotsAllocator =
      new RoundRobinSlotsAllocator();

  private static boolean initialized = false;
  private static double[] taskAllocationRatio = null;

  public LoadAwareSlotsAllocator(CelebornConf conf) {
    diskGroupNum = conf.masterSlotAssignLoadAwareDiskGroupNum();
    diskGroupGradient = conf.masterSlotAssignLoadAwareDiskGroupGradient();
    flushTimeWeight = conf.masterSlotAssignLoadAwareFlushTimeWeight();
    fetchTimeWeight = conf.masterSlotAssignLoadAwareFetchTimeWeight();
  }

  /**
   * It assumes that all disks whose available space is greater than the minimum space are divided
   * into multiple groups. A faster group will allocate more allocations than a slower group by
   * diskGroupGradient.
   */
  @Override
  public Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> offerSlots(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes) {

    if (partitionIds.isEmpty()) {
      return new HashMap<>();
    }

    if (workers.size() < 2 && shouldReplicate) {
      return new HashMap<>();
    }

    if (StorageInfo.HDFSOnly(availableStorageTypes)
        || StorageInfo.S3Only(availableStorageTypes)
        || StorageInfo.OSSOnly(availableStorageTypes)) {
      return fallbackSlotsAllocator.offerSlots(
          workers, partitionIds, shouldReplicate, shouldRackAware, availableStorageTypes);
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
      return fallbackSlotsAllocator.offerSlots(
          workers, partitionIds, shouldReplicate, shouldRackAware, availableStorageTypes);
    }

    if (!initialized) {
      initLoadAwareAlgorithm();
    }

    return locateSlots(
        partitionIds,
        workers,
        getSlotsRestrictions(
            placeDisksToGroups(usableDisks),
            diskToWorkerMap,
            shouldReplicate ? partitionIds.size() * 2 : partitionIds.size()),
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes);
  }

  private void initLoadAwareAlgorithm() {
    taskAllocationRatio = new double[diskGroupNum];
    double totalAllocations = 0;

    for (int i = 0; i < diskGroupNum; i++) {
      totalAllocations += Math.pow(1 + diskGroupGradient, diskGroupNum - 1 - i);
    }
    for (int i = 0; i < diskGroupNum; i++) {
      taskAllocationRatio[i] =
          Math.pow(1 + diskGroupGradient, diskGroupNum - 1 - i) / totalAllocations;
    }
    logger.info(
        "load-aware offer slots algorithm init with taskAllocationRatio {}",
        StringUtils.join(taskAllocationRatio, ','));
    initialized = true;
  }

  private List<List<DiskInfo>> placeDisksToGroups(List<DiskInfo> usableDisks) {
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
    int groupSizeSize = (int) Math.ceil(usableDisks.size() / (double) diskGroupNum);
    for (int i = 0; i < diskGroupNum; i++) {
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
  private Map<WorkerInfo, List<UsableDiskInfo>> getSlotsRestrictions(
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
}
