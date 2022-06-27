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

package com.aliyun.emr.rss.service.deploy.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class MasterUtil {
  private static final Random rand = new Random();
  private static double top30Ratio = 39;
  private static double mid30Ratio = 0.32;
  private static double last40Ratio = 0.27;

  public static Map<WorkerInfo, Map<String, Integer>> workerToAllocatedSlots(
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
    Iterator<WorkerInfo> workers = slots.keySet().iterator();
    Map<WorkerInfo, Map<String, Integer>> workerToSlots = new HashMap<>();
    while (workers.hasNext()) {
      WorkerInfo worker = workers.next();
      workerToSlots.compute(worker, (k,v)->{
        if (v == null) {
          v = new HashMap<>();
        }
        for (PartitionLocation location : slots.get(worker)._1) {
          v.compute(location.getDiskHint(), (hint, slot) -> {
            if (slot == null) {
              slot = 0;
            }
            slot = slot + 1;
            return slot;
          });
        }
        for (PartitionLocation location : slots.get(worker)._2) {
          v.compute(location.getDiskHint(), (hint, slot) -> {
            if (slot == null) {
              slot = 0;
            }
            slot = slot + 1;
            return slot;
          });
        }
        return v;
      });
    }
    return workerToSlots;
  }

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
  offerSlots(
    List<WorkerInfo> workers,
    List<Integer> reduceIds,
    boolean shouldReplicate,
    long minimumUsableSize) {
    int[] oldEpochs = new int[reduceIds.size()];
    Arrays.fill(oldEpochs, -1);
    return offerSlots(workers, reduceIds, oldEpochs, shouldReplicate,
      minimumUsableSize);
  }

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
  offerSlots(
    List<WorkerInfo> workers,
    List<Integer> reduceIds,
    int[] oldEpochs,
    boolean shouldReplicate,
    long minimumUsableSize) {
    if (workers.size() < 2 && shouldReplicate) {
      return null;
    }
    List<WorkerInfo> clonedWorkers = new ArrayList<>(workers.size());
    for (WorkerInfo worker : workers) {
      clonedWorkers.add(new WorkerInfo(worker));
    }

    Map<DiskInfo, WorkerInfo> diskToWorkerMap = new HashMap<>();
    List<DiskInfo> disks = new ArrayList<>();
    workers.forEach(i -> i.disks().entrySet().forEach(entry -> {
      DiskInfo diskInfo = entry.getValue();
      diskToWorkerMap.put(diskInfo, i);
      disks.add(diskInfo);
    }));

    List<DiskInfo> usableDisks = new ArrayList<>();
    for (DiskInfo disk : disks) {
      if (disk.usableSpace() > minimumUsableSize) {
        usableDisks.add(disk);
      }
    }

    Collections.sort(disks, (o1, o2) -> {
      if (o1.flushTime() <= o2.flushTime())
        return -1;
      return 1;
    });

    List<PartitionLocation> masterLocations = new ArrayList<>();
    Map<WorkerInfo, List<PartitionLocation>> masterAggregatedLocations = new HashMap<>();

    List<PartitionLocation> slaveLocations = new ArrayList<>();
    Map<WorkerInfo, List<PartitionLocation>> slaveAggregatedLocations = new HashMap<>();

    AtomicInteger reduceId = new AtomicInteger(0);
    allocatePartitionByDiskSpeed(usableDisks, reduceIds, masterLocations, masterAggregatedLocations, reduceId, oldEpochs, diskToWorkerMap, clonedWorkers, true);

    if (shouldReplicate) {
      reduceId.set(0);
      for (Map.Entry<WorkerInfo, List<PartitionLocation>> entry : masterAggregatedLocations.entrySet()) {
        WorkerInfo workerToExclude = entry.getKey();
        List<PartitionLocation> locations = entry.getValue();
        Map<Integer, Integer> reducerIdToEpoch = new HashMap<>();
        for (PartitionLocation location : locations) {
          reducerIdToEpoch.put(location.getEpoch(), location.getEpoch());
        }

        List<DiskInfo> remainDisks = new ArrayList<>(disks);
        remainDisks.removeAll(workerToExclude.disks().values());
        List<WorkerInfo> remainWorkers = new ArrayList<>(clonedWorkers);
        remainWorkers.remove(workerToExclude);
        allocatePartitionByDiskSpeed(remainDisks, new ArrayList<>(reducerIdToEpoch.keySet()), slaveLocations, slaveAggregatedLocations, reduceId, null, diskToWorkerMap, remainWorkers, false);
      }
    }

    Map<Integer, PartitionLocation> reduceIdToMasterLocations = new HashMap<>();
    for (PartitionLocation location : masterLocations) {
      reduceIdToMasterLocations.put(location.getReduceId(), location);
    }

    if (shouldReplicate) {
      Map<Integer, PartitionLocation> reduceIdToSlaveLocations = new HashMap<>();
      for (PartitionLocation location : slaveLocations) {
        reduceIdToSlaveLocations.put(location.getReduceId(), location);
      }
      for (Map.Entry<Integer, PartitionLocation> entry : reduceIdToMasterLocations.entrySet()) {
        Integer currentReduceId = entry.getKey();
        PartitionLocation masterLocation = reduceIdToMasterLocations.get(currentReduceId);
        PartitionLocation slaveLocation = reduceIdToSlaveLocations.get(currentReduceId);
        int epoch = masterLocation.getEpoch();
        slaveLocation.setEpoch(epoch);
        masterLocation.setPeer(slaveLocation);
      }
    }

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
      new HashMap<>();
    for (Map.Entry<WorkerInfo, List<PartitionLocation>> entry : masterAggregatedLocations.entrySet()) {
      WorkerInfo workerInfo = entry.getKey();

    }

    return slots;
  }

  private static void allocatePartitionByDiskSpeed(List<DiskInfo> disks,
    List<Integer> reduceIds,
    List<PartitionLocation> locations,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations,
    AtomicInteger reducerId,
    int[] oldEpochs,
    Map<DiskInfo, WorkerInfo> diskToWorkerMap,
    List<WorkerInfo> workers,
    boolean isMaster) {

    int thirtyPerDiskCount = (int) Math.floor(disks.size() * 0.3);
    List<DiskInfo> top30 = disks.subList(0, thirtyPerDiskCount);
    List<DiskInfo> mid30 = disks.subList(thirtyPerDiskCount, thirtyPerDiskCount * 2);
    List<DiskInfo> last40 = disks.subList(thirtyPerDiskCount * 2, disks.size());

    long top30Total = top30.stream().map(d -> d.availableSlots()).collect(Collectors.summingLong(l -> l));
    long mid30Total = mid30.stream().map(d -> d.availableSlots()).collect(Collectors.summingLong(l -> l));
    long last40Total = last40.stream().map(d -> d.availableSlots()).collect(Collectors.summingLong(l -> l));

    int requestTotalSlots = reduceIds.size();
    int top30Required = (int) Math.ceil(requestTotalSlots * top30Ratio);
    int mid30Required = (int) Math.ceil(requestTotalSlots * mid30Ratio);
    int last40Required = (int) Math.ceil(requestTotalSlots * last40Ratio);

    int topToMid = 0;
    int midToLast = 0;

    int top30Allocated = 0;
    int mid30Allocated = 0;
    int last40Allocated = 0;
    int lastToRoundRobin = 0;

    if (top30Required > top30Total) {
      topToMid = (int) (top30Required - top30Total);
    }
    if (mid30Required + topToMid > mid30Total) {
      midToLast = (int) (mid30Required + topToMid - mid30Total);
    }
    if (last40Required + midToLast > last40Total) {
      lastToRoundRobin = (int) (last40Required + midToLast - last40Total);
    }

    top30Allocated = top30Required - topToMid;
    mid30Allocated = mid30Required + topToMid - midToLast;
    last40Allocated = last40Required + midToLast - lastToRoundRobin;

    int[] top30Allocations = getAllocationsBySlots(top30Allocated, top30);
    int[] mid30Allocations = getAllocationsBySlots(mid30Allocated, mid30);
    int[] last40Allocations = getAllocationsBySlots(last40Allocated, last40);

    allocateLocations(top30Allocations, top30, reducerId, locations, isMaster, diskToWorkerMap, oldEpochs, reduceIds, workerAggregatedLocations);
    allocateLocations(mid30Allocations, mid30, reducerId, locations, isMaster, diskToWorkerMap, oldEpochs, reduceIds, workerAggregatedLocations);
    allocateLocations(last40Allocations, last40, reducerId, locations, isMaster, diskToWorkerMap, oldEpochs, reduceIds, workerAggregatedLocations);

    if (lastToRoundRobin > 0) {
      allocateEvenly(locations, isMaster, workers, lastToRoundRobin, reducerId, oldEpochs, reduceIds, workerAggregatedLocations);
    }
  }

  private static void allocateEvenly(List<PartitionLocation> locations,
    boolean isMaster,
    List<WorkerInfo> workers,
    int required,
    AtomicInteger reducerId,
    int[] oldEpochs,
    List<Integer> reduceIds,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations
  ) {
    int index = rand.nextInt(workers.size());
    for (int i = 0; i < required; i++) {
      int currentReducerId = reducerId.getAndIncrement();
      PartitionLocation.Mode mode = PartitionLocation.Mode.Master;
      if (!isMaster) {
        mode = PartitionLocation.Mode.Slave;
      }
      WorkerInfo workerInfo = workers.get(index % workers.size());
      int newEpoch = oldEpochs[currentReducerId] + 1;
      PartitionLocation location = new PartitionLocation(
        reduceIds.get(currentReducerId),
        newEpoch,
        workerInfo.host(),
        workerInfo.rpcPort(),
        workerInfo.pushPort(),
        workerInfo.fetchPort(),
        workerInfo.replicatePort(),
        mode,
        null,
        PartitionLocation.StorageHint.HDD,
        null);
      locations.add(location);
      workerAggregatedLocations.compute(workerInfo, (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        v.add(location);
        return v;
      });
    }
  }

  private static void allocateLocations(int[] allocations,
    List<DiskInfo> disks,
    AtomicInteger reducerId,
    List<PartitionLocation> locations,
    boolean isMaster,
    Map<DiskInfo, WorkerInfo> diskToWorkerMap,
    int[] oldEpochs,
    List<Integer> reduceIds,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations) {
    for (int i = 0; i < allocations.length; i++) {
      int allocatedSlots = allocations[i];
      DiskInfo diskInfo = disks.get(i);
      for (int j = 0; j < allocatedSlots; j++) {
        int currentReducerId = reducerId.getAndIncrement();
        WorkerInfo workerInfo = diskToWorkerMap.get(diskInfo);
        int newEpoch = 0;
        if (oldEpochs == null) {
          newEpoch = oldEpochs[currentReducerId] + 1;
        }
        PartitionLocation.Mode mode = PartitionLocation.Mode.Master;
        if (!isMaster) {
          mode = PartitionLocation.Mode.Slave;
        }
        PartitionLocation location = new PartitionLocation(
          reduceIds.get(currentReducerId),
          newEpoch,
          workerInfo.host(),
          workerInfo.rpcPort(),
          workerInfo.pushPort(),
          workerInfo.fetchPort(),
          workerInfo.replicatePort(),
          mode,
          null,
          PartitionLocation.StorageHint.HDD,
          diskInfo.mountPoint());
        locations.add(location);
        workerAggregatedLocations.compute(workerInfo, (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(location);
          return v;
        });
      }
      diskInfo.usedSlots_$eq(diskInfo.usedSlots() + allocatedSlots);
    }
  }

  private static int[] getAllocationsBySlots(int required, List<DiskInfo> disks) {
    int diskCount = disks.size();
    long[] availableSlots = new long[diskCount];
    for (int i = 0; i < diskCount; i++) {
      availableSlots[i] = disks.get(i).availableSlots();
    }
    long total = disks.stream().map(d -> d.availableSlots()).collect(Collectors.summingLong(l -> l));
    int[] allocateArray = new int[diskCount];
    for (int i = 0; i < diskCount; i++) {
      int allocation = (int) (disks.get(i).availableSlots() / total * required);
      if (allocation > disks.get(i).availableSlots()) {
        allocation = (int) disks.get(i).availableSlots();
      }
      allocateArray[i] = allocation;
      availableSlots[i] -= allocation;
    }
    int allocatedTotal = Arrays.stream(allocateArray).sum();
    if (allocatedTotal > required) {
      int extraAllocations = required - allocatedTotal;
      int i = rand.nextInt(diskCount);
      while (extraAllocations != 0) {
        int idx = i % diskCount;
        if (allocateArray[idx] > 1) {
          allocateArray[idx] = allocateArray[idx] - 1;
          extraAllocations--;
        }
        i++;
      }
    } else if (allocatedTotal < required) {
      int underAllocations = required - allocatedTotal;
      int i = rand.nextInt(diskCount);
      while (underAllocations != 0) {
        int idx = i % diskCount;
        if (availableSlots[idx] > 0) {
          allocateArray[idx] = allocateArray[idx] + 1;
        }
        i++;
      }
    }
    return allocateArray;
  }
}
