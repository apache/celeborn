package org.apache.celeborn.service.deploy.master.slotsalloc;

import java.util.*;

import scala.Tuple2;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class RoundRobinSlotsAllocator extends SlotsAllocator {
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

    return locateSlots(
        partitionIds,
        workers,
        getSlotsRestrictions(workers, availableStorageTypes),
        shouldReplicate,
        shouldRackAware,
        availableStorageTypes);
  }

  private static Map<WorkerInfo, List<UsableDiskInfo>> getSlotsRestrictions(
      List<WorkerInfo> workers, int availableStorageTypes) {
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
    return slotsRestrictions;
  }
}
