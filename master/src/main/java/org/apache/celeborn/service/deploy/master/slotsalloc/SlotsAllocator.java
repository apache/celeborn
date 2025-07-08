package org.apache.celeborn.service.deploy.master.slotsalloc;

import java.util.*;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import scala.Tuple2;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public abstract class SlotsAllocator {
  protected static class UsableDiskInfo {
    DiskInfo diskInfo;
    long usableSlots;

    UsableDiskInfo(DiskInfo diskInfo, long usableSlots) {
      this.diskInfo = diskInfo;
      this.usableSlots = usableSlots;
    }
  }

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected static final Random rand = new Random();

  public abstract Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      offerSlots(
          List<WorkerInfo> workers,
          List<Integer> partitionIds,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes);

  /**
   * Progressive locate slots for all partitions <br>
   * 1. try to allocate for all partitions under restrictions <br>
   * 2. allocate remain partitions to all workers <br>
   * 3. allocate remain partitions to all workers again without considering rack aware <br>
   */
  protected static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
      locateSlots(
          List<Integer> partitionIds,
          List<WorkerInfo> workersList,
          Map<WorkerInfo, List<UsableDiskInfo>> slotRestrictions,
          boolean shouldReplicate,
          boolean shouldRackAware,
          int availableStorageTypes) {

    List<WorkerInfo> workersFromSlotRestrictions = new ArrayList<>(slotRestrictions.keySet());
    List<WorkerInfo> workers = workersList;
    if (shouldReplicate && shouldRackAware) {
      workersFromSlotRestrictions = generateRackAwareWorkers(workersFromSlotRestrictions);
      workers = generateRackAwareWorkers(workers);
    }

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();

    List<Integer> remain =
        roundRobin(
            slots,
            partitionIds,
            workersFromSlotRestrictions,
            slotRestrictions,
            shouldReplicate,
            shouldRackAware,
            availableStorageTypes);
    if (!remain.isEmpty()) {
      remain =
          roundRobin(
              slots,
              remain,
              workers,
              null,
              shouldReplicate,
              shouldRackAware,
              availableStorageTypes);
    }
    if (!remain.isEmpty()) {
      roundRobin(slots, remain, workers, null, shouldReplicate, false, availableStorageTypes);
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
  public static List<WorkerInfo> generateRackAwareWorkers(List<WorkerInfo> workers) {

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

  private static List<Integer> roundRobin(
      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots,
      List<Integer> partitionIds,
      List<WorkerInfo> workers,
      Map<WorkerInfo, List<UsableDiskInfo>> slotsRestrictions,
      boolean shouldReplicate,
      boolean shouldRackAware,
      int availableStorageTypes) {
    // workerInfo -> (diskIndexForPrimaryAndReplica)
    Map<WorkerInfo, Integer> workerDiskIndex = new HashMap<>();
    List<Integer> partitionIdList = new LinkedList<>(partitionIds);

    final int workerSize = workers.size();
    final IntUnaryOperator incrementIndex = v -> (v + 1) % workerSize;
    int primaryIndex = rand.nextInt(workerSize);
    int replicaIndex = rand.nextInt(workerSize);

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
        while (!haveUsableSlots(slotsRestrictions, workers, nextPrimaryInd)) {
          nextPrimaryInd = incrementIndex.applyAsInt(nextPrimaryInd);
          if (nextPrimaryInd == primaryIndex) {
            break outer;
          }
        }
        storageInfo =
            getStorageInfo(
                workers, nextPrimaryInd, slotsRestrictions, workerDiskIndex, availableStorageTypes);
      } else {
        if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
          while (!workers.get(nextPrimaryInd).haveDisk()) {
            nextPrimaryInd = incrementIndex.applyAsInt(nextPrimaryInd);
            if (nextPrimaryInd == primaryIndex) {
              break outer;
            }
          }
        }
        storageInfo =
            getStorageInfo(workers, nextPrimaryInd, null, workerDiskIndex, availableStorageTypes);
      }
      PartitionLocation primaryPartition =
          createLocation(partitionId, workers.get(nextPrimaryInd), null, storageInfo, true);

      if (shouldReplicate) {
        int nextReplicaInd = replicaIndex;
        if (slotsRestrictions != null) {
          while (nextReplicaInd == nextPrimaryInd
              || !haveUsableSlots(slotsRestrictions, workers, nextReplicaInd)
              || !satisfyRackAware(shouldRackAware, workers, nextPrimaryInd, nextReplicaInd)) {
            nextReplicaInd = incrementIndex.applyAsInt(nextReplicaInd);
            if (nextReplicaInd == replicaIndex) {
              break outer;
            }
          }
          storageInfo =
              getStorageInfo(
                  workers,
                  nextReplicaInd,
                  slotsRestrictions,
                  workerDiskIndex,
                  availableStorageTypes);
        } else if (shouldRackAware) {
          while (nextReplicaInd == nextPrimaryInd
              || !satisfyRackAware(true, workers, nextPrimaryInd, nextReplicaInd)) {
            nextReplicaInd = incrementIndex.applyAsInt(nextReplicaInd);
            if (nextReplicaInd == replicaIndex) {
              break outer;
            }
          }
        } else {
          if (StorageInfo.localDiskAvailable(availableStorageTypes)) {
            while (nextReplicaInd == nextPrimaryInd || !workers.get(nextReplicaInd).haveDisk()) {
              nextReplicaInd = incrementIndex.applyAsInt(nextReplicaInd);
              if (nextReplicaInd == replicaIndex) {
                break outer;
              }
            }
          }
          storageInfo =
              getStorageInfo(workers, nextReplicaInd, null, workerDiskIndex, availableStorageTypes);
        }
        PartitionLocation replicaPartition =
            createLocation(
                partitionId, workers.get(nextReplicaInd), primaryPartition, storageInfo, false);
        primaryPartition.setPeer(replicaPartition);
        Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
            slots.computeIfAbsent(
                workers.get(nextReplicaInd),
                v -> new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
        locations._2.add(replicaPartition);
        replicaIndex = incrementIndex.applyAsInt(nextReplicaInd);
      }

      Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
          slots.computeIfAbsent(
              workers.get(nextPrimaryInd), v -> new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
      locations._1.add(primaryPartition);
      primaryIndex = incrementIndex.applyAsInt(nextPrimaryInd);
      iter.remove();
    }
    return partitionIdList;
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

  private static boolean haveUsableSlots(
      Map<WorkerInfo, List<UsableDiskInfo>> restrictions, List<WorkerInfo> workers, int index) {
    return restrictions.get(workers.get(index)).stream().mapToLong(i -> i.usableSlots).sum() > 0;
  }

  private static boolean satisfyRackAware(
      boolean shouldRackAware, List<WorkerInfo> workers, int primaryIndex, int nextReplicaInd) {
    return !shouldRackAware
        || !Objects.equals(
            workers.get(primaryIndex).networkLocation(),
            workers.get(nextReplicaInd).networkLocation());
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
