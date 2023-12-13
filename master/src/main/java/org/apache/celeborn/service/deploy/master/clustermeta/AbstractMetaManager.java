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

package org.apache.celeborn.service.deploy.master.clustermeta;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import scala.Option;

import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.AppDiskUsageMetric;
import org.apache.celeborn.common.meta.AppDiskUsageSnapShot;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PbSnapshotMetaInfo;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.master.network.CelebornRackResolver;

public abstract class AbstractMetaManager implements IMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetaManager.class);

  // Metadata for master service
  public final Set<String> registeredShuffle = ConcurrentHashMap.newKeySet();
  public final Set<String> hostnameSet = ConcurrentHashMap.newKeySet();
  public final ArrayList<WorkerInfo> workers = new ArrayList<>();
  public final ConcurrentHashMap<WorkerInfo, Long> lostWorkers = JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, Long> appHeartbeatTime = JavaUtils.newConcurrentHashMap();
  public final Set<WorkerInfo> excludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> manuallyExcludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> shutdownWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> workerLostEvents = ConcurrentHashMap.newKeySet();

  protected RpcEnv rpcEnv;
  protected CelebornConf conf;
  protected CelebornRackResolver rackResolver;

  public long initialEstimatedPartitionSize;
  public long estimatedPartitionSize;
  public final LongAdder partitionTotalWritten = new LongAdder();
  public final LongAdder partitionTotalFileCount = new LongAdder();
  public AppDiskUsageMetric appDiskUsageMetric = null;

  public void updateRequestSlotsMeta(
      String shuffleKey, String hostName, Map<String, Map<String, Integer>> workerWithAllocations) {
    registeredShuffle.add(shuffleKey);

    String appId = Utils.splitShuffleKey(shuffleKey)._1;
    appHeartbeatTime.compute(
        appId,
        (applicationId, oldTimestamp) -> {
          long oldTime = System.currentTimeMillis();
          if (oldTimestamp != null) {
            oldTime = oldTimestamp;
          }
          return Math.max(System.currentTimeMillis(), oldTime);
        });

    if (hostName != null) {
      hostnameSet.add(hostName);
    }
  }

  public void updateUnregisterShuffleMeta(String shuffleKey) {
    registeredShuffle.remove(shuffleKey);
  }

  public void updateAppHeartbeatMeta(String appId, long time, long totalWritten, long fileCount) {
    appHeartbeatTime.put(appId, time);
    partitionTotalWritten.add(totalWritten);
    partitionTotalFileCount.add(fileCount);
  }

  public void updateAppLostMeta(String appId) {
    registeredShuffle.removeIf(shuffleKey -> shuffleKey.startsWith(appId));
    appHeartbeatTime.remove(appId);
  }

  public void updateWorkerExcludeMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    manuallyExcludedWorkers.addAll(workersToAdd);
    workersToRemove.forEach(manuallyExcludedWorkers::remove);
  }

  public void updateWorkerLostMeta(WorkerInfo lostWorker) {
    workerLostEvents.add(lostWorker);
    // remove worker from workers
    synchronized (workers) {
      workers.remove(lostWorker);
      lostWorkers.put(lostWorker, System.currentTimeMillis());
    }
    excludedWorkers.remove(lostWorker);
    workerLostEvents.remove(lostWorker);
  }

  public void updateWorkerRemoveMeta(WorkerInfo lostWorker) {
    // remove worker from workers
    synchronized (workers) {
      workers.remove(lostWorker);
      lostWorkers.put(lostWorker, System.currentTimeMillis());
    }
    excludedWorkers.remove(lostWorker);
  }

  public void removeWorkersUnavailableInfoMeta(List<WorkerInfo> unavailableWorkers) {
    synchronized (workers) {
      for (WorkerInfo workerInfo : unavailableWorkers) {
        if (lostWorkers.containsKey(workerInfo)) {
          lostWorkers.remove(workerInfo);
          shutdownWorkers.remove(workerInfo);
        }
      }
    }
  }

  public void updateWorkerHeartbeatMeta(
      WorkerInfo workerInfo,
      Map<String, Long> estimatedAppDiskUsage,
      long time,
      boolean highWorkload) {
    AtomicLong availableSlots = new AtomicLong();
    LOG.debug("update worker {} heartbeat {}", workerInfo.toUniqueId());
    synchronized (workers) {
      Optional<WorkerInfo> lastInfo =
          workers.stream().filter(w -> w.equals(workerInfo)).findFirst();
      lastInfo.ifPresent(
          info -> {
            info.updateThenGetDiskInfos(
                workerInfo.diskInfos(), Option.apply(estimatedPartitionSize));
            info.updateThenGetUserResourceConsumption(workerInfo.userResourceConsumption());
            availableSlots.set(info.totalAvailableSlots());
            info.lastHeartbeat_$eq(time);
          });
    }
    appDiskUsageMetric.update(estimatedAppDiskUsage);
    // If using HDFSONLY mode, workers with empty disks should not be put into excluded worker list.
    if (!excludedWorkers.contains(workerInfo)
        && (((!workerInfo.haveDisk() || workerInfo.healthyDiskNum() <= 0) && !conf.hasHDFSStorage())
            || highWorkload)) {
      LOG.debug("Worker: {} num total slots is 0, add to excluded list", workerInfo.toUniqueId());
      excludedWorkers.add(workerInfo);
    } else if ((availableSlots.get() > 0 || conf.hasHDFSStorage()) && !highWorkload) {
      // only unblack if numSlots larger than 0
      excludedWorkers.remove(workerInfo);
    }
  }

  public void updateRegisterWorkerMeta(WorkerInfo workerInfo) {
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());
    workerInfo.networkLocation_$eq(rackResolver.resolve(workerInfo.host()).getNetworkLocation());
    workerInfo.updateDiskMaxSlots(estimatedPartitionSize);
    synchronized (workers) {
      if (!workers.contains(workerInfo)) {
        workers.add(workerInfo);
      }
      shutdownWorkers.remove(workerInfo);
      lostWorkers.remove(workerInfo);
      excludedWorkers.remove(workerInfo);
    }
  }

  /**
   * Used for ratis state machine to take snapshot
   *
   * @param file
   * @throws IOException
   */
  public void writeMetaInfoToFile(File file) throws IOException, RuntimeException {
    byte[] snapshotBytes =
        PbSerDeUtils.toPbSnapshotMetaInfo(
                estimatedPartitionSize,
                registeredShuffle,
                hostnameSet,
                excludedWorkers,
                manuallyExcludedWorkers,
                workerLostEvents,
                appHeartbeatTime,
                workers,
                partitionTotalWritten.sum(),
                partitionTotalFileCount.sum(),
                appDiskUsageMetric.snapShots(),
                appDiskUsageMetric.currentSnapShot().get(),
                lostWorkers,
                shutdownWorkers)
            .toByteArray();
    Files.write(file.toPath(), snapshotBytes);
  }

  /**
   * Used for ratis state machine to load snapshot
   *
   * @param file
   * @throws IOException
   */
  public void restoreMetaFromFile(File file) throws IOException {
    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
      PbSnapshotMetaInfo snapshotMetaInfo = PbSnapshotMetaInfo.parseFrom(in);
      cleanUpState();

      estimatedPartitionSize = snapshotMetaInfo.getEstimatedPartitionSize();

      registeredShuffle.addAll(snapshotMetaInfo.getRegisteredShuffleList());
      hostnameSet.addAll(snapshotMetaInfo.getHostnameSetList());
      excludedWorkers.addAll(
          snapshotMetaInfo.getExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      manuallyExcludedWorkers.addAll(
          snapshotMetaInfo.getManuallyExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      workerLostEvents.addAll(
          snapshotMetaInfo.getWorkerLostEventsList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      appHeartbeatTime.putAll(snapshotMetaInfo.getAppHeartbeatTimeMap());

      registeredShuffle.forEach(
          shuffleKey -> {
            String appId = shuffleKey.split("-")[0];
            if (!appHeartbeatTime.containsKey(appId)) {
              appHeartbeatTime.put(appId, System.currentTimeMillis());
            }
          });

      Set<WorkerInfo> workerInfoSet =
          snapshotMetaInfo.getWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      List<String> workerHostList =
          workerInfoSet.stream().map(WorkerInfo::host).collect(Collectors.toList());
      scala.collection.immutable.Map<String, Node> resolveMap =
          rackResolver.resolveToMap(workerHostList);
      workers.addAll(
          workerInfoSet.stream()
              .peek(
                  workerInfo -> {
                    // Reset worker's network location with current master's configuration.
                    workerInfo.networkLocation_$eq(
                        resolveMap.get(workerInfo.host()).get().getNetworkLocation());
                  })
              .collect(Collectors.toSet()));

      snapshotMetaInfo
          .getLostWorkersMap()
          .forEach((key, value) -> lostWorkers.put(WorkerInfo.fromUniqueId(key), value));

      shutdownWorkers.addAll(
          snapshotMetaInfo.getShutdownWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));

      partitionTotalWritten.add(snapshotMetaInfo.getPartitionTotalWritten());
      partitionTotalFileCount.add(snapshotMetaInfo.getPartitionTotalFileCount());
      appDiskUsageMetric.restoreFromSnapshot(
          snapshotMetaInfo.getAppDiskUsageMetricSnapshotsList().stream()
              .map(PbSerDeUtils::fromPbAppDiskUsageSnapshot)
              .toArray(AppDiskUsageSnapShot[]::new));
      appDiskUsageMetric.currentSnapShot_$eq(
          new AtomicReference<AppDiskUsageSnapShot>(
              PbSerDeUtils.fromPbAppDiskUsageSnapshot(
                  snapshotMetaInfo.getCurrentAppDiskUsageMetricsSnapshot())));
    } catch (Exception e) {
      throw new IOException(e);
    }
    LOG.info("Successfully restore meta info from snapshot {}", file.getAbsolutePath());
    LOG.info(
        "Worker size: {}, Registered shuffle size: {}. Worker excluded list size: {}. Manually Excluded list size: {}",
        workers.size(),
        registeredShuffle.size(),
        excludedWorkers.size(),
        manuallyExcludedWorkers.size());
    workers.forEach(workerInfo -> LOG.info(workerInfo.toString()));
    registeredShuffle.forEach(shuffle -> LOG.info("RegisteredShuffle {}", shuffle));
  }

  private void cleanUpState() {
    registeredShuffle.clear();
    hostnameSet.clear();
    workers.clear();
    lostWorkers.clear();
    appHeartbeatTime.clear();
    excludedWorkers.clear();
    shutdownWorkers.clear();
    manuallyExcludedWorkers.clear();
    workerLostEvents.clear();
    partitionTotalWritten.reset();
    partitionTotalFileCount.reset();
  }

  public void updateMetaByReportWorkerUnavailable(List<WorkerInfo> failedWorkers) {
    synchronized (this.workers) {
      shutdownWorkers.addAll(failedWorkers);
    }
  }

  public void updatePartitionSize() {
    long oldEstimatedPartitionSize = estimatedPartitionSize;
    long tmpTotalWritten = partitionTotalWritten.sumThenReset();
    long tmpFileCount = partitionTotalFileCount.sumThenReset();
    LOG.debug(
        "update partition size total written {}, file count {}",
        Utils.bytesToString(tmpTotalWritten),
        tmpFileCount);
    if (tmpFileCount != 0) {
      estimatedPartitionSize = tmpTotalWritten / tmpFileCount;
    } else {
      estimatedPartitionSize = initialEstimatedPartitionSize;
    }
    LOG.warn(
        "Celeborn cluster estimated partition size changed from {} to {}",
        Utils.bytesToString(oldEstimatedPartitionSize),
        Utils.bytesToString(estimatedPartitionSize));
    workers.stream()
        .filter(
            worker ->
                !excludedWorkers.contains(worker) && !manuallyExcludedWorkers.contains(worker))
        .forEach(workerInfo -> workerInfo.updateDiskMaxSlots(estimatedPartitionSize));
  }
}
