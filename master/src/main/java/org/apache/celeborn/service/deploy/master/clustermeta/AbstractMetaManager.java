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

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.AppDiskUsageMetric;
import org.apache.celeborn.common.meta.AppDiskUsageSnapShot;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerEventInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.protocol.PbSnapshotMetaInfo;
import org.apache.celeborn.common.protocol.PbWorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.util.WorkerStatusUtils;

public abstract class AbstractMetaManager implements IMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetaManager.class);

  // Metadata for master service
  public final Set<String> registeredShuffle = ConcurrentHashMap.newKeySet();
  public final Set<String> hostnameSet = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> workers = ConcurrentHashMap.newKeySet();

  public final ConcurrentHashMap<WorkerInfo, Long> lostWorkers = JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<WorkerInfo, WorkerEventInfo> workerEventInfos =
      JavaUtils.newConcurrentHashMap();
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

  public final ConcurrentHashMap<String, ApplicationMeta> applicationMetas =
      JavaUtils.newConcurrentHashMap();

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
    applicationMetas.remove(appId);
  }

  public void updateWorkerExcludeMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    manuallyExcludedWorkers.addAll(workersToAdd);
    workersToRemove.forEach(manuallyExcludedWorkers::remove);
  }

  public void updateWorkerLostMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    workerLostEvents.add(worker);
    // remove worker from workers
    synchronized (workers) {
      workers.remove(worker);
      lostWorkers.put(worker, System.currentTimeMillis());
    }
    excludedWorkers.remove(worker);
    workerLostEvents.remove(worker);
  }

  public void updateWorkerRemoveMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    // remove worker from workers
    synchronized (workers) {
      workers.remove(worker);
      lostWorkers.put(worker, System.currentTimeMillis());
    }
    excludedWorkers.remove(worker);
  }

  public void removeWorkersUnavailableInfoMeta(List<WorkerInfo> unavailableWorkers) {
    synchronized (workers) {
      for (WorkerInfo workerInfo : unavailableWorkers) {
        if (lostWorkers.containsKey(workerInfo)) {
          lostWorkers.remove(workerInfo);
          shutdownWorkers.remove(workerInfo);
          workerEventInfos.remove(workerInfo);
        }
      }
    }
  }

  public void updateWorkerHeartbeatMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<String, DiskInfo> disks,
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption,
      Map<String, Long> estimatedAppDiskUsage,
      long time,
      WorkerStatus workerStatus,
      boolean highWorkload) {
    WorkerInfo worker =
        new WorkerInfo(
            host, rpcPort, pushPort, fetchPort, replicatePort, -1, disks, userResourceConsumption);
    AtomicLong availableSlots = new AtomicLong();
    LOG.debug("update worker {}:{} heartbeat {}", host, rpcPort, disks);
    synchronized (workers) {
      Optional<WorkerInfo> workerInfo = workers.stream().filter(w -> w.equals(worker)).findFirst();
      workerInfo.ifPresent(
          info -> {
            info.updateThenGetDiskInfos(disks, Option.apply(estimatedPartitionSize));
            info.updateThenGetUserResourceConsumption(userResourceConsumption);
            availableSlots.set(info.totalAvailableSlots());
            info.lastHeartbeat_$eq(time);
            info.setWorkerStatus(workerStatus);
          });
    }

    WorkerEventInfo workerEventInfo = workerEventInfos.get(worker);
    if (workerEventInfo != null
        && WorkerStatusUtils.meetFinalState(workerEventInfo, workerStatus)) {
      workerEventInfos.remove(worker);
      if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
        shutdownWorkers.remove(worker);
      }
    }

    appDiskUsageMetric.update(estimatedAppDiskUsage);
    // If using HDFSONLY mode, workers with empty disks should not be put into excluded worker list.
    long healthyDiskNum =
        disks.values().stream().filter(s -> s.status().equals(DiskStatus.HEALTHY)).count();
    if (!excludedWorkers.contains(worker)
        && (((disks.isEmpty() || healthyDiskNum <= 0) && !conf.hasHDFSStorage()) || highWorkload)) {
      LOG.debug("Worker: {} num total slots is 0, add to excluded list", worker);
      excludedWorkers.add(worker);
    } else if ((availableSlots.get() > 0 || conf.hasHDFSStorage()) && !highWorkload) {
      // only unblack if numSlots larger than 0
      excludedWorkers.remove(worker);
    }
  }

  public void updateRegisterWorkerMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      int internalPort,
      String networkLocation,
      Map<String, DiskInfo> disks,
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption) {
    WorkerInfo workerInfo =
        new WorkerInfo(
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            internalPort,
            disks,
            userResourceConsumption);
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());
    if (networkLocation != null
        && !networkLocation.isEmpty()
        && !NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
      workerInfo.networkLocation_$eq(networkLocation);
    } else {
      workerInfo.networkLocation_$eq(rackResolver.resolve(host).getNetworkLocation());
    }
    workerInfo.updateDiskSlots(estimatedPartitionSize);
    synchronized (workers) {
      if (!workers.contains(workerInfo)) {
        workers.add(workerInfo);
      }
      shutdownWorkers.remove(workerInfo);
      lostWorkers.remove(workerInfo);
      excludedWorkers.remove(workerInfo);
      workerEventInfos.remove(workerInfo);
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
                shutdownWorkers,
                workerEventInfos,
                applicationMetas)
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
            String appId = Utils.splitShuffleKey(shuffleKey)._1;
            if (!appHeartbeatTime.containsKey(appId)) {
              appHeartbeatTime.put(appId, System.currentTimeMillis());
            }
          });

      Set<WorkerInfo> workerInfoSet =
          snapshotMetaInfo.getWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      List<String> workerHostList =
          workerInfoSet.stream()
              .filter(w -> NetworkTopology.DEFAULT_RACK.equals(w.networkLocation()))
              .map(WorkerInfo::host)
              .collect(Collectors.toList());
      scala.collection.immutable.Map<String, Node> resolveMap =
          rackResolver.resolveToMap(workerHostList);
      workers.addAll(
          workerInfoSet.stream()
              .peek(
                  workerInfo -> {
                    // Reset worker's network location with current master's configuration.
                    if (NetworkTopology.DEFAULT_RACK.equals(workerInfo.networkLocation())) {
                      workerInfo.networkLocation_$eq(
                          resolveMap.get(workerInfo.host()).get().getNetworkLocation());
                    }
                  })
              .collect(Collectors.toSet()));

      snapshotMetaInfo
          .getLostWorkersMap()
          .forEach((key, value) -> lostWorkers.put(WorkerInfo.fromUniqueId(key), value));

      snapshotMetaInfo
          .getWorkerEventInfosMap()
          .entrySet()
          .forEach(
              entry ->
                  workerEventInfos.put(
                      WorkerInfo.fromUniqueId(entry.getKey()),
                      PbSerDeUtils.fromPbWorkerEventInfo(entry.getValue())));

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

      snapshotMetaInfo
          .getApplicationMetasMap()
          .forEach(
              (key, value) -> applicationMetas.put(key, PbSerDeUtils.fromPbApplicationMeta(value)));
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
    workerEventInfos.clear();
    applicationMetas.clear();
  }

  public void updateMetaByReportWorkerUnavailable(List<WorkerInfo> failedWorkers) {
    synchronized (this.workers) {
      shutdownWorkers.addAll(failedWorkers);
    }
  }

  public void updateWorkerEventMeta(int workerEventTypeValue, List<WorkerInfo> workerInfoList) {
    long eventTime = System.currentTimeMillis();
    ResourceProtos.WorkerEventType eventType =
        ResourceProtos.WorkerEventType.forNumber(workerEventTypeValue);
    synchronized (this.workers) {
      for (WorkerInfo workerInfo : workerInfoList) {
        WorkerEventInfo workerEventInfo = workerEventInfos.get(workerInfo);
        LOG.info("Received worker event: {} for worker: {}", eventType, workerInfo.toUniqueId());
        if (workerEventInfo == null || !workerEventInfo.isSameEvent(eventType.getNumber())) {
          if (eventType == ResourceProtos.WorkerEventType.None) {
            workerEventInfos.remove(workerInfo);
          } else {
            workerEventInfos.put(workerInfo, new WorkerEventInfo(eventType.getNumber(), eventTime));
          }
        }
      }
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
      estimatedPartitionSize =
          Math.max(
              conf.minPartitionSizeToEstimate(),
              Math.min(tmpTotalWritten / tmpFileCount, conf.maxPartitionSizeToEstimate()));
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
        .forEach(workerInfo -> workerInfo.updateDiskSlots(estimatedPartitionSize));
  }

  public boolean isWorkerAvailable(WorkerInfo workerInfo) {
    return !excludedWorkers.contains(workerInfo)
        && !shutdownWorkers.contains(workerInfo)
        && !manuallyExcludedWorkers.contains(workerInfo)
        && (!workerEventInfos.containsKey(workerInfo)
            && workerInfo.getWorkerStatus().getState() == PbWorkerStatus.State.Normal);
  }

  public void updateApplicationMeta(ApplicationMeta applicationMeta) {
    applicationMetas.putIfAbsent(applicationMeta.appId(), applicationMeta);
  }

  public void removeApplicationMeta(String appId) {
    applicationMetas.remove(appId);
  }
}
