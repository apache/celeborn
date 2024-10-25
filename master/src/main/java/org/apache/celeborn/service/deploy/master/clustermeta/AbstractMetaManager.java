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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
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
  public final Map<String, Set<Integer>> registeredAppAndShuffles =
      JavaUtils.newConcurrentHashMap();
  public final Set<String> hostnameSet = ConcurrentHashMap.newKeySet();
  public final ConcurrentHashMap<String, Long> appHeartbeatTime = JavaUtils.newConcurrentHashMap();

  private final Map<String, WorkerInfo> workersMap = JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, WorkerInfo> workerInfoPool =
      JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, Long> lostWorkers = JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, WorkerEventInfo> workerEventInfos =
      JavaUtils.newConcurrentHashMap();
  private final Set<String> excludedWorkers = ConcurrentHashMap.newKeySet();
  private final Set<String> manuallyExcludedWorkers = ConcurrentHashMap.newKeySet();
  private final Set<String> shutdownWorkers = ConcurrentHashMap.newKeySet();
  private final Set<String> decommissionWorkers = ConcurrentHashMap.newKeySet();
  private final Set<String> workerLostEvents = ConcurrentHashMap.newKeySet();

  protected RpcEnv rpcEnv;
  protected CelebornConf conf;
  protected CelebornRackResolver rackResolver;

  public long initialEstimatedPartitionSize;
  public long estimatedPartitionSize;
  public double unhealthyDiskRatioThreshold;
  public final LongAdder partitionTotalWritten = new LongAdder();
  public final LongAdder partitionTotalFileCount = new LongAdder();
  public AppDiskUsageMetric appDiskUsageMetric = null;

  public final ConcurrentHashMap<String, ApplicationMeta> applicationMetas =
      JavaUtils.newConcurrentHashMap();

  public Collection<WorkerInfo> getWorkers() {
    return Collections.unmodifiableCollection(workersMap.values());
  }

  public <T> T synchronizedWorkers(Supplier<T> s) {
    synchronized (workersMap) {
      return s.get();
    }
  }

  public boolean containsWorker(WorkerInfo worker) {
    return workersMap.containsKey(worker.toUniqueId());
  }

  public WorkerInfo getWorker(WorkerInfo worker) {
    return workersMap.get(worker.toUniqueId());
  }

  @VisibleForTesting
  public void updateWorker(WorkerInfo worker) {
    workersMap.put(worker.toUniqueId(), worker);
  }

  @VisibleForTesting
  public void clearWorkers() {
    workersMap.clear();
  }

  private WorkerInfo getFromWorkerInfoPool(String workerUniqueId) {
    return workerInfoPool.computeIfAbsent(workerUniqueId, id -> WorkerInfo.fromUniqueId(id));
  }

  private void recycleToWorkerInfoPool(String workerUniqueId, WorkerInfo workerInfo) {
    workerInfoPool.putIfAbsent(workerUniqueId, workerInfo);
  }

  private void releaseFromWorkerInfoPool(String workerUniqueId) {
    workerInfoPool.remove(workerUniqueId);
  }

  public Set<String> getManuallyExcludedWorkerIds() {
    return Collections.unmodifiableSet(manuallyExcludedWorkers);
  }

  public Set<WorkerInfo> getManuallyExcludedWorkerInfos() {
    return manuallyExcludedWorkers.stream()
        .map(this::getFromWorkerInfoPool)
        .collect(Collectors.toSet());
  }

  public Set<String> getShutdownWorkerIds() {
    return Collections.unmodifiableSet(shutdownWorkers);
  }

  public Set<WorkerInfo> getShutdownWorkerInfos() {
    return shutdownWorkers.stream().map(this::getFromWorkerInfoPool).collect(Collectors.toSet());
  }

  public Set<String> getDecommissionWorkerIds() {
    return Collections.unmodifiableSet(decommissionWorkers);
  }

  public Set<WorkerInfo> getDecommissionWorkerInfos() {
    return decommissionWorkers.stream()
        .map(this::getFromWorkerInfoPool)
        .collect(Collectors.toSet());
  }

  public Set<WorkerInfo> getWorkerLostEventWorkerInfos() {
    return workerLostEvents.stream().map(this::getFromWorkerInfoPool).collect(Collectors.toSet());
  }

  public boolean containsWorkerLostEvent(WorkerInfo workerInfo) {
    return workerLostEvents.contains(workerInfo.toUniqueId());
  }

  public void removeWorkerLostEvent(WorkerInfo workerInfo) {
    workerLostEvents.remove(workerInfo.toUniqueId());
  }

  @VisibleForTesting
  public void clearWorkerLostEvents() {
    workerLostEvents.clear();
  }

  public Map<String, Long> getLostWorkerIds() {
    return Collections.unmodifiableMap(lostWorkers);
  }

  public Map<WorkerInfo, Long> getLostWorkerInfos() {
    return lostWorkers.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> getFromWorkerInfoPool(entry.getKey()), Map.Entry::getValue));
  }

  public Map<WorkerInfo, WorkerEventInfo> getWorkerEventInfos() {
    return workerEventInfos.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> getFromWorkerInfoPool(entry.getKey()), Map.Entry::getValue));
  }

  public WorkerEventInfo getWorkerEventInfo(WorkerInfo workerInfo) {
    return workerEventInfos.get(workerInfo.toUniqueId());
  }

  public Set<String> getExcludedWorkerIds() {
    return Collections.unmodifiableSet(excludedWorkers);
  }

  public Set<WorkerInfo> getExcludedWorkerInfos() {
    return excludedWorkers.stream().map(this::getFromWorkerInfoPool).collect(Collectors.toSet());
  }

  @VisibleForTesting
  public void addExcludeWorker(WorkerInfo workerInfo) {
    excludedWorkers.add(workerInfo.toUniqueId());
  }

  @VisibleForTesting
  public void clearExcludedWorkers() {
    excludedWorkers.clear();
  }

  public void updateRequestSlotsMeta(
      String shuffleKey, String hostName, Map<String, Map<String, Integer>> workerWithAllocations) {
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    registeredAppAndShuffles
        .computeIfAbsent(appIdShuffleId._1(), v -> new HashSet<>())
        .add((Integer) appIdShuffleId._2);

    String appId = appIdShuffleId._1;
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
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    Set<Integer> shuffleIds = registeredAppAndShuffles.get(appIdShuffleId._1());
    if (shuffleIds != null) {
      shuffleIds.remove(appIdShuffleId._2);
      registeredAppAndShuffles.compute(
          appIdShuffleId._1(),
          (s, shuffles) -> {
            if (shuffles.size() == 0) {
              return null;
            }
            return shuffles;
          });
    }
  }

  public void updateBatchUnregisterShuffleMeta(List<String> shuffleKeys) {
    for (String shuffleKey : shuffleKeys) {
      Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
      String appId = appIdShuffleId._1;
      if (registeredAppAndShuffles.containsKey(appId)) {
        registeredAppAndShuffles.get(appId).remove(appIdShuffleId._2);
      }
    }
  }

  public void updateAppHeartbeatMeta(String appId, long time, long totalWritten, long fileCount) {
    appHeartbeatTime.put(appId, time);
    partitionTotalWritten.add(totalWritten);
    partitionTotalFileCount.add(fileCount);
  }

  public void updateAppLostMeta(String appId) {
    registeredAppAndShuffles.remove(appId);
    appHeartbeatTime.remove(appId);
    applicationMetas.remove(appId);
  }

  public void updateWorkerExcludeMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    workersToAdd.forEach(
        worker -> {
          String workerId = worker.toUniqueId();
          recycleToWorkerInfoPool(workerId, worker);
          manuallyExcludedWorkers.add(workerId);
        });
    workersToRemove.forEach(worker -> manuallyExcludedWorkers.remove(worker.toUniqueId()));
  }

  public void reviseLostShuffles(String appId, List<Integer> lostShuffles) {
    registeredAppAndShuffles.computeIfAbsent(appId, v -> new HashSet<>()).addAll(lostShuffles);
  }

  public void deleteApp(String appId) {
    registeredAppAndShuffles.remove(appId);
  }

  public void updateWorkerLostMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    String workerId = worker.toUniqueId();
    recycleToWorkerInfoPool(workerId, worker);
    workerLostEvents.add(workerId);
    // remove worker from workers
    synchronized (workersMap) {
      workersMap.remove(workerId);
      lostWorkers.put(workerId, System.currentTimeMillis());
    }
    excludedWorkers.remove(workerId);
    workerLostEvents.remove(workerId);
  }

  public void updateWorkerRemoveMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    String workerId = worker.toUniqueId();
    recycleToWorkerInfoPool(workerId, worker);
    // remove worker from workers
    synchronized (workersMap) {
      workersMap.remove(workerId);
      lostWorkers.put(workerId, System.currentTimeMillis());
    }
    excludedWorkers.remove(workerId);
  }

  public void removeWorkersUnavailableInfoMeta(List<WorkerInfo> unavailableWorkers) {
    synchronized (workersMap) {
      for (WorkerInfo workerInfo : unavailableWorkers) {
        String workerId = workerInfo.toUniqueId();
        if (lostWorkers.containsKey(workerId)) {
          lostWorkers.remove(workerId);
          shutdownWorkers.remove(workerId);
          workerEventInfos.remove(workerId);
          decommissionWorkers.remove(workerId);
          releaseFromWorkerInfoPool(workerId);
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
    String workerId = worker.toUniqueId();
    AtomicLong availableSlots = new AtomicLong();
    LOG.debug("update worker {}:{} heartbeat {}", host, rpcPort, disks);
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(workerId));
      workerInfo.ifPresent(
          info -> {
            info.updateThenGetDiskInfos(disks, Option.apply(estimatedPartitionSize));
            info.updateThenGetUserResourceConsumption(userResourceConsumption);
            availableSlots.set(info.totalAvailableSlots());
            info.lastHeartbeat_$eq(time);
            info.setWorkerStatus(workerStatus);
          });
    }

    WorkerEventInfo workerEventInfo = workerEventInfos.get(workerId);
    if (workerEventInfo != null
        && WorkerStatusUtils.meetFinalState(workerEventInfo, workerStatus)) {
      workerEventInfos.remove(workerId);
      if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
        shutdownWorkers.remove(workerId);
      }
    }

    appDiskUsageMetric.update(estimatedAppDiskUsage);
    // If using HDFSONLY mode, workers with empty disks should not be put into excluded worker list.
    long unhealthyDiskNum =
        disks.values().stream().filter(s -> !s.status().equals(DiskStatus.HEALTHY)).count();
    boolean exceed = unhealthyDiskNum * 1.0 / disks.size() >= unhealthyDiskRatioThreshold;
    if (!excludedWorkers.contains(workerId)
        && (((disks.isEmpty() || exceed) && !conf.hasHDFSStorage() && !conf.hasS3Storage())
            || highWorkload)) {
      LOG.warn(
          "Worker {} (unhealthy disks num: {}) adds to excluded workers", worker, unhealthyDiskNum);
      recycleToWorkerInfoPool(workerId, worker);
      excludedWorkers.add(workerId);
    } else if ((availableSlots.get() > 0 || conf.hasHDFSStorage() || conf.hasS3Storage())
        && !highWorkload) {
      // only unblack if numSlots larger than 0
      excludedWorkers.remove(workerId);
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
    String workerId = workerInfo.toUniqueId();
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());
    if (networkLocation != null
        && !networkLocation.isEmpty()
        && !NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
      workerInfo.networkLocation_$eq(networkLocation);
    } else {
      workerInfo.networkLocation_$eq(rackResolver.resolve(host).getNetworkLocation());
    }
    workerInfo.updateDiskMaxSlots(estimatedPartitionSize);
    synchronized (workersMap) {
      if (!workersMap.containsKey(workerId)) {
        workersMap.put(workerId, workerInfo);
      }
      shutdownWorkers.remove(workerId);
      lostWorkers.remove(workerId);
      excludedWorkers.remove(workerId);
      workerEventInfos.remove(workerId);
      decommissionWorkers.remove(workerId);
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
                registeredAppAndShuffles,
                hostnameSet,
                getExcludedWorkerInfos(),
                getManuallyExcludedWorkerInfos(),
                getWorkerLostEventWorkerInfos(),
                appHeartbeatTime,
                new HashSet(getWorkers()),
                partitionTotalWritten.sum(),
                partitionTotalFileCount.sum(),
                appDiskUsageMetric.snapShots(),
                appDiskUsageMetric.currentSnapShot().get(),
                getLostWorkerInfos(),
                getShutdownWorkerInfos(),
                getWorkerEventInfos(),
                applicationMetas,
                getDecommissionWorkerInfos())
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

      for (String shuffleKey : snapshotMetaInfo.getRegisteredShuffleList()) {
        Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
        registeredAppAndShuffles
            .computeIfAbsent(appIdShuffleId._1, v -> new HashSet<>())
            .add((Integer) appIdShuffleId._2);
      }
      hostnameSet.addAll(snapshotMetaInfo.getHostnameSetList());

      Set<WorkerInfo> excludeWorkerInfos =
          snapshotMetaInfo.getExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      excludedWorkers.addAll(
          excludeWorkerInfos.stream().map(WorkerInfo::toUniqueId).collect(Collectors.toSet()));
      workerInfoPool.putAll(
          excludeWorkerInfos.stream().collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      Set<WorkerInfo> manuallyExcludedWorkerInfos =
          snapshotMetaInfo.getManuallyExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      manuallyExcludedWorkers.addAll(
          manuallyExcludedWorkerInfos.stream()
              .map(WorkerInfo::toUniqueId)
              .collect(Collectors.toSet()));
      workerInfoPool.putAll(
          manuallyExcludedWorkerInfos.stream()
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      Set<WorkerInfo> workerLostEventInfos =
          snapshotMetaInfo.getWorkerLostEventsList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      workerLostEvents.addAll(
          workerLostEventInfos.stream().map(WorkerInfo::toUniqueId).collect(Collectors.toSet()));
      workerInfoPool.putAll(
          workerLostEventInfos.stream().collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      appHeartbeatTime.putAll(snapshotMetaInfo.getAppHeartbeatTimeMap());

      registeredAppAndShuffles.forEach(
          (appId, shuffleId) -> {
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
      workersMap.putAll(
          workerInfoSet.stream()
              .peek(
                  workerInfo -> {
                    // Reset worker's network location with current master's configuration.
                    if (NetworkTopology.DEFAULT_RACK.equals(workerInfo.networkLocation())) {
                      workerInfo.networkLocation_$eq(
                          resolveMap.get(workerInfo.host()).get().getNetworkLocation());
                    }
                  })
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      lostWorkers.putAll(snapshotMetaInfo.getLostWorkersMap());
      workerInfoPool.putAll(
          lostWorkers.keySet().stream()
              .map(WorkerInfo::fromUniqueId)
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      workerEventInfos.putAll(
          snapshotMetaInfo.getWorkerEventInfosMap().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> entry.getKey(),
                      entry -> PbSerDeUtils.fromPbWorkerEventInfo(entry.getValue()))));
      workerInfoPool.putAll(
          workerEventInfos.keySet().stream()
              .map(WorkerInfo::fromUniqueId)
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      Set<WorkerInfo> shutdownWorkerInfos =
          snapshotMetaInfo.getShutdownWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      shutdownWorkers.addAll(
          shutdownWorkerInfos.stream().map(WorkerInfo::toUniqueId).collect(Collectors.toSet()));
      workerInfoPool.putAll(
          shutdownWorkerInfos.stream().collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      Set<WorkerInfo> decommissionWorkerInfos =
          snapshotMetaInfo.getDecommissionWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      decommissionWorkers.addAll(
          decommissionWorkerInfos.stream().map(WorkerInfo::toUniqueId).collect(Collectors.toSet()));
      workerInfoPool.putAll(
          decommissionWorkerInfos.stream()
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

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
        workersMap.size(),
        registeredAppAndShuffles.size(),
        excludedWorkers.size(),
        manuallyExcludedWorkers.size());
    getWorkers().forEach(workerInfo -> LOG.info(workerInfo.toString()));
    registeredAppAndShuffles.forEach(
        (appId, shuffleId) -> LOG.info("RegisteredShuffle {}-{}", appId, shuffleId));
  }

  private void cleanUpState() {
    registeredAppAndShuffles.clear();
    hostnameSet.clear();
    workersMap.clear();
    lostWorkers.clear();
    appHeartbeatTime.clear();
    excludedWorkers.clear();
    shutdownWorkers.clear();
    decommissionWorkers.clear();
    manuallyExcludedWorkers.clear();
    workerLostEvents.clear();
    partitionTotalWritten.reset();
    partitionTotalFileCount.reset();
    workerEventInfos.clear();
    applicationMetas.clear();
  }

  public void updateMetaByReportWorkerUnavailable(List<WorkerInfo> failedWorkers) {
    synchronized (this.workersMap) {
      failedWorkers.forEach(
          worker -> {
            String workerId = worker.toUniqueId();
            recycleToWorkerInfoPool(workerId, worker);
            shutdownWorkers.add(workerId);
          });
    }
  }

  public void updateWorkerEventMeta(int workerEventTypeValue, List<WorkerInfo> workerInfoList) {
    long eventTime = System.currentTimeMillis();
    ResourceProtos.WorkerEventType eventType =
        ResourceProtos.WorkerEventType.forNumber(workerEventTypeValue);
    synchronized (this.workersMap) {
      for (WorkerInfo workerInfo : workerInfoList) {
        String workerId = workerInfo.toUniqueId();
        WorkerEventInfo workerEventInfo = workerEventInfos.get(workerId);
        LOG.info("Received worker event: {} for worker: {}", eventType, workerId);
        if (workerEventInfo == null || !workerEventInfo.isSameEvent(eventType.getNumber())) {
          if (eventType == ResourceProtos.WorkerEventType.None) {
            workerEventInfos.remove(workerId);
          } else {
            recycleToWorkerInfoPool(workerId, workerInfo);
            workerEventInfos.put(workerId, new WorkerEventInfo(eventType.getNumber(), eventTime));
          }
        }
      }
    }
  }

  public void updateMetaByReportWorkerDecommission(List<WorkerInfo> workers) {
    synchronized (this.workersMap) {
      workers.forEach(
          worker -> {
            String workerId = worker.toUniqueId();
            recycleToWorkerInfoPool(workerId, worker);
            decommissionWorkers.add(workerId);
          });
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

    // Do not trigger update is estimated partition size value is unchanged
    if (estimatedPartitionSize == oldEstimatedPartitionSize) {
      return;
    }

    LOG.warn(
        "Celeborn cluster estimated partition size changed from {} to {}",
        Utils.bytesToString(oldEstimatedPartitionSize),
        Utils.bytesToString(estimatedPartitionSize));
    getWorkers().stream()
        .filter(
            worker -> {
              String workerId = worker.toUniqueId();
              return !excludedWorkers.contains(workerId)
                  && !manuallyExcludedWorkers.contains(workerId);
            })
        .forEach(workerInfo -> workerInfo.updateDiskMaxSlots(estimatedPartitionSize));
  }

  public boolean isWorkerAvailable(WorkerInfo workerInfo) {
    String workerId = workerInfo.toUniqueId();
    return (workerInfo.getWorkerStatus().getState() == PbWorkerStatus.State.Normal
            && !workerEventInfos.containsKey(workerId))
        && !excludedWorkers.contains(workerId)
        && !shutdownWorkers.contains(workerId)
        && !manuallyExcludedWorkers.contains(workerId);
  }

  public void updateApplicationMeta(ApplicationMeta applicationMeta) {
    applicationMetas.putIfAbsent(applicationMeta.appId(), applicationMeta);
  }

  public void removeApplicationMeta(String appId) {
    applicationMetas.remove(appId);
  }

  public int registeredShuffleCount() {
    return registeredAppAndShuffles.values().stream().mapToInt(Set::size).sum();
  }
}
