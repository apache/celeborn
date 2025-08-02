/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink.tiered;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.plugin.flink.FlinkResultPartitionInfo;
import org.apache.celeborn.plugin.flink.RemoteShuffleResource;
import org.apache.celeborn.plugin.flink.ShuffleResourceDescriptor;
import org.apache.celeborn.plugin.flink.ShuffleResourceTracker;
import org.apache.celeborn.plugin.flink.ShuffleTaskInfo;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class CelebornTierMasterAgent implements TierMasterAgent {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornTierMasterAgent.class);

  // Flink JobId -> Celeborn register shuffleIds
  private final Map<JobID, Set<Integer>> jobShuffleIds = JavaUtils.newConcurrentHashMap();

  private final ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();

  private final ScheduledExecutorService executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("celeborn-client-tier-master-executor");
  private final long lifecycleManagerTimestamp;

  private final CelebornConf conf;

  private ShuffleResourceTracker shuffleResourceTracker;

  private String celebornAppId;

  private volatile LifecycleManager lifecycleManager;

  public CelebornTierMasterAgent(CelebornConf conf) {
    this.conf = conf;
    this.lifecycleManagerTimestamp = System.currentTimeMillis();
  }

  @Override
  public void registerJob(JobID jobID, TierShuffleHandler tierShuffleHandler) {
    if (lifecycleManager == null) {
      synchronized (CelebornTierMasterAgent.class) {
        if (lifecycleManager == null) {
          celebornAppId = FlinkUtils.toCelebornAppId(lifecycleManagerTimestamp, jobID);
          LOG.info("CelebornAppId: {}", celebornAppId);
          lifecycleManager = new LifecycleManager(celebornAppId, conf);
          this.shuffleResourceTracker = new ShuffleResourceTracker(executor, lifecycleManager);
        }
      }
    }

    Set<Integer> previousShuffleIds = jobShuffleIds.putIfAbsent(jobID, new HashSet<>());
    LOG.info("Register job: {}.", jobID);
    if (previousShuffleIds != null) {
      throw new RuntimeException("Duplicated registration job: " + jobID);
    }
    shuffleResourceTracker.registerJob(getJobShuffleContext(jobID, tierShuffleHandler));
  }

  @Override
  public void unregisterJob(JobID jobID) {
    LOG.info("Unregister job: {}.", jobID);
    Set<Integer> shuffleIds = jobShuffleIds.remove(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            for (Integer shuffleId : shuffleIds) {
              lifecycleManager.unregisterShuffle(shuffleId);
              shuffleTaskInfo.removeExpiredShuffle(shuffleId);
            }
            shuffleResourceTracker.unRegisterJob(jobID);
          });
    }
  }

  @Override
  public TierShuffleDescriptor addPartitionAndGetShuffleDescriptor(
      JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID) {
    Set<Integer> shuffleIds = jobShuffleIds.get(jobID);
    if (shuffleIds == null) {
      throw new RuntimeException("Can not find job in master agent, job: " + jobID);
    }
    FlinkResultPartitionInfo resultPartitionInfo =
        new FlinkResultPartitionInfo(jobID, resultPartitionID);
    ShuffleResourceDescriptor shuffleResourceDescriptor =
        shuffleTaskInfo.genShuffleResourceDescriptor(
            resultPartitionInfo.getShuffleId(),
            resultPartitionInfo.getTaskId(),
            resultPartitionInfo.getAttemptId());
    shuffleIds.add(shuffleResourceDescriptor.getShuffleId());
    shuffleResourceTracker.addPartitionResource(
        jobID,
        shuffleResourceDescriptor.getShuffleId(),
        shuffleResourceDescriptor.getPartitionId(),
        resultPartitionID);

    RemoteShuffleResource remoteShuffleResource =
        new RemoteShuffleResource(
            lifecycleManager.getHost(),
            lifecycleManager.getPort(),
            lifecycleManagerTimestamp,
            shuffleResourceDescriptor);
    return new TierShuffleDescriptorImpl(
        celebornAppId,
        jobID,
        resultPartitionInfo.getShuffleId(),
        resultPartitionID,
        remoteShuffleResource);
  }

  @Override
  public void releasePartition(TierShuffleDescriptor shuffleDescriptor) {
    checkState(shuffleDescriptor instanceof TierShuffleDescriptorImpl, "Wrong descriptor type.");
    try {
      TierShuffleDescriptorImpl descriptor = (TierShuffleDescriptorImpl) shuffleDescriptor;
      RemoteShuffleResource shuffleResource = descriptor.getShuffleResource();
      ShuffleResourceDescriptor resourceDescriptor =
          shuffleResource.getMapPartitionShuffleDescriptor();
      LOG.debug("release partition resource: {}.", resourceDescriptor);
      lifecycleManager.releasePartition(
          resourceDescriptor.getShuffleId(), resourceDescriptor.getPartitionId());
      shuffleResourceTracker.removePartitionResource(
          descriptor.getJobId(),
          resourceDescriptor.getShuffleId(),
          resourceDescriptor.getPartitionId());
    } catch (Throwable throwable) {
      LOG.debug("Failed to release data partition {}.", shuffleDescriptor, throwable);
    }
  }

  @Override
  public void close() {
    try {
      jobShuffleIds.clear();
      if (null != lifecycleManager) {
        lifecycleManager.stop();
      }
    } catch (Exception e) {
      LOG.warn("Encounter exception when shutdown: {}", e.getMessage(), e);
    }

    ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executor);
  }

  private JobShuffleContext getJobShuffleContext(
      JobID jobID, TierShuffleHandler tierShuffleHandler) {
    return new JobShuffleContext() {
      @Override
      public JobID getJobId() {
        return jobID;
      }

      @Override
      public CompletableFuture<?> stopTrackingAndReleasePartitions(
          Collection<ResultPartitionID> resultPartitionIds) {
        return tierShuffleHandler.onReleasePartitions(
            resultPartitionIds.stream()
                .map(TieredStorageIdMappingUtils::convertId)
                .collect(Collectors.toList()));
      }

      @Override
      public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
          Duration duration, Set<ResultPartitionID> set) {
        // TODO we should impl this when we support JM failover.
        return CompletableFuture.completedFuture(Collections.emptyList());
      }

      @Override
      public void notifyPartitionRecoveryStarted() {
        // TODO we should impl this when we support JM failover.
      }
    };
  }
}
