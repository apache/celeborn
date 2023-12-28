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
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
import org.apache.celeborn.plugin.flink.RemoteShuffleDescriptor;
import org.apache.celeborn.plugin.flink.RemoteShuffleMaster;
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

  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(
          1, ThreadUtils.namedThreadFactory("remote-shuffle-master-executor"));

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
      synchronized (RemoteShuffleMaster.class) {
        if (lifecycleManager == null) {
          celebornAppId = FlinkUtils.toCelebornAppId(lifecycleManagerTimestamp, jobID);
          LOG.info("CelebornAppId: {}", celebornAppId);
          // if not set, set to true as default for flink
          conf.setIfMissing(CelebornConf.CLIENT_CHECKED_USE_ALLOCATED_WORKERS(), true);
          lifecycleManager = new LifecycleManager(celebornAppId, conf);
          this.shuffleResourceTracker = new ShuffleResourceTracker(executor, lifecycleManager);
        }
      }
    }

    Set<Integer> previousShuffleIds = jobShuffleIds.putIfAbsent(jobID, new HashSet<>());
    LOG.info("Register job: {}.", jobID);
    shuffleResourceTracker.registerJob(getJobShuffleContext(jobID, tierShuffleHandler));
    if (previousShuffleIds != null) {
      throw new RuntimeException("Duplicated registration job: " + jobID);
    }
  }

  @Override
  public void unregisterJob(JobID jobID) {
    LOG.info("Unregister job: {}.", jobID);
    Set<Integer> shuffleIds = jobShuffleIds.remove(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            try {
              for (Integer shuffleId : shuffleIds) {
                lifecycleManager.unregisterShuffle(shuffleId);
                shuffleTaskInfo.removeExpiredShuffle(shuffleId);
              }
              shuffleResourceTracker.unRegisterJob(jobID);
            } catch (Throwable throwable) {
              LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
            }
          });
    }
  }

  @Override
  public TierShuffleDescriptor addPartitionAndGetShuffleDescriptor(
      JobID jobID, ResultPartitionID resultPartitionID) {
    FlinkResultPartitionInfo resultPartitionInfo =
        new FlinkResultPartitionInfo(jobID, resultPartitionID);
    ShuffleResourceDescriptor shuffleResourceDescriptor =
        shuffleTaskInfo.genShuffleResourceDescriptor(
            resultPartitionInfo.getShuffleId(),
            resultPartitionInfo.getTaskId(),
            resultPartitionInfo.getAttemptId());
    Set<Integer> shuffleIds = jobShuffleIds.get(jobID);
    if (shuffleIds == null) {
      throw new RuntimeException("Can not find job in master agent, job: " + jobID);
    }
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
    return new RemoteShuffleDescriptor(
        celebornAppId,
        jobID,
        resultPartitionInfo.getShuffleId(),
        resultPartitionID,
        remoteShuffleResource);
  }

  @Override
  public void releasePartition(TierShuffleDescriptor shuffleDescriptor) {
    checkState(shuffleDescriptor instanceof RemoteShuffleDescriptor, "Wrong descriptor type.");
    try {
      RemoteShuffleDescriptor descriptor = (RemoteShuffleDescriptor) shuffleDescriptor;
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
      lifecycleManager.stop();
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
        return CompletableFuture.completedFuture(Collections.emptyList());
      }

      @Override
      public void notifyPartitionRecoveryStarted() {}
    };
  }
}
