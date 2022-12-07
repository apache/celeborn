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

package org.apache.celeborn.plugin.flink;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;
import org.apache.celeborn.plugin.flink.utils.ThreadUtils;

public class RemoteShuffleMaster implements ShuffleMaster<RemoteShuffleDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMaster.class);

  private final ShuffleMasterContext shuffleMasterContext;
  // Flink JobId -> Celeborn LifecycleManager
  private Map<JobID, LifecycleManager> lifecycleManagers = new ConcurrentHashMap<>();
  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(
          1,
          ThreadUtils.createFactoryWithDefaultExceptionHandler(
              "remote-shuffle-master-executor", LOG));

  public RemoteShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
    this.shuffleMasterContext = shuffleMasterContext;
  }

  @Override
  public void registerJob(JobShuffleContext context) {
    JobID jobId = context.getJobId();
    Future<?> submit =
        executor.submit(
            () -> {
              if (lifecycleManagers.containsKey(jobId)) {
                throw new RuntimeException("Duplicated registration job: " + jobId);
              } else {
                CelebornConf celebornConf =
                    FlinkUtils.toCelebornConf(shuffleMasterContext.getConfiguration());
                LifecycleManager lifecycleManager =
                    new LifecycleManager(FlinkUtils.toCelebornAppId(jobId), celebornConf);
                lifecycleManagers.put(jobId, lifecycleManager);
              }
            });

    try {
      submit.get();
    } catch (InterruptedException e) {
      LOG.error("Encounter interruptedException when registration job: {}.", jobId, e);
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Encounter an error when registration job: {}.", jobId, e);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void unregisterJob(JobID jobID) {
    executor.execute(
        () -> {
          try {
            LOG.info("Unregister job: {}.", jobID);
            LifecycleManager lifecycleManager = lifecycleManagers.remove(jobID);
            if (lifecycleManager != null) {
              lifecycleManager.stop();
            }
          } catch (Throwable throwable) {
            LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
          }
        });
  }

  @Override
  public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    CompletableFuture<RemoteShuffleDescriptor> completableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              LifecycleManager lifecycleManager = lifecycleManagers.get(jobID);
              FlinkResultPartitionInfo resultPartitionInfo =
                  new FlinkResultPartitionInfo(partitionDescriptor, producerDescriptor);
              LifecycleManager.ShuffleTask shuffleTask =
                  lifecycleManager.encodeExternalShuffleTask(
                      resultPartitionInfo.getShuffleId(),
                      resultPartitionInfo.getTaskId(),
                      resultPartitionInfo.getAttemptId());
              ShuffleResourceDescriptor shuffleResourceDescriptor =
                  new ShuffleResourceDescriptor(shuffleTask);
              RemoteShuffleResource remoteShuffleResource =
                  new RemoteShuffleResource(
                      lifecycleManager.getRssMetaServiceHost(),
                      lifecycleManager.getRssMetaServicePort(),
                      shuffleResourceDescriptor);
              return new RemoteShuffleDescriptor(
                  jobID, resultPartitionInfo.getResultPartitionId(), remoteShuffleResource);
            },
            executor);

    return completableFuture;
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    // TODO
  }

  @Override
  public void close() throws Exception {
    try {
      for (LifecycleManager lifecycleManager : lifecycleManagers.values()) {
        lifecycleManager.stop();
      }
    } catch (Exception e) {
      LOG.warn("Encounter exception when shutdown: " + e.getMessage(), e);
    }

    ThreadUtils.shutdownExecutors(10, executor);
  }
}
