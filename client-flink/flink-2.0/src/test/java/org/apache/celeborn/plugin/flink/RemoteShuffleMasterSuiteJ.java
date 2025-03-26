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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.FallbackPolicy;
import org.apache.celeborn.common.util.Utils$;
import org.apache.celeborn.plugin.flink.fallback.ForceFallbackPolicy;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class RemoteShuffleMasterSuiteJ {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMasterSuiteJ.class);
  private RemoteShuffleMaster remoteShuffleMaster;
  private Configuration configuration;

  @Before
  public void setUp() {
    configuration = new Configuration();
    int startPort = Utils$.MODULE$.selectRandomInt(1024, 65535);
    configuration.setString("celeborn.master.port", String.valueOf(startPort));
    configuration.setString("celeborn.master.endpoints", "localhost:" + startPort);
    configuration.setString("celeborn.client.application.heartbeatInterval", "30s");
    remoteShuffleMaster = createShuffleMaster(configuration);
  }

  @Test
  public void testRegisterJob() {
    JobShuffleContext jobShuffleContext = createJobShuffleContext(JobID.generate());
    remoteShuffleMaster.registerJob(jobShuffleContext);

    // reRunRegister job
    try {
      remoteShuffleMaster.registerJob(jobShuffleContext);
    } catch (Exception e) {
      Assert.assertTrue(true);
    }

    // unRegister job
    remoteShuffleMaster.unregisterJob(jobShuffleContext.getJobId());
    remoteShuffleMaster.registerJob(jobShuffleContext);
  }

  @Test
  public void testRegisterJobWithForceFallbackPolicy() {
    configuration.setString(
        CelebornConf.FLINK_SHUFFLE_FALLBACK_POLICY().key(), FallbackPolicy.ALWAYS.name());
    remoteShuffleMaster = createShuffleMaster(configuration, new NettyShuffleServiceFactory());
    JobID jobID = JobID.generate();
    JobShuffleContext jobShuffleContext = createJobShuffleContext(jobID);
    remoteShuffleMaster.registerJob(jobShuffleContext);
    Assert.assertTrue(remoteShuffleMaster.jobFallbackPolicies().containsKey(jobID));
    remoteShuffleMaster.unregisterJob(jobShuffleContext.getJobId());
    Assert.assertTrue(remoteShuffleMaster.jobFallbackPolicies().isEmpty());
  }

  @Test
  public void testRegisterPartitionWithProducer()
      throws UnknownHostException, ExecutionException, InterruptedException {
    JobID jobID = JobID.generate();
    JobShuffleContext jobShuffleContext = createJobShuffleContext(jobID);
    remoteShuffleMaster.registerJob(jobShuffleContext);

    IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
    PartitionDescriptor partitionDescriptor = createPartitionDescriptor(intermediateDataSetID, 0);
    ProducerDescriptor producerDescriptor = createProducerDescriptor();
    RemoteShuffleDescriptor remoteShuffleDescriptor =
        (RemoteShuffleDescriptor)
            remoteShuffleMaster
                .registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor)
                .get();
    Assert.assertEquals(1, remoteShuffleMaster.lifecycleManager().shuffleCount().sum());
    ShuffleResource shuffleResource = remoteShuffleDescriptor.getShuffleResource();
    ShuffleResourceDescriptor mapPartitionShuffleDescriptor =
        shuffleResource.getMapPartitionShuffleDescriptor();

    LOG.info("remoteShuffleDescriptor:{}", remoteShuffleDescriptor);
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getPartitionId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getAttemptId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getMapId());

    // use same dataset id
    partitionDescriptor = createPartitionDescriptor(intermediateDataSetID, 1);
    remoteShuffleDescriptor =
        (RemoteShuffleDescriptor)
            remoteShuffleMaster
                .registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor)
                .get();
    Assert.assertEquals(2, remoteShuffleMaster.lifecycleManager().shuffleCount().sum());
    mapPartitionShuffleDescriptor =
        remoteShuffleDescriptor.getShuffleResource().getMapPartitionShuffleDescriptor();
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getMapId());

    // use another attemptId
    producerDescriptor = createProducerDescriptor();
    remoteShuffleDescriptor =
        (RemoteShuffleDescriptor)
            remoteShuffleMaster
                .registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor)
                .get();
    Assert.assertEquals(3, remoteShuffleMaster.lifecycleManager().shuffleCount().sum());
    mapPartitionShuffleDescriptor =
        remoteShuffleDescriptor.getShuffleResource().getMapPartitionShuffleDescriptor();
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getAttemptId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getMapId());
  }

  @Test
  public void testRegisterPartitionWithProducerForForceFallbackPolicy()
      throws UnknownHostException, ExecutionException, InterruptedException {
    configuration.setString(
        CelebornConf.FLINK_SHUFFLE_FALLBACK_POLICY().key(), FallbackPolicy.ALWAYS.name());
    remoteShuffleMaster = createShuffleMaster(configuration, new NettyShuffleServiceFactory());
    JobID jobID = JobID.generate();
    JobShuffleContext jobShuffleContext = createJobShuffleContext(jobID);
    remoteShuffleMaster.registerJob(jobShuffleContext);

    IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
    PartitionDescriptor partitionDescriptor = createPartitionDescriptor(intermediateDataSetID, 0);
    ProducerDescriptor producerDescriptor = createProducerDescriptor();
    ShuffleDescriptor shuffleDescriptor =
        remoteShuffleMaster
            .registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor)
            .get();
    Assert.assertTrue(shuffleDescriptor instanceof NettyShuffleDescriptor);
    Assert.assertEquals(1, remoteShuffleMaster.lifecycleManager().shuffleCount().sum());
    Map<String, Long> shuffleFallbackCounts =
        remoteShuffleMaster.lifecycleManager().shuffleFallbackCounts();
    Assert.assertEquals(1, shuffleFallbackCounts.size());
    Assert.assertEquals(
        1L, shuffleFallbackCounts.get(ForceFallbackPolicy.class.getName()).longValue());
  }

  @Test
  public void testRegisterMultipleJobs()
      throws UnknownHostException, ExecutionException, InterruptedException {
    JobID jobID1 = JobID.generate();
    JobShuffleContext jobShuffleContext1 = createJobShuffleContext(jobID1);
    remoteShuffleMaster.registerJob(jobShuffleContext1);

    JobID jobID2 = JobID.generate();
    JobShuffleContext jobShuffleContext2 = createJobShuffleContext(jobID2);
    remoteShuffleMaster.registerJob(jobShuffleContext2);

    IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
    PartitionDescriptor partitionDescriptor = createPartitionDescriptor(intermediateDataSetID, 0);
    ProducerDescriptor producerDescriptor = createProducerDescriptor();
    RemoteShuffleDescriptor remoteShuffleDescriptor1 =
        (RemoteShuffleDescriptor)
            remoteShuffleMaster
                .registerPartitionWithProducer(jobID1, partitionDescriptor, producerDescriptor)
                .get();

    // use same datasetId but different jobId
    RemoteShuffleDescriptor remoteShuffleDescriptor2 =
        (RemoteShuffleDescriptor)
            remoteShuffleMaster
                .registerPartitionWithProducer(jobID2, partitionDescriptor, producerDescriptor)
                .get();

    Assert.assertEquals(
        remoteShuffleDescriptor1
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor()
            .getShuffleId(),
        0);
    Assert.assertEquals(
        remoteShuffleDescriptor2
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor()
            .getShuffleId(),
        1);
  }

  @Test
  public void testShuffleMemoryAnnouncing() {
    Map<IntermediateDataSetID, Integer> numberOfInputGateChannels = new HashMap<>();
    Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions = new HashMap<>();
    Map<IntermediateDataSetID, Integer> subPartitionNums = new HashMap<>();
    Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes = new HashMap<>();
    Map<IntermediateDataSetID, ResultPartitionType> resultPartitionTypes = new HashMap<>();
    IntermediateDataSetID inputDataSetID0 = new IntermediateDataSetID();
    IntermediateDataSetID inputDataSetID1 = new IntermediateDataSetID();
    IntermediateDataSetID outputDataSetID0 = new IntermediateDataSetID();
    IntermediateDataSetID outputDataSetID1 = new IntermediateDataSetID();
    IntermediateDataSetID outputDataSetID2 = new IntermediateDataSetID();
    Random random = new Random();
    numberOfInputGateChannels.put(inputDataSetID0, random.nextInt(1000));
    numberOfInputGateChannels.put(inputDataSetID1, random.nextInt(1000));
    int subPartitionNum1 = random.nextInt(1000);
    int subPartitionNum2 = random.nextInt(1000);
    int subPartitionNum3 = random.nextInt(1000);
    numbersOfResultSubpartitions.put(outputDataSetID0, subPartitionNum1);
    numbersOfResultSubpartitions.put(outputDataSetID1, subPartitionNum2);
    numbersOfResultSubpartitions.put(outputDataSetID2, subPartitionNum3);
    subPartitionNums.put(outputDataSetID0, subPartitionNum1);
    subPartitionNums.put(outputDataSetID1, subPartitionNum2);
    subPartitionNums.put(outputDataSetID2, subPartitionNum3);
    inputPartitionTypes.put(inputDataSetID0, ResultPartitionType.BLOCKING);
    inputPartitionTypes.put(inputDataSetID1, ResultPartitionType.BLOCKING);
    resultPartitionTypes.put(outputDataSetID0, ResultPartitionType.BLOCKING);
    resultPartitionTypes.put(outputDataSetID1, ResultPartitionType.BLOCKING);
    resultPartitionTypes.put(outputDataSetID2, ResultPartitionType.BLOCKING);
    MemorySize calculated =
        remoteShuffleMaster.computeShuffleMemorySizeForTask(
            TaskInputsOutputsDescriptor.from(
                3,
                numberOfInputGateChannels,
                numbersOfResultSubpartitions,
                subPartitionNums,
                inputPartitionTypes,
                resultPartitionTypes));

    CelebornConf celebornConf = FlinkUtils.toCelebornConf(configuration);

    long numBytesPerGate = celebornConf.clientFlinkMemoryPerInputGate();
    long expectedInput = 2 * numBytesPerGate;

    long numBytesPerResultPartition = celebornConf.clientFlinkMemoryPerResultPartition();
    long expectedOutput = 3 * numBytesPerResultPartition;
    MemorySize expected = new MemorySize(expectedInput + expectedOutput);

    Assert.assertEquals(expected, calculated);
  }

  @Test
  public void testInvalidShuffleConfig() {
    Assert.assertThrows(
        String.format(
            "The config option %s should configure as %s",
            ExecutionOptions.BATCH_SHUFFLE_MODE.key(),
            BatchShuffleMode.ALL_EXCHANGES_BLOCKING.name()),
        IllegalArgumentException.class,
        () ->
            createShuffleMaster(
                new Configuration()
                    .set(
                        ExecutionOptions.BATCH_SHUFFLE_MODE,
                        BatchShuffleMode.ALL_EXCHANGES_PIPELINED)));
    Configuration configuration = new Configuration();
    configuration.setString(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().key(), "true");
    Assert.assertThrows(
        String.format(
            "Flink does not support replicate shuffle data. Please check the config %s.",
            CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().key()),
        IllegalArgumentException.class,
        () -> createShuffleMaster(configuration));
  }

  @After
  public void tearDown() {
    if (remoteShuffleMaster != null) {
      try {
        remoteShuffleMaster.close();
      } catch (Exception e) {
        LOG.warn(e.getMessage(), e);
      }
    }
  }

  public RemoteShuffleMaster createShuffleMaster(Configuration configuration) {
    return createShuffleMaster(configuration, null);
  }

  public RemoteShuffleMaster createShuffleMaster(
      Configuration configuration, NettyShuffleServiceFactory nettyShuffleServiceFactory) {
    remoteShuffleMaster =
        new RemoteShuffleMaster(
            new ShuffleMasterContext() {
              @Override
              public Configuration getConfiguration() {
                return configuration;
              }

              @Override
              public void onFatalError(Throwable throwable) {
                System.exit(-1);
              }
            },
            nettyShuffleServiceFactory);

    return remoteShuffleMaster;
  }

  public JobShuffleContext createJobShuffleContext(JobID jobId) {
    return new JobShuffleContext() {
      @Override
      public org.apache.flink.api.common.JobID getJobId() {
        return jobId;
      }

      @Override
      public CompletableFuture<?> stopTrackingAndReleasePartitions(
          Collection<ResultPartitionID> collection) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
          Duration duration, Set<ResultPartitionID> set) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void notifyPartitionRecoveryStarted() {
        // no-op
      }
    };
  }

  public PartitionDescriptor createPartitionDescriptor(
      IntermediateDataSetID intermediateDataSetId, int partitionNum) {
    IntermediateResultPartitionID intermediateResultPartitionId =
        new IntermediateResultPartitionID(intermediateDataSetId, partitionNum);
    // Flink 1.19.0
    // [FLINK-33743][runtime] Align watermark at subpartition granularity
    return new PartitionDescriptor(
        intermediateDataSetId,
        10,
        intermediateResultPartitionId,
        ResultPartitionType.BLOCKING,
        5,
        1,
        false,
        false,
        false);
  }

  public ProducerDescriptor createProducerDescriptor() throws UnknownHostException {
    ExecutionAttemptID executionAttemptId =
        new ExecutionAttemptID(
            new ExecutionGraphID(), new ExecutionVertexID(new JobVertexID(0, 0), 0), 0);
    return new ProducerDescriptor(
        ResourceID.generate(), executionAttemptId, InetAddress.getLocalHost(), 100);
  }
}
