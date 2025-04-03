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

package org.apache.celeborn.plugin.flink.tiered;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils$;
import org.apache.celeborn.plugin.flink.ShuffleResource;
import org.apache.celeborn.plugin.flink.ShuffleResourceDescriptor;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class CelebornTierMasterAgentSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(CelebornTierMasterAgentSuiteJ.class);
  private CelebornTierMasterAgent masterAgent;

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
    int startPort = Utils$.MODULE$.selectRandomInt(1024, 65535);
    configuration.setString("celeborn.master.port", String.valueOf(startPort));
    configuration.setString("celeborn.master.endpoints", "localhost:" + startPort);
    masterAgent = createMasterAgent(configuration);
  }

  @Test
  public void testRegisterJob() {
    TierShuffleHandler tierShuffleHandler = createTierShuffleHandler();
    JobID jobID = JobID.generate();
    masterAgent.registerJob(jobID, tierShuffleHandler);

    // reRunRegister job
    try {
      masterAgent.registerJob(jobID, tierShuffleHandler);
      Assert.fail("should throw exception if double register job");
    } catch (Exception e) {
      Assert.assertTrue(true);
    }

    // unRegister job
    masterAgent.unregisterJob(jobID);
    masterAgent.registerJob(jobID, tierShuffleHandler);
  }

  private static TierShuffleHandler createTierShuffleHandler() {
    return new TierShuffleHandler() {

      @Override
      public CompletableFuture<?> onReleasePartitions(
          Collection<TieredStoragePartitionId> collection) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void onFatalError(Throwable throwable) {
        System.exit(-1);
      }
    };
  }

  @Test
  public void testRegisterPartitionWithProducer() {
    JobID jobID = JobID.generate();
    TierShuffleHandler tierShuffleHandler = createTierShuffleHandler();
    masterAgent.registerJob(jobID, tierShuffleHandler);

    ExecutionAttemptID executionAttemptID =
        new ExecutionAttemptID(
            new ExecutionGraphID(), new ExecutionVertexID(new JobVertexID(0L, 0L), 0), 0);
    ResultPartitionID resultPartitionID =
        new ResultPartitionID(
            new IntermediateResultPartitionID(new IntermediateDataSetID(), 0), executionAttemptID);
    TierShuffleDescriptor tierShuffleDescriptor =
        masterAgent.addPartitionAndGetShuffleDescriptor(jobID, 1, resultPartitionID);
    Assert.assertTrue(tierShuffleDescriptor instanceof TierShuffleDescriptorImpl);
    ShuffleResource shuffleResource =
        ((TierShuffleDescriptorImpl) tierShuffleDescriptor).getShuffleResource();
    ShuffleResourceDescriptor mapPartitionShuffleDescriptor =
        shuffleResource.getMapPartitionShuffleDescriptor();

    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getPartitionId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getAttemptId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getMapId());

    // use same partition id
    tierShuffleDescriptor =
        masterAgent.addPartitionAndGetShuffleDescriptor(jobID, 1, resultPartitionID);
    mapPartitionShuffleDescriptor =
        ((TierShuffleDescriptorImpl) tierShuffleDescriptor)
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor();
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getMapId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getPartitionId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getAttemptId());

    // use another partition number
    tierShuffleDescriptor =
        masterAgent.addPartitionAndGetShuffleDescriptor(
            jobID,
            1,
            new ResultPartitionID(
                new IntermediateResultPartitionID(
                    resultPartitionID.getPartitionId().getIntermediateDataSetID(), 1),
                executionAttemptID));
    mapPartitionShuffleDescriptor =
        ((TierShuffleDescriptorImpl) tierShuffleDescriptor)
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor();
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getShuffleId());
    Assert.assertEquals(1, mapPartitionShuffleDescriptor.getMapId());
    Assert.assertEquals(2, mapPartitionShuffleDescriptor.getPartitionId());
    Assert.assertEquals(0, mapPartitionShuffleDescriptor.getAttemptId());
  }

  @Test
  public void testRegisterMultipleJobs() throws UnknownHostException {
    JobID jobID1 = JobID.generate();
    TierShuffleHandler tierShuffleHandler1 = createTierShuffleHandler();
    masterAgent.registerJob(jobID1, tierShuffleHandler1);

    JobID jobID2 = JobID.generate();
    TierShuffleHandler tierShuffleHandler2 = createTierShuffleHandler();
    masterAgent.registerJob(jobID2, tierShuffleHandler2);

    IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
    ResultPartitionID resultPartitionID = new ResultPartitionID();
    TierShuffleDescriptor tierShuffleDescriptor1 =
        masterAgent.addPartitionAndGetShuffleDescriptor(jobID1, 1, resultPartitionID);

    // use same partition id but different jobId
    TierShuffleDescriptor tierShuffleDescriptor2 =
        masterAgent.addPartitionAndGetShuffleDescriptor(jobID2, 1, resultPartitionID);

    Assert.assertEquals(
        ((TierShuffleDescriptorImpl) tierShuffleDescriptor1)
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor()
            .getShuffleId(),
        0);
    Assert.assertEquals(
        ((TierShuffleDescriptorImpl) tierShuffleDescriptor2)
            .getShuffleResource()
            .getMapPartitionShuffleDescriptor()
            .getShuffleId(),
        1);
  }

  @After
  public void tearDown() {
    if (masterAgent != null) {
      try {
        masterAgent.close();
      } catch (Exception e) {
        LOG.warn(e.getMessage(), e);
      }
    }
  }

  public CelebornTierMasterAgent createMasterAgent(Configuration configuration) {
    CelebornConf conf = FlinkUtils.toCelebornConf(configuration);
    return new CelebornTierMasterAgent(conf);
  }
}
