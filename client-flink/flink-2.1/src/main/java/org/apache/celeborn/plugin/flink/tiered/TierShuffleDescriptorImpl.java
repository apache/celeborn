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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import org.apache.celeborn.plugin.flink.RemoteShuffleDescriptor;
import org.apache.celeborn.plugin.flink.RemoteShuffleResource;

/**
 * Wrap the {@link RemoteShuffleDescriptor} to implement {@link TierShuffleDescriptor} interface.
 */
public class TierShuffleDescriptorImpl implements TierShuffleDescriptor {

  private final RemoteShuffleDescriptor remoteShuffleDescriptor;

  public TierShuffleDescriptorImpl(
      String celebornAppId,
      JobID jobId,
      String shuffleId,
      ResultPartitionID resultPartitionID,
      RemoteShuffleResource shuffleResource) {
    this.remoteShuffleDescriptor =
        new RemoteShuffleDescriptor(
            celebornAppId, jobId, shuffleId, resultPartitionID, shuffleResource);
  }

  public ResultPartitionID getResultPartitionID() {
    return remoteShuffleDescriptor.getResultPartitionID();
  }

  public String getCelebornAppId() {
    return remoteShuffleDescriptor.getCelebornAppId();
  }

  public JobID getJobId() {
    return remoteShuffleDescriptor.getJobId();
  }

  public String getShuffleId() {
    return remoteShuffleDescriptor.getShuffleId();
  }

  public RemoteShuffleResource getShuffleResource() {
    return remoteShuffleDescriptor.getShuffleResource();
  }

  @Override
  public String toString() {
    return "TierShuffleDescriptorImpl{"
        + "remoteShuffleDescriptor="
        + remoteShuffleDescriptor
        + '}';
  }
}
