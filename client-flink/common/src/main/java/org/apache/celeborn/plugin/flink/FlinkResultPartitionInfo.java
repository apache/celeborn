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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;

import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class FlinkResultPartitionInfo {
  private final JobID jobID;
  private final ResultPartitionID resultPartitionId;
  private final IntermediateResultPartitionID partitionId;
  private final ExecutionAttemptID producerId;

  public FlinkResultPartitionInfo(JobID jobID, ResultPartitionID resultPartitionId) {
    this.jobID = jobID;
    this.resultPartitionId = resultPartitionId;
    this.partitionId = resultPartitionId.getPartitionId();
    this.producerId = resultPartitionId.getProducerId();
  }

  public FlinkResultPartitionInfo(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    this.jobID = jobID;
    this.resultPartitionId =
        new ResultPartitionID(
            partitionDescriptor.getPartitionId(), producerDescriptor.getProducerExecutionId());
    this.partitionId = partitionDescriptor.getPartitionId();
    this.producerId = producerDescriptor.getProducerExecutionId();
  }

  public ResultPartitionID getResultPartitionId() {
    return resultPartitionId;
  }

  public String getShuffleId() {
    return FlinkUtils.toShuffleId(jobID, partitionId.getIntermediateDataSetID());
  }

  public int getTaskId() {
    return partitionId.getPartitionNumber();
  }

  public String getAttemptId() {
    return FlinkUtils.toAttemptId(producerId);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("FlinkResultPartitionInfo{");
    sb.append("jobID=").append(jobID);
    sb.append(", resultPartitionId=").append(resultPartitionId);
    sb.append(", partitionDescriptor=").append(partitionId);
    sb.append(", producerDescriptor=").append(producerId);
    sb.append('}');
    return sb.toString();
  }
}
