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

package org.apache.celeborn.plugin.flink.utils;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.celeborn.common.CelebornConf;

public class FlinkUtils {
  private static final JobID ZERO_JOB_ID = new JobID(0, 0);
  public static final Set<String> pluginConfNames =
      ImmutableSet.of(
          "remote-shuffle.job.concurrent-readings-per-gate",
          "remote-shuffle.job.memory-per-partition",
          "remote-shuffle.job.memory-per-gate",
          "remote-shuffle.job.support-floating-buffer-per-input-gate",
          "remote-shuffle.job.enable-data-compression",
          "remote-shuffle.job.support-floating-buffer-per-output-gate",
          "remote-shuffle.job.compression.codec");

  public static CelebornConf toCelebornConf(Configuration configuration) {
    if (Boolean.parseBoolean(
        configuration.getString(
            CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().key(),
            CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().defaultValueString()))) {
      throw new IllegalArgumentException(
          String.format(
              "Flink does not support replicate shuffle data. Please check the config %s.",
              CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().key()));
    }
    CelebornConf tmpCelebornConf = new CelebornConf();
    Map<String, String> confMap = configuration.toMap();
    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("celeborn.") || pluginConfNames.contains(key)) {
        tmpCelebornConf.set(key, entry.getValue());
      }
    }

    // The default value of this config option is false. If set to true, Celeborn will use local
    // allocated workers as candidate being checked workers, this is more useful for map partition
    // to regenerate the lost data. So if not set, set to true as default for flink.
    return tmpCelebornConf.setIfMissing(CelebornConf.CLIENT_CHECKED_USE_ALLOCATED_WORKERS(), true);
  }

  public static String toCelebornAppId(long lifecycleManagerTimestamp, JobID jobID) {
    // Workaround for FLINK-19358, use first none ZERO_JOB_ID as celeborn shared appId for all
    // other flink jobs
    if (!ZERO_JOB_ID.equals(jobID)) {
      return lifecycleManagerTimestamp + "-" + jobID.toString();
    }

    return lifecycleManagerTimestamp + "-" + JobID.generate();
  }

  public static String toShuffleId(JobID jobID, IntermediateDataSetID dataSetID) {
    return jobID.toString() + "-" + dataSetID.toString();
  }

  public static String toAttemptId(ExecutionAttemptID attemptID) {
    return attemptID.toString();
  }
}
