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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.JobShuffleContext;

import org.apache.celeborn.plugin.flink.CelebornFlinkShim;

/** Shim for Celeborn and Flink with tiered shuffle. */
public abstract class CelebornFlinkTierShim extends CelebornFlinkShim {

  public static JobShuffleContext getJobShuffleContext(
      JobID jobID, TierShuffleHandler tierShuffleHandler) {
    return ((CelebornFlinkTierShim) getInstance())
        .getJobShuffleContextImpl(jobID, tierShuffleHandler);
  }

  protected abstract JobShuffleContext getJobShuffleContextImpl(
      JobID jobID, TierShuffleHandler tierShuffleHandler);
}
