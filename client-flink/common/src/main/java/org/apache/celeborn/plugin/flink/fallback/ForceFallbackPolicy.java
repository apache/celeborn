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

package org.apache.celeborn.plugin.flink.fallback;

import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.FallbackPolicy;

public class ForceFallbackPolicy implements ShuffleFallbackPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(ForceFallbackPolicy.class);

  public static final ForceFallbackPolicy INSTANCE = new ForceFallbackPolicy();
  /**
   * If celeborn.client.flink.shuffle.fallback.policy is ALWAYS, fallback to flink built-in shuffle
   * implementation.
   *
   * @param shuffleContext The job shuffle context of Flink.
   * @param celebornConf The configuration of Celeborn.
   * @param lifecycleManager The {@link LifecycleManager} of Celeborn.
   * @return Return true if celeborn.client.flink.shuffle.fallback.policy is ALWAYS, otherwise
   *     false.
   */
  @Override
  public boolean needFallback(
      JobShuffleContext shuffleContext,
      CelebornConf celebornConf,
      LifecycleManager lifecycleManager) {
    FallbackPolicy shuffleFallbackPolicy = celebornConf.flinkShuffleFallbackPolicy();
    if (FallbackPolicy.ALWAYS.equals(shuffleFallbackPolicy)) {
      LOG.warn(
          "{} is {}, forcibly fallback to flink built-in shuffle implementation.",
          CelebornConf.FLINK_SHUFFLE_FALLBACK_POLICY().key(),
          FallbackPolicy.ALWAYS.name());
    }
    return FallbackPolicy.ALWAYS.equals(shuffleFallbackPolicy);
  }
}
