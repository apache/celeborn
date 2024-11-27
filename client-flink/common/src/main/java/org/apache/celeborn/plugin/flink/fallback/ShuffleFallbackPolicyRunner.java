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

import java.util.List;
import java.util.Optional;

import org.apache.flink.runtime.shuffle.JobShuffleContext;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.protocol.FallbackPolicy;

public class ShuffleFallbackPolicyRunner {

  private static final List<ShuffleFallbackPolicy> FALLBACK_POLICIES =
      ShuffleFallbackPolicyFactory.getShuffleFallbackPolicies();

  public static boolean applyFallbackPolicies(
      JobShuffleContext shuffleContext,
      CelebornConf celebornConf,
      LifecycleManager lifecycleManager)
      throws CelebornIOException {
    Optional<ShuffleFallbackPolicy> fallbackPolicy =
        FALLBACK_POLICIES.stream()
            .filter(
                shuffleFallbackPolicy ->
                    shuffleFallbackPolicy.needFallback(
                        shuffleContext, celebornConf, lifecycleManager))
            .findFirst();
    boolean needFallback = fallbackPolicy.isPresent();
    if (needFallback && FallbackPolicy.NEVER.equals(celebornConf.flinkShuffleFallbackPolicy())) {
      throw new CelebornIOException(
          "Fallback to flink built-in shuffle implementation is prohibited.");
    }
    return needFallback;
  }
}
