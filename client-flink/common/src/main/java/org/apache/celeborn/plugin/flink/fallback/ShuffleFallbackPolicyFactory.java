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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ShuffleFallbackPolicyFactory {

  public static List<ShuffleFallbackPolicy> getShuffleFallbackPolicies() {
    List<ShuffleFallbackPolicy> shuffleFallbackPolicies = new ArrayList<>();
    // Loading order of ShuffleFallbackPolicy should be ForceFallbackPolicy,
    // QuotaFallbackPolicy, WorkersAvailableFallbackPolicy, Custom
    // to reduce unnecessary RPC for check whether to fallback.
    shuffleFallbackPolicies.add(ForceFallbackPolicy.INSTANCE);
    shuffleFallbackPolicies.add(QuotaFallbackPolicy.INSTANCE);
    shuffleFallbackPolicies.add(WorkersAvailableFallbackPolicy.INSTANCE);
    for (ShuffleFallbackPolicy shuffleFallbackPolicy :
        ServiceLoader.load(ShuffleFallbackPolicy.class)) {
      if (!(shuffleFallbackPolicy instanceof ForceFallbackPolicy
          || shuffleFallbackPolicy instanceof QuotaFallbackPolicy
          || shuffleFallbackPolicy instanceof WorkersAvailableFallbackPolicy)) {
        shuffleFallbackPolicies.add(shuffleFallbackPolicy);
      }
    }
    return shuffleFallbackPolicies;
  }
}
