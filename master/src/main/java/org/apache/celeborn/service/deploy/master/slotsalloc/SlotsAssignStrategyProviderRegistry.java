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

package org.apache.celeborn.service.deploy.master.slotsalloc;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;

/** Discovers and selects a named {@link SlotsAssignStrategyProvider}. */
public final class SlotsAssignStrategyProviderRegistry {

  private SlotsAssignStrategyProviderRegistry() {}

  public static SlotsAssignStrategyProvider get(String configuredName) {
    return get(configuredName, ServiceLoader.load(SlotsAssignStrategyProvider.class));
  }

  static SlotsAssignStrategyProvider get(
      String configuredName, Iterable<SlotsAssignStrategyProvider> providers) {
    Map<String, SlotsAssignStrategyProvider> providersByName = new LinkedHashMap<>();
    for (SlotsAssignStrategyProvider provider : providers) {
      String providerName = provider.getName();
      if (providerName == null || providerName.isEmpty()) {
        throw new IllegalStateException(
            "Slots assignment strategy provider "
                + provider.getClass().getName()
                + " has an empty name");
      }

      String normalizedName = providerName.toUpperCase(Locale.ROOT);
      SlotsAssignStrategyProvider previous = providersByName.put(normalizedName, provider);
      if (previous != null) {
        throw new IllegalStateException(
            "Multiple slots assignment strategy providers are registered for '"
                + providerName
                + "': "
                + previous.getClass().getName()
                + " and "
                + provider.getClass().getName());
      }
    }

    SlotsAssignStrategyProvider provider =
        providersByName.get(configuredName.toUpperCase(Locale.ROOT));
    if (provider == null) {
      throw new IllegalArgumentException(
          "No slots assignment strategy provider is registered for '"
              + configuredName
              + "'. Available providers: "
              + new TreeSet<>(providersByName.keySet()));
    }
    return provider;
  }
}
