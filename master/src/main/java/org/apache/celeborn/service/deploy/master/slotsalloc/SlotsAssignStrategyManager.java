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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.server.common.service.config.SystemConfig;

/**
 * Discovers slot assignment strategy providers and atomically switches the active strategy after
 * dynamic configuration updates.
 */
public final class SlotsAssignStrategyManager {

  private static final Logger LOG = LoggerFactory.getLogger(SlotsAssignStrategyManager.class);

  private final CelebornConf staticConf;
  private final ConfigService configService;
  private final Map<String, SlotsAssignStrategyProvider> providersByName;

  private volatile ConfiguredStrategy configuredStrategy;
  private Map<String, String> lastAttemptedDynamicConfigs;

  public SlotsAssignStrategyManager(CelebornConf staticConf, ConfigService configService) {
    this.staticConf = staticConf.clone();
    this.configService = configService;
    this.providersByName =
        Collections.unmodifiableMap(
            loadProviders(ServiceLoader.load(SlotsAssignStrategyProvider.class)));

    Map<String, String> initialDynamicConfigs = currentDynamicConfigs();
    this.lastAttemptedDynamicConfigs = initialDynamicConfigs;
    try {
      this.configuredStrategy = createStrategy(initialDynamicConfigs);
    } catch (RuntimeException e) {
      if (initialDynamicConfigs.isEmpty()) {
        throw e;
      }
      LOG.error(
          "Failed to initialize slots assignment strategy from dynamic configuration; "
              + "falling back to static configuration",
          e);
      this.configuredStrategy = createStrategy(Collections.emptyMap());
    }

    if (configService != null) {
      configService.registerListenerOnConfigUpdate(this::reload);
      // Close the gap between taking the initial snapshot and registering the listener.
      reload();
    }
  }

  public SlotsAssignStrategy getStrategy() {
    return configuredStrategy.strategy;
  }

  static Map<String, SlotsAssignStrategyProvider> loadProviders(
      Iterable<SlotsAssignStrategyProvider> providers) {
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
    return providersByName;
  }

  private synchronized void reload() {
    Map<String, String> latestDynamicConfigs = currentDynamicConfigs();
    if (latestDynamicConfigs.equals(lastAttemptedDynamicConfigs)) {
      return;
    }

    // Record the snapshot before creating the strategy so an unchanged invalid configuration is
    // reported only once instead of being retried on every polling cycle.
    lastAttemptedDynamicConfigs = latestDynamicConfigs;
    try {
      ConfiguredStrategy updatedStrategy = createStrategy(latestDynamicConfigs);
      configuredStrategy = updatedStrategy;
      LOG.info(
          "Reloaded slots assignment strategy provider {} after dynamic configuration update",
          updatedStrategy.providerName);
    } catch (RuntimeException e) {
      LOG.error(
          "Failed to reload slots assignment strategy; keeping provider {}",
          configuredStrategy.providerName,
          e);
    }
  }

  private ConfiguredStrategy createStrategy(Map<String, String> dynamicConfigs) {
    CelebornConf effectiveConf = staticConf.clone();
    dynamicConfigs.forEach(effectiveConf::set);

    String providerName = effectiveConf.masterSlotAssignPolicyName();
    SlotsAssignStrategyProvider provider =
        providersByName.get(providerName.toUpperCase(Locale.ROOT));
    if (provider == null) {
      throw new IllegalArgumentException(
          "No slots assignment strategy provider is registered for '"
              + providerName
              + "'. Available providers: "
              + new TreeSet<>(providersByName.keySet()));
    }
    SlotsAssignStrategy strategy =
        Objects.requireNonNull(
            provider.create(effectiveConf),
            "Slots assignment strategy provider '" + providerName + "' returned null");
    return new ConfiguredStrategy(providerName, strategy);
  }

  private Map<String, String> currentDynamicConfigs() {
    if (configService == null) {
      return Collections.emptyMap();
    }

    SystemConfig systemConfig = configService.getSystemConfigFromCache();
    if (systemConfig == null || systemConfig.getConfigs() == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new HashMap<>(systemConfig.getConfigs()));
  }

  private static final class ConfiguredStrategy {
    private final String providerName;
    private final SlotsAssignStrategy strategy;

    private ConfiguredStrategy(String providerName, SlotsAssignStrategy strategy) {
      this.providerName = providerName;
      this.strategy = strategy;
    }
  }
}
