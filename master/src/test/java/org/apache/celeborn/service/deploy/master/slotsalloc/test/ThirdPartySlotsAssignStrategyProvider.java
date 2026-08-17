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

package org.apache.celeborn.service.deploy.master.slotsalloc.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategy;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategyProvider;
import org.apache.celeborn.service.deploy.master.slotsalloc.UsableDiskInfo;

/** Simulates a third-party provider packaged outside Celeborn's slots-allocation package. */
public final class ThirdPartySlotsAssignStrategyProvider implements SlotsAssignStrategyProvider {

  public static final String NAME = "THIRD_PARTY_LIMITED";
  public static final String CONFIG_RESOURCE_KEY =
      "celeborn.master.slot.assign.thirdParty.config.resource";
  public static final String TEST_CONFIG_RESOURCE = "third-party-slots-assign.properties";

  private static final String MAX_SLOTS_PER_DISK_KEY = "max-slots-per-disk";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SlotsAssignStrategy create(CelebornConf conf) {
    String resourceName = conf.get(CONFIG_RESOURCE_KEY, "").trim();
    if (resourceName.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing third-party configuration resource: " + CONFIG_RESOURCE_KEY);
    }
    ThirdPartyConfig config =
        ThirdPartyConfig.load(
            resourceName, ThirdPartySlotsAssignStrategyProvider.class.getClassLoader());
    return new LimitedSlotsAssignStrategy(config.maxSlotsPerDisk);
  }

  private static final class ThirdPartyConfig {
    private final long maxSlotsPerDisk;

    private ThirdPartyConfig(long maxSlotsPerDisk) {
      this.maxSlotsPerDisk = maxSlotsPerDisk;
    }

    private static ThirdPartyConfig load(String resourceName, ClassLoader classLoader) {
      InputStream resource = classLoader.getResourceAsStream(resourceName);
      if (resource == null) {
        throw new IllegalArgumentException(
            "Third-party configuration resource does not exist: " + resourceName);
      }

      Properties properties = new Properties();
      try (InputStream input = resource) {
        properties.load(input);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to load third-party configuration resource: " + resourceName, e);
      }

      String configuredLimit = properties.getProperty(MAX_SLOTS_PER_DISK_KEY);
      if (configuredLimit == null) {
        throw new IllegalArgumentException(
            "Missing third-party configuration property: " + MAX_SLOTS_PER_DISK_KEY);
      }

      final long maxSlotsPerDisk;
      try {
        maxSlotsPerDisk = Long.parseLong(configuredLimit);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid third-party configuration property "
                + MAX_SLOTS_PER_DISK_KEY
                + ": "
                + configuredLimit,
            e);
      }
      if (maxSlotsPerDisk <= 0) {
        throw new IllegalArgumentException(MAX_SLOTS_PER_DISK_KEY + " must be positive");
      }
      return new ThirdPartyConfig(maxSlotsPerDisk);
    }
  }

  private static final class LimitedSlotsAssignStrategy implements SlotsAssignStrategy {
    private final long maxSlotsPerDisk;

    private LimitedSlotsAssignStrategy(long maxSlotsPerDisk) {
      this.maxSlotsPerDisk = maxSlotsPerDisk;
    }

    @Override
    public Map<WorkerInfo, List<UsableDiskInfo>> computeSlotBudgets(
        List<WorkerInfo> workers,
        List<Integer> partitionIds,
        boolean shouldReplicate,
        int availableStorageTypes) {
      Map<WorkerInfo, List<UsableDiskInfo>> budgets = new HashMap<>();
      for (WorkerInfo worker : workers) {
        for (DiskInfo disk : worker.diskInfos().values()) {
          if (DiskStatus.HEALTHY.equals(disk.status())
              && StorageInfo.isAvailable(disk.storageType(), availableStorageTypes)
              && disk.getAvailableSlots() > 0) {
            budgets
                .computeIfAbsent(worker, ignored -> new ArrayList<>())
                .add(new UsableDiskInfo(disk, Math.min(maxSlotsPerDisk, disk.getAvailableSlots())));
          }
        }
      }
      return budgets;
    }
  }
}
