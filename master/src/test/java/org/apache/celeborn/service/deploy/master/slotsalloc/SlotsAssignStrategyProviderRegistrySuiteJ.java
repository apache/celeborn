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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.master.slotsalloc.test.ThirdPartySlotsAssignStrategyProvider;

public class SlotsAssignStrategyProviderRegistrySuiteJ {

  @Test
  public void testRoundRobinProviderIsDiscovered() {
    SlotsAssignStrategyProvider provider = SlotsAssignStrategyProviderRegistry.get("roundrobin");

    assertTrue(provider instanceof RoundRobinSlotsAssignStrategyProvider);
    assertTrue(provider.create(conf("roundrobin")) instanceof RoundRobinSlotsAssignStrategy);
  }

  @Test
  public void testLoadAwareProviderIsDiscovered() {
    SlotsAssignStrategyProvider provider = SlotsAssignStrategyProviderRegistry.get("loadaware");

    assertTrue(provider instanceof LoadAwareSlotsAssignStrategyProvider);
    assertTrue(provider.create(conf("loadaware")) instanceof LoadAwareSlotsAssignStrategy);
  }

  @Test
  public void testThirdPartyProviderLoadsCustomConfigAndCreatesStrategy() {
    CelebornConf conf =
        conf(ThirdPartySlotsAssignStrategyProvider.NAME.toLowerCase(Locale.ROOT))
            .set(
                ThirdPartySlotsAssignStrategyProvider.CONFIG_RESOURCE_KEY,
                ThirdPartySlotsAssignStrategyProvider.TEST_CONFIG_RESOURCE);

    SlotsAssignStrategyProvider provider =
        SlotsAssignStrategyProviderRegistry.get(conf.masterSlotAssignPolicyName());
    SlotsAssignStrategy strategy = provider.create(conf);

    assertTrue(provider instanceof ThirdPartySlotsAssignStrategyProvider);

    WorkerInfo worker = workerWithAvailableSlots(20);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        strategy.computeSlotBudgets(
            Collections.singletonList(worker),
            Collections.singletonList(0),
            false,
            StorageInfo.LOCAL_DISK_MASK);

    assertEquals(1, budgets.size());
    assertEquals(1, budgets.get(worker).size());
    assertEquals(7L, budgets.get(worker).get(0).usableSlots);
  }

  @Test
  public void testThirdPartyProviderValidatesCustomConfig() {
    SlotsAssignStrategyProvider provider =
        SlotsAssignStrategyProviderRegistry.get(ThirdPartySlotsAssignStrategyProvider.NAME);

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> provider.create(conf(ThirdPartySlotsAssignStrategyProvider.NAME)));

    assertTrue(
        error.getMessage().contains(ThirdPartySlotsAssignStrategyProvider.CONFIG_RESOURCE_KEY));
  }

  @Test
  public void testUnknownProviderFailsWithAvailableNames() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> SlotsAssignStrategyProviderRegistry.get("missing"));

    assertTrue(error.getMessage().contains("missing"));
    assertTrue(error.getMessage().contains("ROUNDROBIN"));
    assertTrue(error.getMessage().contains("LOADAWARE"));
    assertTrue(error.getMessage().contains(ThirdPartySlotsAssignStrategyProvider.NAME));
  }

  @Test
  public void testDuplicateProviderNamesFailFast() {
    SlotsAssignStrategyProvider first = new FixedProvider("duplicate");
    SlotsAssignStrategyProvider second = new FixedProvider("DUPLICATE");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                SlotsAssignStrategyProviderRegistry.get("duplicate", Arrays.asList(first, second)));

    assertTrue(error.getMessage().contains("Multiple slots assignment strategy providers"));
  }

  private static CelebornConf conf(String policyName) {
    return new CelebornConf().set(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), policyName);
  }

  private static WorkerInfo workerWithAvailableSlots(int availableSlots) {
    String mountPoint = "/mnt/third-party";
    DiskInfo disk = new DiskInfo(mountPoint, 1024, 0, 0, 0);
    disk.availableSlots_$eq(availableSlots);
    Map<String, DiskInfo> disks = new HashMap<>();
    disks.put(mountPoint, disk);
    return new WorkerInfo("third-party-worker", 1, 2, 3, 4, 5, disks, null);
  }

  private static final class FixedProvider implements SlotsAssignStrategyProvider {
    private final String name;

    private FixedProvider(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public SlotsAssignStrategy create(CelebornConf conf) {
      return new RoundRobinSlotsAssignStrategy();
    }
  }
}
