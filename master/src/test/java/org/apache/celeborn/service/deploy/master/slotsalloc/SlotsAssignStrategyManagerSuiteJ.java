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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.server.common.service.config.SystemConfig;
import org.apache.celeborn.service.deploy.master.slotsalloc.test.NullSlotsAssignStrategyProvider;
import org.apache.celeborn.service.deploy.master.slotsalloc.test.ThirdPartySlotsAssignStrategyProvider;
import org.apache.celeborn.service.deploy.master.slotsalloc.test.TransientSlotsAssignStrategyProvider;

public class SlotsAssignStrategyManagerSuiteJ {

  @Test
  public void testRoundRobinProviderIsDiscovered() {
    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(conf("roundrobin"), null);

    assertTrue(manager.getStrategy() instanceof RoundRobinSlotsAssignStrategy);
  }

  @Test
  public void testLoadAwareProviderIsDiscovered() {
    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(conf("loadaware"), null);

    assertTrue(manager.getStrategy() instanceof LoadAwareSlotsAssignStrategy);
  }

  @Test
  public void testThirdPartyProviderLoadsCustomConfigAndCreatesStrategy() {
    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(thirdPartyConf("7"), null);

    assertEquals(7L, slotBudget(manager.getStrategy()));
  }

  @Test
  public void testThirdPartyProviderValidatesCustomConfig() {
    SlotsAssignStrategyProvider provider = new ThirdPartySlotsAssignStrategyProvider();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> provider.create(conf(ThirdPartySlotsAssignStrategyProvider.NAME)));

    assertTrue(
        error.getMessage().contains(ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY));
  }

  @Test
  public void testUnknownProviderFailsWithAvailableNames() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SlotsAssignStrategyManager(conf("missing"), null));

    assertTrue(error.getMessage().contains("MISSING"));
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
            () -> SlotsAssignStrategyManager.loadProviders(Arrays.asList(first, second)));

    assertTrue(error.getMessage().contains("Multiple slots assignment strategy providers"));
  }

  @Test
  public void testNullStrategyProviderFailsFast() {
    NullPointerException error =
        assertThrows(
            NullPointerException.class,
            () -> new SlotsAssignStrategyManager(conf(NullSlotsAssignStrategyProvider.NAME), null));

    assertTrue(error.getMessage().contains(NullSlotsAssignStrategyProvider.NAME));
  }

  @Test
  public void testThirdPartyStrategyReloadsCustomConfigFromCelebornConf() {
    CelebornConf staticConf = thirdPartyConf("7");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());

    assertEquals(7L, slotBudget(manager.getStrategy()));

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY, "3"));
    listener.getValue().run();

    assertEquals(3L, slotBudget(manager.getStrategy()));
  }

  @Test
  public void testDynamicConfigSwitchesProvider() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    assertTrue(manager.getStrategy() instanceof RoundRobinSlotsAssignStrategy);

    Map<String, String> updatedConfigs = new HashMap<>();
    updatedConfigs.put(
        CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), ThirdPartySlotsAssignStrategyProvider.NAME);
    updatedConfigs.put(ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY, "3");
    dynamicConfig.setConfigs(updatedConfigs);
    listener.getValue().run();

    assertEquals(3L, slotBudget(manager.getStrategy()));
  }

  @Test
  public void testInvalidInitialDynamicProviderFallsBackToStaticStrategy() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(
        Collections.singletonMap(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), "missing"));
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    assertTrue(manager.getStrategy() instanceof RoundRobinSlotsAssignStrategy);

    dynamicConfig.setConfigs(
        Collections.singletonMap(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), "loadaware"));
    listener.getValue().run();

    assertTrue(manager.getStrategy() instanceof LoadAwareSlotsAssignStrategy);
  }

  @Test
  public void testInvalidInitialDynamicParameterFallsBackToStaticStrategy() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    Map<String, String> invalidConfigs = new HashMap<>();
    invalidConfigs.put(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), "loadaware");
    invalidConfigs.put(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "0");
    dynamicConfig.setConfigs(invalidConfigs);
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    assertTrue(manager.getStrategy() instanceof RoundRobinSlotsAssignStrategy);

    Map<String, String> validConfigs = new HashMap<>();
    validConfigs.put(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), "loadaware");
    validConfigs.put(CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "3");
    dynamicConfig.setConfigs(validConfigs);
    listener.getValue().run();

    assertTrue(manager.getStrategy() instanceof LoadAwareSlotsAssignStrategy);
  }

  @Test
  public void testUnknownDynamicProviderKeepsPreviousStrategy() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    SlotsAssignStrategy previousStrategy = manager.getStrategy();

    dynamicConfig.setConfigs(
        Collections.singletonMap(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), "missing"));
    listener.getValue().run();

    assertSame(previousStrategy, manager.getStrategy());
  }

  @Test
  public void testNullDynamicStrategyKeepsPreviousStrategy() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    SlotsAssignStrategy previousStrategy = manager.getStrategy();

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), NullSlotsAssignStrategyProvider.NAME));
    listener.getValue().run();

    assertSame(previousStrategy, manager.getStrategy());
  }

  @Test
  public void testFailedDynamicStrategyIsRetriedOnlyAfterConfigChange() {
    CelebornConf staticConf = conf("roundrobin");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    SlotsAssignStrategy previousStrategy = manager.getStrategy();

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(),
            TransientSlotsAssignStrategyProvider.NAME));
    listener.getValue().run();
    assertSame(previousStrategy, manager.getStrategy());

    listener.getValue().run();
    assertSame(previousStrategy, manager.getStrategy());

    Map<String, String> changedConfigs = new HashMap<>();
    changedConfigs.put(
        CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), TransientSlotsAssignStrategyProvider.NAME);
    changedConfigs.put(TransientSlotsAssignStrategyProvider.REVISION_KEY, "1");
    dynamicConfig.setConfigs(changedConfigs);
    listener.getValue().run();
    assertNotSame(previousStrategy, manager.getStrategy());
    assertTrue(manager.getStrategy() instanceof RoundRobinSlotsAssignStrategy);
  }

  @Test
  public void testInvalidDynamicConfigKeepsPreviousStrategy() {
    CelebornConf staticConf = thirdPartyConf("7");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(
        Collections.singletonMap(
            ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY, "3"));
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    SlotsAssignStrategy previousStrategy = manager.getStrategy();

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY, "0"));
    listener.getValue().run();

    assertSame(previousStrategy, manager.getStrategy());
    assertEquals(3L, slotBudget(manager.getStrategy()));
  }

  @Test
  public void testInvalidDynamicLoadAwareConfigKeepsPreviousStrategy() {
    CelebornConf staticConf = conf("loadaware");
    SystemConfig dynamicConfig = new SystemConfig(staticConf);
    dynamicConfig.setConfigs(Collections.emptyMap());
    ConfigService configService = mock(ConfigService.class);
    when(configService.getSystemConfigFromCache()).thenReturn(dynamicConfig);

    SlotsAssignStrategyManager manager = new SlotsAssignStrategyManager(staticConf, configService);
    ArgumentCaptor<Runnable> listener = ArgumentCaptor.forClass(Runnable.class);
    verify(configService).registerListenerOnConfigUpdate(listener.capture());
    SlotsAssignStrategy previousStrategy = manager.getStrategy();

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "0"));
    listener.getValue().run();
    assertSame(previousStrategy, manager.getStrategy());

    dynamicConfig.setConfigs(
        Collections.singletonMap(
            CelebornConf.MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM().key(), "3"));
    listener.getValue().run();
    assertNotSame(previousStrategy, manager.getStrategy());
    assertTrue(manager.getStrategy() instanceof LoadAwareSlotsAssignStrategy);
  }

  private static CelebornConf thirdPartyConf(String maxSlotsPerDisk) {
    return conf(ThirdPartySlotsAssignStrategyProvider.NAME)
        .set(ThirdPartySlotsAssignStrategyProvider.MAX_SLOTS_PER_DISK_KEY, maxSlotsPerDisk);
  }

  private static CelebornConf conf(String policyName) {
    return new CelebornConf().set(CelebornConf.MASTER_SLOT_ASSIGN_POLICY().key(), policyName);
  }

  private static long slotBudget(SlotsAssignStrategy strategy) {
    WorkerInfo worker = workerWithAvailableSlots(20);
    Map<WorkerInfo, List<UsableDiskInfo>> budgets =
        strategy.computeSlotBudgets(
            Collections.singletonList(worker),
            Collections.singletonList(0),
            false,
            StorageInfo.LOCAL_DISK_MASK);
    return budgets.get(worker).get(0).usableSlots;
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
