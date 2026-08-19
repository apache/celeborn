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

import org.apache.celeborn.common.CelebornConf;

/**
 * Service provider interface for named slot assignment strategies.
 *
 * <p>Implementations must have a public no-argument constructor and be registered in {@code
 * META-INF/services/org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategyProvider}.
 * Provider names are matched case-insensitively against {@code celeborn.master.slot.assign.policy}.
 * This is a master extension point: providers must be compiled against the same Celeborn release
 * and Scala binary version as the master where they are installed.
 */
public interface SlotsAssignStrategyProvider {

  /** Returns the name used to select this provider from configuration. */
  String getName();

  /**
   * Creates a configured strategy instance for a master.
   *
   * <p>A custom provider may read its own namespaced keys directly from the supplied Celeborn
   * configuration. When system-level dynamic configuration is enabled, it overrides the static
   * configuration in this snapshot and the master calls {@code create} after an update. The master
   * does not retry an unchanged system-level configuration snapshot. Configuration should therefore
   * be validated and captured when the strategy is created rather than read during slot allocation.
   */
  SlotsAssignStrategy create(CelebornConf conf);
}
