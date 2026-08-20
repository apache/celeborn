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

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.service.deploy.master.slotsalloc.RoundRobinSlotsAssignStrategy;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategy;
import org.apache.celeborn.service.deploy.master.slotsalloc.SlotsAssignStrategyProvider;

/** Test provider that fails its first creation attempt and succeeds after configuration changes. */
public final class TransientSlotsAssignStrategyProvider implements SlotsAssignStrategyProvider {

  public static final String NAME = "TEST_TRANSIENT";
  public static final String REVISION_KEY = "celeborn.master.slot.assign.test.revision";

  private boolean firstCreation = true;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SlotsAssignStrategy create(CelebornConf conf) {
    if (firstCreation) {
      firstCreation = false;
      throw new IllegalStateException("Transient strategy creation failure");
    }
    return new RoundRobinSlotsAssignStrategy();
  }
}
