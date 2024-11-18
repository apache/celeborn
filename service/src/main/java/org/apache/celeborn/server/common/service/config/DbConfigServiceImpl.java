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

package org.apache.celeborn.server.common.service.config;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.store.IServiceManager;
import org.apache.celeborn.server.common.service.store.db.DbServiceManagerImpl;

public class DbConfigServiceImpl extends BaseConfigServiceImpl implements ConfigService {
  private volatile IServiceManager iServiceManager;

  public DbConfigServiceImpl(CelebornConf celebornConf) throws IOException {
    super(celebornConf);
  }

  @Override
  public void refreshCache() throws IOException {
    if (iServiceManager == null) {
      synchronized (this) {
        if (iServiceManager == null) {
          iServiceManager = new DbServiceManagerImpl(celebornConf, this);
        }
      }
    }

    systemConfigAtomicReference.get().setConfigs(iServiceManager.getSystemConfig());
    systemConfigAtomicReference.get().setTags(iServiceManager.getClusterTags());

    tenantConfigAtomicReference.set(
        iServiceManager.getAllTenantConfigs().stream()
            .collect(Collectors.toMap(TenantConfig::getTenantId, Function.identity())));
    tenantUserConfigAtomicReference.set(
        iServiceManager.getAllTenantUserConfigs().stream()
            .collect(
                Collectors.toMap(
                    tenantConfig -> Pair.of(tenantConfig.getTenantId(), tenantConfig.getName()),
                    Function.identity())));
  }

  @VisibleForTesting
  public IServiceManager getServiceManager() {
    return iServiceManager;
  }
}
