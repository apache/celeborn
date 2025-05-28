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

import java.util.ServiceLoader;

import com.google.common.annotations.VisibleForTesting;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils;

public class DynamicConfigServiceFactory {
  private static volatile ConfigService _INSTANCE;

  public static ConfigService getConfigService(CelebornConf celebornConf) {
    if (celebornConf.dynamicConfigStoreBackend().isEmpty()) {
      return null;
    }

    if (_INSTANCE == null) {
      synchronized (DynamicConfigServiceFactory.class) {
        if (_INSTANCE == null) {
          String configStoreBackend = celebornConf.dynamicConfigStoreBackend().get();
          for (ConfigStore configStore : ServiceLoader.load(ConfigStore.class)) {
            if (configStore.getName().equalsIgnoreCase(configStoreBackend)) {
              configStoreBackend = configStore.getService();
              break;
            }
          }
          _INSTANCE = Utils.instantiateClassWithCelebornConf(configStoreBackend, celebornConf);
        }
      }
    }

    return _INSTANCE;
  }

  @VisibleForTesting
  public static void reset() {
    if (_INSTANCE != null) {
      _INSTANCE.shutdown();
      _INSTANCE = null;
    }
  }
}
