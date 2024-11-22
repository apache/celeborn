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

package org.apache.celeborn.tez.plugin.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.exception.CelebornRuntimeException;

public class CelebornTezUtils {
  public static final String TEZ_PREFIX = "tez.";
  public static final String TEZ_CELEBORN_LM_HOST = "celeborn.lifecycleManager.host";
  public static final String TEZ_CELEBORN_LM_PORT = "celeborn.lifecycleManager.port";
  public static final String TEZ_CELEBORN_APPLICATION_ID = "celeborn.applicationId";
  public static final String TEZ_SHUFFLE_ID = "celeborn.tez.shuffle.id";
  public static final String TEZ_BROADCAST_OR_ONETOONE = "celeborn.tez.broadcastOrOneToOne";

  public static final CelebornConf fromTezConfiguration(Configuration tezConfig) {
    CelebornConf tmpCelebornConf = new CelebornConf();
    for (Map.Entry<String, String> property : tezConfig) {
      String proName = property.getKey();
      String proValue = property.getValue();
      if (proName.startsWith(TEZ_PREFIX + "celeborn")) {
        tmpCelebornConf.set(proName.substring(TEZ_PREFIX.length()), proValue);
      }
    }
    return tmpCelebornConf;
  }

  public static Object getPrivateField(Object object, String name) {
    try {
      Field f = object.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(object);
    } catch (Exception e) {
      throw new CelebornRuntimeException(e.getMessage(), e);
    }
  }

  public static Object getParentPrivateField(Object object, String name) {
    try {
      Field f = object.getClass().getSuperclass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(object);
    } catch (Exception e) {
      throw new CelebornRuntimeException(e.getMessage(), e);
    }
  }

  public static void setParentPrivateField(Object object, String name, Object value) {
    try {
      Field f = object.getClass().getSuperclass().getDeclaredField(name);
      f.setAccessible(true);
      f.set(object, value);
    } catch (Exception e) {
      throw new CelebornRuntimeException(e.getMessage(), e);
    }
  }

  public static String uniqueIdentifierToAttemptId(String uniqueIdentifier) {
    if (uniqueIdentifier == null) {
      throw new CelebornRuntimeException("uniqueIdentifier should not be null");
    }
    String[] ids = uniqueIdentifier.split("_");
    return StringUtils.join(ids, "_", 0, 7);
  }

  public static String ensureGetSysEnv(String envName) throws IOException {
    String value = System.getenv(envName);
    if (value == null) {
      String msg = envName + " is null";
      throw new CelebornIOException(msg);
    }
    return value;
  }
}
