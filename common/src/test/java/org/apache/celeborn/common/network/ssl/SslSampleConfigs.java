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

package org.apache.celeborn.common.network.ssl;

import java.io.File;
import java.io.IOException;
import java.security.*;
import java.util.HashMap;
import java.util.Map;

public class SslSampleConfigs {

  public static final String DEFAULT_KEY_STORE_PATH = getAbsolutePath("/ssl/server.jks");
  public static final String SECOND_KEY_STORE_PATH = getAbsolutePath("/ssl/server_another.jks");

  // trust store has ca's for both keys.
  public static final String TRUST_STORE_PATH = getAbsolutePath("/ssl/truststore.jks");

  // this is a trust store which does not have either the primary or second cert's ca
  public static final String TRUST_STORE_WITHOUT_CA =
      getAbsolutePath("/ssl/truststore-without-ca.jks");

  public static Map<String, String> createDefaultConfigMapForModule(String module) {
    return createConfigMapForModule(module, true);
  }

  public static Map<String, String> createAnotherConfigMapForModule(String module) {
    return createConfigMapForModule(module, false);
  }

  private static Map<String, String> createConfigMapForModule(String module, boolean forDefault) {
    Map<String, String> confMap = new HashMap<>();
    confMap.put("celeborn.ssl." + module + ".enabled", "true");
    confMap.put("celeborn.ssl." + module + ".trustStoreReloadingEnabled", "false");
    confMap.put("celeborn.ssl." + module + ".openSslEnabled", "false");
    confMap.put("celeborn.ssl." + module + ".trustStoreReloadIntervalMs", "10000");
    if (forDefault) {
      confMap.put("celeborn.ssl." + module + ".keyStore", DEFAULT_KEY_STORE_PATH);
    } else {
      confMap.put("celeborn.ssl." + module + ".keyStore", SECOND_KEY_STORE_PATH);
    }
    confMap.put("celeborn.ssl." + module + ".keyStorePassword", "password");
    confMap.put("celeborn.ssl." + module + ".keyPassword", "password");
    confMap.put("celeborn.ssl." + module + ".privateKeyPassword", "password");
    confMap.put("celeborn.ssl." + module + ".protocol", "TLSv1.2");
    confMap.put("celeborn.ssl." + module + ".trustStore", TRUST_STORE_PATH);
    confMap.put("celeborn.ssl." + module + ".trustStorePassword", "password");
    return confMap;
  }

  public static String getAbsolutePath(String path) {
    try {
      return new File(SslSampleConfigs.class.getResource(path).getFile()).getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException("Failed to resolve path " + path, e);
    }
  }
}
