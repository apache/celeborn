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

package org.apache.celeborn.common.network;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.ssl.SslSampleConfigs;

/** A few helper utilities to reduce duplication within test code. */
public class TestHelper {

  public static CelebornConf updateCelebornConfWithMap(CelebornConf conf, Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  public static String getResourceAsAbsolutePath(String path) {
    try {
      File tempFile = File.createTempFile(new File(path).getName(), null);
      tempFile.deleteOnExit();
      URL url = SslSampleConfigs.class.getResource(path);
      if (null == url) {
        throw new IllegalArgumentException("Unable to find " + path);
      }
      FileUtils.copyInputStreamToFile(url.openStream(), tempFile);
      return tempFile.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException("Failed to resolve path " + path, e);
    }
  }
}
