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

package org.apache.celeborn.service.deploy.worker;

import java.nio.charset.StandardCharsets;

import org.apache.celeborn.service.deploy.worker.shuffledb.StoreVersion;

public abstract class ShuffleRecoverHelper {
  protected StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

  protected byte[] dbShuffleKey(String shuffleKey) {
    return (getKeyPrefix() + ";" + shuffleKey).getBytes(StandardCharsets.UTF_8);
  }

  protected String parseDbShuffleKey(String s) {
    if (!s.startsWith(getKeyPrefix())) {
      throw new IllegalArgumentException("Expected a string starting with " + getKeyPrefix());
    }
    return s.substring(getKeyPrefix().length() + 1);
  }

  protected String getKeyPrefix() {
    return "SHUFFLE-KEY";
  }
}
