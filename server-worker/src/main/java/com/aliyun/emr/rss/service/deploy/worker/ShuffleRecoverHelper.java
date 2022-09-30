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

package com.aliyun.emr.rss.service.deploy.worker;

import java.nio.charset.StandardCharsets;

import com.aliyun.emr.rss.common.protocol.message.ControlMessages;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier;

public abstract class ShuffleRecoverHelper {
  protected String SHUFFLE_KEY_PREFIX = "SHUFFLE-KEY";
  protected String USER_IDENTIFIER_PREFIX = "USER_IDENTIFIER";
  protected LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0);

  protected byte[] dbShuffleKey(String shuffleKey) {
    return (SHUFFLE_KEY_PREFIX + ";" + shuffleKey).getBytes(StandardCharsets.UTF_8);
  }

  protected String parseDbShuffleKey(String s) {
    if (!s.startsWith(SHUFFLE_KEY_PREFIX)) {
      throw new IllegalArgumentException("Expected a string starting with " + SHUFFLE_KEY_PREFIX);
    }
    return s.substring(SHUFFLE_KEY_PREFIX.length() + 1);
  }

  protected byte[] dbUserIdentifier(UserIdentifier userIdentifier) {
    return (USER_IDENTIFIER_PREFIX + ";" + userIdentifier.toString()).getBytes(StandardCharsets.UTF_8);
  }

  protected UserIdentifier parseDbUserIdentifier(String s) {
    if(!s.startsWith(USER_IDENTIFIER_PREFIX)) {
      throw new IllegalArgumentException("Expected a string starting with " + USER_IDENTIFIER_PREFIX);
    }
    return ControlMessages.UserIdentifier$.MODULE$.apply(s.substring(USER_IDENTIFIER_PREFIX.length() + 1));
  }
}
