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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.iq80.leveldb.DB;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;

public abstract class ShuffleRecovery {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ShuffleRecovery.class);
  protected String SHUFFLE_KEY_PREFIX = "SHUFFLE-KEY";
  protected LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0);
  protected DB db = null;

  public ShuffleRecovery(RssConf conf) {
    // ShuffleClient can fetch shuffle data from a restarted worker only
    // when the worker's fetching port is stable and enables graceful shutdown.
    if (RssConf.workerGracefulShutdown(conf)) {
      File recoverFile = new File(RssConf.workerRecoverPath(conf), recoverFileName());
      try {
        this.db = LevelDBProvider.initLevelDB(recoverFile, CURRENT_VERSION);
        reloadAndCleanDB();
      } catch (Exception e) {
        logger.error("Failed to reload LevelDB for sorted shuffle files from: " + recoverFile, e);
      }
    }
  }

  protected abstract String recoverFileName();

  protected abstract void reloadAndCleanDBContent();

  protected abstract void updateDBContent();

  protected byte[] dbShuffleKey(String shuffleKey) {
    return (SHUFFLE_KEY_PREFIX + ";" + shuffleKey).getBytes(StandardCharsets.UTF_8);
  }

  protected String parseDbShuffleKey(String s) {
    if (!s.startsWith(SHUFFLE_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + SHUFFLE_KEY_PREFIX);
    }
    return s.substring(SHUFFLE_KEY_PREFIX.length() + 1);
  }

  protected void reloadAndCleanDB() {
    if (db != null) {
      reloadAndCleanDBContent();
    }
  }

  protected void updateAndCloseDB() {
    if (db != null) {
      try {
        updateDBContent();
        db.close();
      } catch (IOException e) {
        logger.error("Store recover data to LevelDB failed.", e);
      }
    }
  }

  protected void close() {
    updateAndCloseDB();
  }
}
