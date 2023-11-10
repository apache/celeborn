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

package org.apache.celeborn.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.CelebornConf;

public class MemCacheManager {
  private static MemCacheManager memCacheManager;
  private ConcurrentMap<String, ByteBuf> caches = new ConcurrentHashMap<>();
  private long cacheCapacity;
  private boolean cacheEnable;
  private AtomicLong currentCacheSize = new AtomicLong(0);

  public MemCacheManager(CelebornConf conf) {
    cacheCapacity = conf.workerMemoryCacheCapacity();
    cacheEnable = conf.workerMemoryCacheEnabled() && !conf.hasHDFSStorage();
  }

  public void putCache(String key, ByteBuf cache) {
    int cacheSize = cache.readableBytes();
    caches.put(key, cache);
    currentCacheSize.getAndAdd(cacheSize);
  }

  public boolean canCache(int cacheSize) {
    return cacheEnable && (cacheCapacity > currentCacheSize.get() + cacheSize);
  }

  public void removeCache(String key) {
    ByteBuf cache = caches.remove(key);
    if (cache != null) {
      currentCacheSize.getAndAdd(-1 * cache.readableBytes());
      cache.release();
    }
  }

  public boolean contains(String key) {
    if (!cacheEnable) return false;
    return caches.containsKey(key);
  }

  public ByteBuf getCache(String key) {
    return caches.get(key);
  }

  public static MemCacheManager getMemCacheManager(CelebornConf conf) {
    if (memCacheManager == null) {
      synchronized (MemCacheManager.class) {
        if (memCacheManager == null) {
          memCacheManager = new MemCacheManager(conf);
        }
      }
    }
    return memCacheManager;
  }

  public long getCurrentCacheSize() {
    return currentCacheSize.get();
  }

  public int getCacheFileNum() {
    return caches.size();
  }
}
