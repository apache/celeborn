package org.apache.celeborn.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.CelebornConf;

public class MemCacheManager {
  private static MemCacheManager memCacheManager;
  private ConcurrentMap<String, ByteBuf> caches = new ConcurrentHashMap<>();
  private CelebornConf conf;
  private long maxCacheSize;
  private boolean cacheEnable;
  private AtomicLong currentCacheSize = new AtomicLong(0);

  public MemCacheManager(CelebornConf conf) {
    this.conf = conf;
    maxCacheSize = this.conf.fileCacheMaxSize();
    cacheEnable = this.conf.fileCacheEnable();
  }

  public void putCache(String key, ByteBuf cache) {
    int cacheSize = cache.readableBytes();
    caches.put(key, cache);
    currentCacheSize.getAndAdd(cacheSize);
  }

  public boolean canCache(int cacheSize) {
    return cacheEnable && (maxCacheSize > currentCacheSize.get() + cacheSize);
  }

  public void removeCache(String key) {
    ByteBuf cache = caches.remove(key);
    currentCacheSize.getAndAdd(-1 * cache.readableBytes());
  }

  public boolean contains(String key) {
    if (!cacheEnable) return false;
    return caches.containsKey(key);
  }

  public ByteBuf getCache(String key) {
    return caches.get(key);
  }

  public static synchronized MemCacheManager getMemCacheManager(CelebornConf conf) {
    if (memCacheManager == null) {
      memCacheManager = new MemCacheManager(conf);
    }
    return memCacheManager;
  }

  public static synchronized MemCacheManager getMemCacheManager() {
    if (memCacheManager == null) {
      memCacheManager = new MemCacheManager(new CelebornConf());
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
