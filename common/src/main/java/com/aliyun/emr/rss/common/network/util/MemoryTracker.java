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

package com.aliyun.emr.rss.common.network.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

public class MemoryTracker {
  private long maxDirectorMemory = VM.maxDirectMemory();
  private long high = 0;
  private long low = 0;
  private long directMemoryCritical = 0;
  private AtomicLong storageMemoryTracker = new AtomicLong();
  private LongAdder writeSpeedCounter = new LongAdder();
  private LongAdder flushSpeedCounter = new LongAdder();
  private List<MemoryTrackerListener> listeners = new ArrayList<>();
  private ScheduledExecutorService scheduledExecutorService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-check-thread").build());
  private ScheduledExecutorService reportService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-report-thread").build());
  private AtomicLong directMemoryIndicator = null;
  private static Logger logger = LoggerFactory.getLogger(MemoryTracker.class);
  private static volatile MemoryTracker _INSTANCE = null;

  public static MemoryTracker initialize(double highRatio, double lowRatio,
    double directMemoryCriticalRatio, int interval) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryTracker(highRatio, lowRatio, directMemoryCriticalRatio, interval);
    }
    return _INSTANCE;
  }

  public void registerMemoryListener(MemoryTrackerListener listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  public static MemoryTracker instance() {
    return _INSTANCE;
  }

  public void oomOccurred() {
    high = currentValue() - 16 * 1024 * 1024;
    low = (long) (high * 0.8);
    logger.warn("[MemoryTracker] detect OOM exception, " +
                  "set new high {} ({} MB) ,set new low {} ({} MB)", high, toMb(high),
      low, toMb(low));
    listeners.stream().forEach(MemoryTrackerListener::onOOM);
  }

  private MemoryTracker(double highRatio, double lowRatio,
    double directMemoryCriticalRatio, int interval) {
    assert highRatio > 0 && highRatio < 1;
    assert lowRatio > 0 && lowRatio < 1;
    assert directMemoryCriticalRatio > 0 && directMemoryCriticalRatio < 1;
    high = (long) (maxDirectorMemory * highRatio);
    low = (long) (maxDirectorMemory * lowRatio);
    directMemoryCritical = (long) (maxDirectorMemory * directMemoryCriticalRatio);
    assert high > 0;
    assert low > 0;
    assert directMemoryCritical > 0;

    scheduledExecutorService.scheduleWithFixedDelay(() -> {
      try {
        if (needFreeMemory()) {
          logger.info("Trigger storage memory on-pressure action");
          listeners.forEach(MemoryTrackerListener::onMemoryPressure);
        }
      } catch (Exception e) {
        logger.error("Storage memory release on high pressure with error");
      }
      if (logger.isDebugEnabled()) {
        if (storageMemoryTracker.get() != 0) {
          logger.debug("Storage used memory :{} MB",
            toMb(storageMemoryTracker.get()));
          if (directMemoryIndicator != null) {
            logger.debug("Track all direct memory usage :{}/{} ({}/{} MB)",
              directMemoryIndicator.get(), maxDirectorMemory, toMb(directMemoryIndicator.get()),
              toMb(maxDirectorMemory));
          }
        }
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(() -> {
      if (storageMemoryTracker.get() != 0) {
        long writeSpeed = writeSpeedCounter.sumThenReset();
        long flushSpeed = flushSpeedCounter.sumThenReset();
        logger.info("Storage used memory :{} MB",
          toMb(storageMemoryTracker.get()));
        logger.info("Storage memory write speed :{} MB/s",
          toMb(writeSpeed) / 10);
        logger.info("Storage memory flush speed :{} MB/s",
          toMb(flushSpeed) / 10);
        if (directMemoryIndicator != null) {
          logger.info("Track all direct memory usage :{}/{}",
            toMb(directMemoryIndicator.get()), toMb(maxDirectorMemory));
        }
      }
    }, 10, 10, TimeUnit.SECONDS);

    logger.info("Storage memory tracker initialized with :  " +
                  "\n max direct memory : {} ({} MB)" +
                  "\n direct memory critical : {} ({} MB)" +
                  "\n high water mark : {} ({} MB)" +
                  "\n low water mark : {} ({} MB)", maxDirectorMemory, toMb(maxDirectorMemory),
      directMemoryCritical, toMb(directMemoryCritical), high, toMb(high), low, toMb(low));

    initDirectMemoryIndicator();
  }

  private double toMb(long bytes) {
    return bytes / 1024.0 / 1024.0;
  }

  private void initDirectMemoryIndicator() {
    Field field = null;
    Field[] result = PlatformDependent.class.getDeclaredFields();
    for (Field tf : result) {
      if ("DIRECT_MEMORY_COUNTER".equals(tf.getName())) {
        field = tf;
      }
    }
    field.setAccessible(true);
    try {
      directMemoryIndicator = ((AtomicLong) field.get(PlatformDependent.class));
    } catch (IllegalAccessException e) {
      logger.warn("Get netty_direct_memory error, reason : {}", e);
      logger.warn("Works without direct memory counter");
    }
  }

  private boolean directMemoryIsCritical() {
    if (directMemoryIndicator == null) {
      return false;
    } else {
      return directMemoryIndicator.get() > directMemoryCritical;
    }
  }

  public void increment(long inc) {
    storageMemoryTracker.addAndGet(inc);
    writeSpeedCounter.add(inc);
  }

  public void decrement(long dec) {
    storageMemoryTracker.addAndGet(-1 * dec);
    flushSpeedCounter.add(dec);
  }

  public boolean highPressure() {
    return storageMemoryTracker.get() > high || directMemoryIsCritical();
  }

  public boolean normalPressure() {
    return storageMemoryTracker.get() < low && !directMemoryIsCritical();
  }

  public boolean needFreeMemory() {
    return storageMemoryTracker.get() > low || directMemoryIsCritical();
  }

  public long currentValue() {
    return storageMemoryTracker.get();
  }
}
