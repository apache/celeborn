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

package com.aliyun.emr.rss.common.network.server;

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
//  private long high = 0;
//  private long low = 0;
  private long directMemoryCritical = 0;
//  private AtomicLong storageMemoryTracker = new AtomicLong();
//  private LongAdder writeSpeedCounter = new LongAdder();
//  private LongAdder flushSpeedCounter = new LongAdder();
  private List<MemoryTrackerListener> memoryTrackerListeners = new ArrayList<>();
  private ScheduledExecutorService checkService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-check-thread").build());
  private ScheduledExecutorService reportService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-report-thread").build());
  private AtomicLong directMemoryIndicator = null;
  private static Logger logger = LoggerFactory.getLogger(MemoryTracker.class);
  private static volatile MemoryTracker _INSTANCE = null;

  public static MemoryTracker initialize(double directMemoryCriticalRatio, int checkInterval,
    int reportInterval) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryTracker( directMemoryCriticalRatio, checkInterval,
        reportInterval);
    }
    return _INSTANCE;
  }

  public void registerMemoryListener(MemoryTrackerListener listener) {
    synchronized (memoryTrackerListeners) {
      memoryTrackerListeners.add(listener);
    }
  }

  public static MemoryTracker instance() {
    return _INSTANCE;
  }

  public void oomOccurred() {
    logger.warn("[MemoryTracker] detect OOM exception, " +
                  "current memory usage :${}", toMb(directMemoryIndicator.get()));
    memoryTrackerListeners.stream().forEach(MemoryTrackerListener::onOOM);
  }

  private MemoryTracker(double directMemoryCriticalRatio, int checkInterval, int reportInterval) {
    assert directMemoryCriticalRatio > 0 && directMemoryCriticalRatio < 1;
    directMemoryCritical = (long) (maxDirectorMemory * directMemoryCriticalRatio);
    assert directMemoryCritical > 0;

    checkService.scheduleWithFixedDelay(() -> {
      try {
        if (directMemoryIsCritical()) {
          logger.info("Trigger storage memory on-pressure action");
          memoryTrackerListeners.forEach(MemoryTrackerListener::onMemoryPressure);
        }
      } catch (Exception e) {
        logger.error("Storage memory release on high pressure with error , detail : {}", e);
      }
      if (logger.isDebugEnabled() && directMemoryIndicator != null) {
        logger.debug("Track all direct memory usage :{}/{} ({}/{} MB)",
          directMemoryIndicator.get(), maxDirectorMemory, toMb(directMemoryIndicator.get()),
          toMb(maxDirectorMemory));
      }
    }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(() -> {
      logger.info("Track all direct memory usage :{}/{}",
        toMb(directMemoryIndicator.get()), toMb(maxDirectorMemory));
    }, reportInterval, reportInterval, TimeUnit.SECONDS);

    logger.info("Memory tracker initialized with :  " +
                  "\n max direct memory : {} ({} MB)" +
                  "\n direct memory critical : {} ({} MB)",
      maxDirectorMemory, toMb(maxDirectorMemory),
      directMemoryCritical, toMb(directMemoryCritical));

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
      logger.error("Get netty_direct_memory error, works without direct memory counter," +
                     " reason : {}", e);
    }
  }

  private boolean directMemoryIsCritical() {
    if (directMemoryIndicator == null) {
      return false;
    } else {
      return directMemoryIndicator.get() > directMemoryCritical;
    }
  }
}
