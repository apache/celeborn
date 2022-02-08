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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

public class MemoryTracker {
  private static Logger logger = LoggerFactory.getLogger(MemoryTracker.class);
  private static volatile MemoryTracker _INSTANCE = null;

  private long maxDirectorMemory = VM.maxDirectMemory();
  private long offheapMemoryCriticalThreshold = 0;
  private List<MemoryTrackerListener> memoryTrackerListeners = new ArrayList<>();
  private ScheduledExecutorService checkService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-check-thread").build());
  private ScheduledExecutorService reportService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-report-thread").build());
  private ExecutorService actionService = Executors
    .newFixedThreadPool(4, new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("MemoryTracker-action-thread").build());
  private AtomicLong nettyMemoryCounter = null;
  private AtomicLong reserveForSort = new AtomicLong(0);

  public static MemoryTracker initialize(double directMemoryCriticalRatio, int checkInterval,
    int reportInterval) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryTracker(directMemoryCriticalRatio, checkInterval, reportInterval);
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

  private MemoryTracker(double directMemoryCriticalRatio, int checkInterval, int reportInterval) {
    assert directMemoryCriticalRatio > 0 && directMemoryCriticalRatio < 1;
    offheapMemoryCriticalThreshold = (long) (maxDirectorMemory * directMemoryCriticalRatio);
    assert offheapMemoryCriticalThreshold > 0;

    initDirectMemoryIndicator();

    checkService.scheduleWithFixedDelay(() -> {
      try {
        if (directMemoryCritical()) {
          logger.info("Trigger storage memory critical action");
          actionService.submit(() -> memoryTrackerListeners
            .forEach(MemoryTrackerListener::onMemoryCritical));
        }
      } catch (Exception e) {
        logger.error("Storage memory release on high pressure with error , detail : {}", e);
      }
    }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(() -> logger.info("Track all direct memory usage :{}/{}",
        toMb(nettyMemoryCounter.get()), toMb(maxDirectorMemory)), reportInterval,
      reportInterval, TimeUnit.SECONDS);

    logger.info("Memory tracker initialized with :  " +
                  "\n max direct memory : {} ({} MB)" +
                  "\n direct memory critical : {} ({} MB)",
      maxDirectorMemory, toMb(maxDirectorMemory),
      offheapMemoryCriticalThreshold, toMb(offheapMemoryCriticalThreshold));
  }

  private double toMb(long bytes) {
    return bytes / 1024.0 / 1024.0;
  }

  private void initDirectMemoryIndicator() {
    try {
      Field field = null;
      Field[] result = PlatformDependent.class.getDeclaredFields();
      for (Field tf : result) {
        if ("DIRECT_MEMORY_COUNTER".equals(tf.getName())) {
          field = tf;
        }
      }
      field.setAccessible(true);
      nettyMemoryCounter = ((AtomicLong) field.get(PlatformDependent.class));
    } catch (Exception e) {
      logger.error("Fatal error, get netty_direct_memory failed, worker should stop, detail : {}",
        e);
      System.exit(-1);
    }
  }

  public boolean directMemoryCritical() {
    return nettyMemoryCounter.get() + reserveForSort.get() > offheapMemoryCriticalThreshold;
  }

  public interface MemoryTrackerListener {
    void onMemoryCritical();
  }

  public void reserveSortMemory(int fileLen) {
    reserveForSort.addAndGet(fileLen);
  }

  public boolean sortMemoryReady() {
    return !directMemoryCritical();
  }

  public void releaseSortMemory(int filelen) {
    synchronized (this) {
      if (reserveForSort.get() - filelen < 0) {
        reserveForSort.set(0);
      } else {
        reserveForSort.addAndGet(-1L * filelen);
      }
    }
  }
}
