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
import java.util.concurrent.atomic.LongAdder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

import com.aliyun.emr.rss.common.protocol.TransportModuleConstants;

public class MemoryTracker {
  private static final Logger logger = LoggerFactory.getLogger(MemoryTracker.class);
  private static volatile MemoryTracker _INSTANCE = null;
  private final long maxDirectorMemory = VM.maxDirectMemory();
  private long pauseFlowInThreshold;
  private long pauseReplicateThreshold;
  private long resumeFlowInThreshold;
  private long maxSortMemory;
  private final List<MemoryTrackerListener> memoryTrackerListeners = new ArrayList<>();

  private final ScheduledExecutorService checkService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-check-thread").build());
  private final ScheduledExecutorService reportService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-report-thread").build());
  private final ExecutorService actionService = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("MemoryTracker-action-thread").build());

  private AtomicLong nettyMemoryCounter = null;
  private final AtomicLong sortMemoryCounter = new AtomicLong(0);
  private final AtomicLong diskBufferCounter = new AtomicLong(0);
  private final AtomicLong replicateBufferCounter = new AtomicLong(0);
  private final LongAdder pauseFlowInCounter = new LongAdder();
  private final LongAdder pauseFlowInAndReplicateCounter = new LongAdder();
  private MemoryTrackerAction memoryTrackerAction = MemoryTrackerAction.resume;
  private int trimCount = 0;

  public static MemoryTracker initialize(
    double pauseFlowInRatio,
    int checkInterval,
    int reportInterval,
    double maxSortRatio,
    double pauseReplicateRatio,
    double resumeFlowInRatio,
    int trimActionThreshold) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryTracker(pauseReplicateRatio,
        checkInterval,
        reportInterval,
        maxSortRatio,
        pauseFlowInRatio,
        resumeFlowInRatio,
        trimActionThreshold);
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

  private MemoryTracker(
    double pauseFlowInRatio,
    int checkInterval,
    int reportInterval,
    double maxSortMemRatio,
    double pauseReplicateRatio,
    double resumeFlowInRatio,
    int trimActionThreshold) {
    maxSortMemory = ((long) (maxDirectorMemory * maxSortMemRatio));
    pauseFlowInThreshold = (long) (maxDirectorMemory * pauseFlowInRatio);
    pauseReplicateThreshold = (long) (maxDirectorMemory * pauseReplicateRatio);
    resumeFlowInThreshold = (long) (maxDirectorMemory * resumeFlowInRatio);

    initDirectMemoryIndicator();

    checkService.scheduleWithFixedDelay(() -> {
      try {
        MemoryTrackerAction lastAction = memoryTrackerAction;
        memoryTrackerAction = currentMemoryAction();
        if (lastAction != memoryTrackerAction) {
          if (memoryTrackerAction == MemoryTrackerAction.pauseFlowIn) {
            pauseFlowInCounter.increment();
            actionService.submit(() -> {
              logger.info("Trigger pauseFlowIn action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.PUSH_MODULE));
              memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onResume(TransportModuleConstants.REPLICATE_MODULE));
            });
          } else if (memoryTrackerAction == MemoryTrackerAction.pauseFlowInAndReplicate) {
            pauseFlowInAndReplicateCounter.increment();
            actionService.submit(() -> {
              logger.info("Trigger pauseReplicateAndFlowIn action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.PUSH_MODULE));
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onPause(TransportModuleConstants.REPLICATE_MODULE));
              memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
            });
          } else {
            actionService.submit(() -> {
              logger.info("Trigger resume action");
              memoryTrackerListeners.forEach(memoryTrackerListener ->
                memoryTrackerListener.onResume("all"));
            });
          }
        } else {
          if (memoryTrackerAction != MemoryTrackerAction.resume) {
            if (trimCount++ % trimActionThreshold == 0) {
              actionService.submit(() -> {
                logger.info("Trigger trim action");
                memoryTrackerListeners.forEach(MemoryTrackerListener::onTrim);
              });
              trimCount = 0;
            }
          }
        }
      } catch (Exception e) {
        logger.error("Memory tracker check error", e);
      }
    }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(() -> logger.info("Track all direct memory usage :{}/{}," +
        "disk buffer size:{}, sort memory size : {}, replicate buffer size : {}",
        toMb(nettyMemoryCounter.get()), toMb(maxDirectorMemory),
        toMb(diskBufferCounter.get()), toMb(sortMemoryCounter.get()),
        toMb(replicateBufferCounter.get())),
      reportInterval, reportInterval, TimeUnit.SECONDS);

    logger.info("Memory tracker initialized with :  " +
                  "\n max direct memory : {} ({} MB)" +
                  "\n pause flowin memory : {} ({} MB)" +
                  "\n pause replication memory : {} ({} MB)" +
                  "\n resume flowin memory : {} ({} MB)",
      maxDirectorMemory, toMb(maxDirectorMemory),
      pauseFlowInThreshold, toMb(pauseFlowInThreshold),
      pauseReplicateThreshold, toMb(pauseReplicateThreshold),
      resumeFlowInThreshold, toMb(resumeFlowInThreshold));
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
      logger.error("Fatal error, get netty_direct_memory failed, worker should stop", e);
      System.exit(-1);
    }
  }

  public MemoryTrackerAction currentMemoryAction() {
    boolean pauseFlowIn =
      nettyMemoryCounter.get() + sortMemoryCounter.get() > pauseFlowInThreshold;
    boolean pauseReplication =
      nettyMemoryCounter.get() + sortMemoryCounter.get() > pauseReplicateThreshold;
    if (pauseFlowIn) {
      if (pauseReplication) {
        return MemoryTrackerAction.pauseFlowInAndReplicate;
      } else {
        return MemoryTrackerAction.pauseFlowIn;
      }
    } else {
      boolean resumeFlowIn =
        nettyMemoryCounter.get() + sortMemoryCounter.get() < resumeFlowInThreshold;
      if (resumeFlowIn) {
        return MemoryTrackerAction.resume;
      } else {
        return MemoryTrackerAction.pauseFlowIn;
      }
    }
  }

  public interface MemoryTrackerListener {
    void onPause(String moduleName);

    void onResume(String moduleName);

    void onTrim();
  }

  public void reserveSortMemory(long fileLen) {
    sortMemoryCounter.addAndGet(fileLen);
  }

  public boolean sortMemoryReady() {
    return (currentMemoryAction().equals(MemoryTrackerAction.resume)) &&
             sortMemoryCounter.get() < maxSortMemory;
  }

  public void releaseSortMemory(long size) {
    synchronized (this) {
      if (sortMemoryCounter.get() - size < 0) {
        sortMemoryCounter.set(0);
      } else {
        sortMemoryCounter.addAndGet(-1L * size);
      }
    }
  }

  public void incrementDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size);
  }

  public void releaseDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size * -1);
  }

  public AtomicLong getNettyMemoryCounter() {
    return nettyMemoryCounter;
  }

  public AtomicLong getSortMemoryCounter() {
    return sortMemoryCounter;
  }

  public AtomicLong getDiskBufferCounter() {
    return diskBufferCounter;
  }

  public long getPauseFlowInCounter() {
    return pauseFlowInCounter.sum();
  }

  public long getPauseFlowInAndReplicateCounter(){
    return pauseFlowInAndReplicateCounter.sum();
  }

  public AtomicLong getReplicateBufferCounter() {
    return replicateBufferCounter;
  }

  public void increaseReplicateBuffer(long size) {
    replicateBufferCounter.addAndGet(size);
  }

  public void releaseReplicateBuffer(long size) {
    replicateBufferCounter.addAndGet(-1 * size);
  }

  enum MemoryTrackerAction {
    resume, pauseFlowInAndReplicate, pauseFlowIn
  }
}
