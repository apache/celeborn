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

package org.apache.celeborn.common.network.server.memory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.server.ChannelsLimiter;
import org.apache.celeborn.common.network.server.CreditStreamManager;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;

public class MemoryManager {
  private static final Logger logger = LoggerFactory.getLogger(MemoryManager.class);
  private static volatile MemoryManager _INSTANCE = null;
  private long maxDirectorMemory = 0;
  private final long pausePushDataThreshold;
  private final long pauseReplicateThreshold;
  private final long resumeThreshold;
  private final long maxSortMemory;
  private final List<MemoryPressureListener> memoryPressureListeners = new ArrayList<>();

  private final ScheduledExecutorService checkService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("memory-manager-checker");

  private final ScheduledExecutorService reportService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("memory-manager-reporter");

  private AtomicLong nettyMemoryCounter = null;
  private final AtomicLong sortMemoryCounter = new AtomicLong(0);
  private final AtomicLong diskBufferCounter = new AtomicLong(0);
  private final LongAdder pausePushDataCounter = new LongAdder();
  private final LongAdder pausePushDataAndReplicateCounter = new LongAdder();
  private MemoryManagerStat memoryManagerStat = MemoryManagerStat.resumeAll;
  private boolean underPressure;

  // For credit stream
  private final AtomicLong readBufferCounter = new AtomicLong(0);
  private long readBufferThreshold = 0;
  private long readBufferTarget = 0;
  private ReadBufferDispatcher readBufferDispatcher;
  private List<ReadBufferTargetChangeListener> readBufferTargetChangeListeners;
  private long lastNotifiedTarget = 0;
  private final ScheduledExecutorService readBufferTargetUpdateService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "memory-mananger-readBufferTarget-updater");
  private CreditStreamManager creditStreamManager = null;

  // For memory shuffle storage
  private final AtomicLong memoryShuffleStorageCounter = new AtomicLong(0);
  private long memoryShuffleStorageThreshold = 0;

  public static MemoryManager initialize(CelebornConf conf) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryManager(conf);
    }
    return _INSTANCE;
  }

  public void registerMemoryListener(MemoryPressureListener listener) {
    synchronized (memoryPressureListeners) {
      memoryPressureListeners.add(listener);
      if (listener instanceof ChannelsLimiter) {
        channelsLimiters.add(listener);
      } else {
        otherListeners.add(listener);
      }
    }
  }

  public static MemoryManager instance() {
    return _INSTANCE;
  }

  private MemoryManager(CelebornConf conf) {
    double pausePushDataRatio = conf.workerDirectMemoryRatioToPauseReceive();
    double pauseReplicateRatio = conf.workerDirectMemoryRatioToPauseReplicate();
    double resumeRatio = conf.workerDirectMemoryRatioToResume();
    double maxSortMemRatio = conf.partitionSorterDirectMemoryRatioThreshold();
    double readBufferRatio = conf.workerDirectMemoryRatioForReadBuffer();
    double shuffleStorageRatio = conf.workerDirectMemoryRatioForShuffleStorage();
    long checkInterval = conf.workerDirectMemoryPressureCheckIntervalMs();
    long reportInterval = conf.workerDirectMemoryReportIntervalSecond();
    long readBufferAllocationWait = conf.readBufferAllocationWait();
    double readBufferTargetRatio = conf.readBufferTargetRatio();
    long readBufferTargetUpdateInterval = conf.readBufferTargetUpdateInterval();
    long readBufferTargetNotifyThreshold = conf.readBufferTargetNotifyThreshold();

    String[] provider;
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_10)) {
      provider = new String[] {"jdk.internal.misc.VM", "maxDirectMemory"};
    } else {
      provider = new String[] {"sun.misc.VM", "maxDirectMemory"};
    }

    Method maxMemMethod;
    String clazz = provider[0];
    String method = provider[1];
    try {
      Class<?> vmClass = Class.forName(clazz);
      maxMemMethod = vmClass.getDeclaredMethod(method);

      maxDirectorMemory = (long) maxMemMethod.invoke(null);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException ignored) {
      System.out.println("exception " + ignored);
      // Ignore Exception
    }
    Preconditions.checkArgument(maxDirectorMemory > 0);
    Preconditions.checkArgument(pauseReplicateRatio > pausePushDataRatio);
    Preconditions.checkArgument(pausePushDataRatio > resumeRatio);
    Preconditions.checkArgument(resumeRatio > (readBufferRatio + shuffleStorageRatio));

    maxSortMemory = ((long) (maxDirectorMemory * maxSortMemRatio));
    pausePushDataThreshold = (long) (maxDirectorMemory * pausePushDataRatio);
    pauseReplicateThreshold = (long) (maxDirectorMemory * pauseReplicateRatio);
    resumeThreshold = (long) (maxDirectorMemory * resumeRatio);
    readBufferThreshold = (long) (maxDirectorMemory * readBufferRatio);
    readBufferTarget = (long) (readBufferThreshold * readBufferTargetRatio);
    memoryShuffleStorageThreshold = (long) (maxDirectorMemory * shuffleStorageRatio);

    initDirectMemoryIndicator();

    checkService.scheduleWithFixedDelay(
        () -> {
          try {
            MemoryManagerStat lastAction = memoryManagerStat;
            memoryManagerStat = currentMemoryAction();
            if (lastAction != memoryManagerStat) {
              logger.info(
                  "Memory manager actions transformed {} -> {}", lastAction, memoryManagerStat);
              if (memoryManagerStat == MemoryManagerStat.pausePushDataAndResumeReplicate) {
                pausePushDataCounter.increment();
                logger.info("Trigger pausePushDataAndResumeReplicate action");
                memoryPressureListeners.forEach(
                    memoryPressureListener ->
                        memoryPressureListener.onPause(TransportModuleConstants.PUSH_MODULE));
                memoryPressureListeners.forEach(MemoryPressureListener::onTrim);
                memoryPressureListeners.forEach(
                    memoryPressureListener ->
                        memoryPressureListener.onResume(TransportModuleConstants.REPLICATE_MODULE));
              } else if (memoryManagerStat == MemoryManagerStat.pausePushDataAndReplicate) {
                pausePushDataAndReplicateCounter.increment();
                logger.info("Trigger pausePushDataAndReplicate action");
                memoryPressureListeners.forEach(
                    memoryPressureListener ->
                        memoryPressureListener.onPause(TransportModuleConstants.PUSH_MODULE));
                memoryPressureListeners.forEach(
                    memoryPressureListener ->
                        memoryPressureListener.onPause(TransportModuleConstants.REPLICATE_MODULE));
                memoryPressureListeners.forEach(MemoryPressureListener::onTrim);
              } else {
                logger.info("Trigger resume action");
                memoryPressureListeners.forEach(
                    memoryPressureListener -> memoryPressureListener.onResume("all"));
              }
            } else {
              if (memoryManagerStat != MemoryManagerStat.resumeAll) {
                logger.debug("Trigger trim action");
                memoryPressureListeners.forEach(MemoryPressureListener::onTrim);
              }
            }
          } catch (Exception e) {
            logger.error("Memory tracker check error", e);
          }
        },
        checkInterval,
        checkInterval,
        TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(
        () ->
            logger.info(
                "Direct memory usage: {}/{}, disk buffer size: {}, sort memory size: {}, read buffer size: {}",
                Utils.bytesToString(nettyMemoryCounter.get()),
                Utils.bytesToString(maxDirectorMemory),
                Utils.bytesToString(diskBufferCounter.get()),
                Utils.bytesToString(sortMemoryCounter.get()),
                Utils.bytesToString(readBufferCounter.get())),
        reportInterval,
        reportInterval,
        TimeUnit.SECONDS);

    if (readBufferThreshold > 0) {
      // if read buffer threshold is zero means that there will be no map data partitions
      readBufferDispatcher = new ReadBufferDispatcher(this, readBufferAllocationWait);
      readBufferTargetChangeListeners = new ArrayList<>();
      readBufferTargetUpdateService.scheduleWithFixedDelay(
          () -> {
            try {
              if (creditStreamManager != null) {
                int mapDataPartitionCount = creditStreamManager.getActiveMapPartitionCount();
                if (mapDataPartitionCount > 0) {
                  long currentTarget =
                      (long) Math.ceil(readBufferTarget * 1.0 / mapDataPartitionCount);
                  if (Math.abs(lastNotifiedTarget - currentTarget)
                      > readBufferTargetNotifyThreshold) {
                    synchronized (readBufferTargetChangeListeners) {
                      logger.debug(
                          "read buffer target changed {} -> {} active map partition count {}",
                          lastNotifiedTarget,
                          currentTarget,
                          mapDataPartitionCount);
                      for (ReadBufferTargetChangeListener changeListener :
                          readBufferTargetChangeListeners) {
                        changeListener.onChange(currentTarget);
                      }
                      lastNotifiedTarget = currentTarget;
                    }
                  }
                }
              }
            } catch (Exception e) {
              logger.warn("Failed update buffer target", e);
            }
          },
          readBufferTargetUpdateInterval,
          readBufferTargetUpdateInterval,
          TimeUnit.MILLISECONDS);
    }

    logger.info(
        "Memory tracker initialized with: "
            + "max direct memory: {}, pause pushdata memory: {}, "
            + "pause replication memory: {}, resume memory: {},"
            + "read buffer memory limit: {} target :{} , memory shuffle storage limit : {}",
        Utils.bytesToString(maxDirectorMemory),
        Utils.bytesToString(pausePushDataThreshold),
        Utils.bytesToString(pauseReplicateThreshold),
        Utils.bytesToString(resumeThreshold),
        Utils.bytesToString(readBufferThreshold),
        Utils.bytesToString(readBufferTarget),
        Utils.bytesToString(memoryShuffleStorageThreshold));
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

  public MemoryManagerStat currentMemoryAction() {
    long memoryUsage = getMemoryUsage();
    boolean pausePushData = memoryUsage > pausePushDataThreshold;
    boolean pauseReplication = memoryUsage > pauseReplicateThreshold;
    if (pausePushData) {
      underPressure = true;
      if (pauseReplication) {
        return MemoryManagerStat.pausePushDataAndReplicate;
      } else {
        return MemoryManagerStat.pausePushDataAndResumeReplicate;
      }
    } else {
      boolean resume = memoryUsage < resumeThreshold;
      if (resume) {
        underPressure = false;
        return MemoryManagerStat.resumeAll;
      } else {
        if (underPressure) {
          return MemoryManagerStat.pausePushDataAndResumeReplicate;
        } else {
          return MemoryManagerStat.resumeAll;
        }
      }
    }
  }

  public void trimAllListeners() {
    memoryPressureListeners.forEach(MemoryPressureListener::onTrim);
  }

  public void reserveSortMemory(long fileLen) {
    sortMemoryCounter.addAndGet(fileLen);
  }

  public boolean sortMemoryReady() {
    return (currentMemoryAction().equals(MemoryManagerStat.resumeAll))
        && sortMemoryCounter.get() < maxSortMemory;
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

  public long getMemoryUsage() {
    return nettyMemoryCounter.get() + sortMemoryCounter.get();
  }

  public AtomicLong getSortMemoryCounter() {
    return sortMemoryCounter;
  }

  public AtomicLong getDiskBufferCounter() {
    return diskBufferCounter;
  }

  public long getReadBufferCounter() {
    return readBufferCounter.get();
  }

  public long getPausePushDataCounter() {
    return pausePushDataCounter.sum();
  }

  public void requestReadBuffers(ReadBufferRequest request) {
    readBufferDispatcher.addBufferRequest(request);
  }

  public void recycleReadBuffer(ByteBuf readBuf) {
    readBufferDispatcher.recycle(readBuf);
  }

  protected void changeReadBufferCounter(int delta) {
    readBufferCounter.addAndGet(delta);
  }

  protected boolean readBufferAvailable(int requiredBytes) {
    return readBufferCounter.get() + requiredBytes < readBufferThreshold;
  }

  public long getPausePushDataAndReplicateCounter() {
    return pausePushDataAndReplicateCounter.sum();
  }

  public long getAllocatedReadBuffers() {
    return readBufferDispatcher.getAllocatedReadBuffers();
  }

  public int dispatchRequestsLength() {
    return readBufferDispatcher.requestsLength();
  }

  public void addReadBufferTargetChangeListener(ReadBufferTargetChangeListener listener) {
    synchronized (readBufferTargetChangeListeners) {
      readBufferTargetChangeListeners.add(listener);
    }
  }

  public void removeReadBufferTargetChangeListener(ReadBufferTargetChangeListener listener) {
    synchronized (readBufferTargetChangeListeners) {
      readBufferTargetChangeListeners.remove(listener);
    }
  }

  public void setCreditStreamManager(CreditStreamManager creditStreamManager) {
    this.creditStreamManager = creditStreamManager;
  }

  public void close() {
    checkService.shutdown();
    reportService.shutdown();
    readBufferTargetUpdateService.shutdown();
    memoryPressureListeners.clear();
    readBufferTargetChangeListeners.clear();
    readBufferDispatcher.close();
  }

  public interface MemoryPressureListener {
    void onPause(String moduleName);

    void onResume(String moduleName);

    void onTrim();
  }

  public interface ReadBufferTargetChangeListener {
    void onChange(long newMemoryTarget);
  }

  enum MemoryManagerStat {
    resumeAll,
    pausePushDataAndReplicate,
    pausePushDataAndResumeReplicate
  }
}
