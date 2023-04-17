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

package org.apache.celeborn.common.network.server;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapPartitionWorker {
  public static final Logger logger = LoggerFactory.getLogger(MapPartitionWorker.class);
  private LinkedBlockingQueue<Runnable>[] fetcherTaskQueue;
  private LinkedBlockingQueue<Runnable>[] senderTaskQueue;
  private Thread[] fetcherThreads;
  private Thread[] senderThreads;
  private AtomicInteger fetcherIndex = new AtomicInteger();
  private AtomicInteger senderIndex = new AtomicInteger();
  private int threadCount;
  private String mountPoint;

  public MapPartitionWorker(String mountPoint, int threads) {
    this.mountPoint = mountPoint;
    fetcherThreads = new Thread[threads];
    fetcherTaskQueue = new LinkedBlockingQueue[threads];
    senderThreads = new Thread[threads];
    senderTaskQueue = new LinkedBlockingQueue[threads];

    threadCount = threads;

    for (int i = 0; i < threads; i++) {
      final int threadIndex = i;
      fetcherTaskQueue[i] = new LinkedBlockingQueue<>();
      senderTaskQueue[i] = new LinkedBlockingQueue<>();
      fetcherThreads[i] =
          new Thread(
              () -> {
                processTasks(fetcherTaskQueue[threadIndex]);
              },
              mountPoint + "-fetch-thread-" + threadIndex);
      fetcherThreads[i].start();
      senderThreads[i] =
          new Thread(
              () -> {
                processTasks(senderTaskQueue[threadIndex]);
              },
              mountPoint + "-send-thread-" + threadIndex);
      senderThreads[i].start();
    }
  }

  private void processTasks(LinkedBlockingQueue<Runnable> taskQueue) {
    while (true) {
      try {
        Runnable task = taskQueue.take();
        task.run();
      } catch (Exception e) {
        logger.error("Map partition worker {} failed ", mountPoint, e);
      }
    }
  }

  private void addTask(int index, Runnable task, boolean isFetch) {
    LinkedBlockingQueue taskQueue;
    if (isFetch) {
      taskQueue = fetcherTaskQueue[index];
    } else {
      taskQueue = senderTaskQueue[index];
    }
    if (!taskQueue.offer(task)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.warn("Map partition worker is closing now.");
      }
      if (!taskQueue.offer(task)) {
        throw new RuntimeException("Read or send task queue is full, failed to retry add task");
      }
    }
  }

  public void addFetchTask(int fetchIndex, Runnable task) {
    addTask(fetchIndex, task, true);
  }

  public void addSendTask(int sendIndex, Runnable task) {
    addTask(sendIndex, task, false);
  }

  public int getFetcherIndex() {
    return fetcherIndex.incrementAndGet() % threadCount;
  }

  public int getSenderIndex() {
    return senderIndex.incrementAndGet() % threadCount;
  }
}
