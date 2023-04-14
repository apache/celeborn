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

public class MapPartitionFetcher {
  public static final Logger logger = LoggerFactory.getLogger(MapPartitionFetcher.class);
  private LinkedBlockingQueue<Runnable>[] workingQueues;
  private Thread[] threads;
  private AtomicInteger fetchIndex = new AtomicInteger();
  private int threadCount;

  public MapPartitionFetcher(String mountPoint, int threads) {
    this.threads = new Thread[threads];
    workingQueues = new LinkedBlockingQueue[threads];
    this.threadCount = threads;
    for (int i = 0; i < threads; i++) {
      final int threadIndex = i;
      workingQueues[i] = new LinkedBlockingQueue<>();
      this.threads[i] =
          new Thread(
              () -> {
                while (true) {
                  try {
                    Runnable task = workingQueues[threadIndex].take();
                    task.run();
                  } catch (Exception e) {
                    logger.error(
                        "Map partition fetcher {} failed",
                        mountPoint + "-fetch-thread-" + threadIndex,
                        e);
                  }
                }
              },
              mountPoint + "-fetch-thread-" + threadIndex);
      this.threads[i].start();
    }
  }

  public void addTask(int fetchIndex, Runnable runnable) {
    if (!workingQueues[fetchIndex].offer(runnable)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.warn("MapPartitionFetcher is closing now.");
      }
      if (!workingQueues[fetchIndex].offer(runnable)) {
        throw new RuntimeException("Fetcher " + fetchIndex + " failed to retry add task");
      }
    }
  }

  public int getFetcherIndex() {
    return fetchIndex.incrementAndGet() % threadCount;
  }
}
