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
