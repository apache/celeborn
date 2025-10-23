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

package org.apache.celeborn.service.deploy.worker.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.ThreadUtils;

public class ReadBufferDispatcher {
  private final Logger logger = LoggerFactory.getLogger(ReadBufferDispatcher.class);
  private final LinkedBlockingQueue<ReadBufferRequest> requests = new LinkedBlockingQueue<>();
  private final MemoryManager memoryManager;
  private final ByteBufAllocator readBufferAllocator;
  private final LongAdder allocatedReadBuffers = new LongAdder();
  private final long readBufferAllocationWait;
  @VisibleForTesting public volatile boolean stopFlag = false;
  @VisibleForTesting public final AtomicReference<Thread> dispatcherThread;

  public ReadBufferDispatcher(
      MemoryManager memoryManager, CelebornConf conf, AbstractSource source) {
    readBufferAllocationWait = conf.readBufferAllocationWait();
    long checkThreadInterval = conf.readBufferDispatcherCheckThreadInterval();
    // readBuffer is not a module name, it's a placeholder.
    readBufferAllocator =
        NettyUtils.getByteBufAllocator(new TransportConf("readBuffer", conf), source, true);
    this.memoryManager = memoryManager;
    dispatcherThread =
        new AtomicReference<>(
            ThreadUtils.newThread(new DispatcherRunnable(), "worker-read-buffer-dispatcher"));
    dispatcherThread.get().start();

    if (checkThreadInterval > 0) {
      ScheduledExecutorService checkAliveThread =
          ThreadUtils.newDaemonSingleThreadScheduledExecutor(
              "worker-read-buffer-dispatcher-checker");
      checkAliveThread.scheduleWithFixedDelay(
          () -> {
            if (!dispatcherThread.get().isAlive()) {
              dispatcherThread.set(
                  ThreadUtils.newThread(new DispatcherRunnable(), "worker-read-buffer-dispatcher"));
              dispatcherThread.get().start();
            }
          },
          checkThreadInterval,
          checkThreadInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  public void addBufferRequest(ReadBufferRequest request) {
    requests.add(request);
  }

  public void recycle(ByteBuf buf) {
    int bufferSize = buf.capacity();
    int refCnt = buf.refCnt();
    // If a reader failed, related read buffers will have more than one reference count
    if (refCnt > 0) {
      buf.release(refCnt);
    }
    allocatedReadBuffers.decrement();
    memoryManager.changeReadBufferCounter(-1 * bufferSize);
  }

  public int requestsLength() {
    return requests.size();
  }

  public long getAllocatedReadBuffers() {
    return allocatedReadBuffers.sum();
  }

  public void close() {
    stopFlag = true;
    requests.clear();
  }

  private class DispatcherRunnable implements Runnable {

    public DispatcherRunnable() {}

    @Override
    public void run() {
      while (!stopFlag) {
        try {
          ReadBufferRequest request;
          request = requests.poll(1000, TimeUnit.MILLISECONDS);
          List<ByteBuf> buffers = new ArrayList<>();
          try {
            if (request != null) {
              processBufferRequest(request, buffers);
            } else {
              if (readBufferAllocator instanceof PooledByteBufAllocator) {
                // Free buffer pool memory to main direct memory when dispatcher is idle.
                ((PooledByteBufAllocator) readBufferAllocator).trimCurrentThreadCache();
              }
            }
          } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            try {
              // recycle all allocated buffers
              for (ByteBuf buffer : buffers) {
                recycle(buffer);
              }
            } catch (Throwable e1) {
              logger.error("Recycle read buffer failed.", e1);
            }
            request.getBufferListener().notifyBuffers(null, e);
          }
        } catch (Throwable e) {
          logger.error("Read buffer dispatcher encountered error: {}", e.getMessage(), e);
        }
      }
    }

    void processBufferRequest(ReadBufferRequest request, List<ByteBuf> buffers) {
      long start = System.nanoTime();
      int bufferSize = request.getBufferSize();
      while (buffers.size() < request.getNumber()) {
        if (memoryManager.readBufferAvailable(bufferSize)) {
          ByteBuf buf = readBufferAllocator.buffer(bufferSize, bufferSize);
          buffers.add(buf);
          memoryManager.changeReadBufferCounter(bufferSize);
          allocatedReadBuffers.increment();
        } else {
          try {
            // If dispatcher can not allocate requested buffers, it will wait here until
            // necessary buffers are get.
            Thread.sleep(readBufferAllocationWait);
          } catch (InterruptedException e) {
            logger.warn("Buffer dispatcher is closing");
          }
        }
      }
      long end = System.nanoTime();
      logger.debug(
          "process read buffer request using {} ms", TimeUnit.NANOSECONDS.toMillis(end - start));
      request.getBufferListener().notifyBuffers(buffers, null);
    }
  }
}
