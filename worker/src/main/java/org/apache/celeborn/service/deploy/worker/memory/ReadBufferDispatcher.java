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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;

public class ReadBufferDispatcher extends Thread implements MemoryManager.MemoryPressureListener {
  private final Logger logger = LoggerFactory.getLogger(ReadBufferDispatcher.class);
  private final LinkedBlockingQueue<ReadBufferRequest> requests = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<ByteBuf> idleBuffers = new LinkedBlockingQueue<>();
  private final MemoryManager memoryManager;
  private final PooledByteBufAllocator readBufferAllocator;
  private final LongAdder allocatedReadBuffers = new LongAdder();
  private final long readBufferAllocationWait;
  private volatile boolean stopFlag = false;
  private volatile boolean onTrim = false;

  public ReadBufferDispatcher(MemoryManager memoryManager, CelebornConf conf) {
    this.readBufferAllocationWait = conf.readBufferAllocationWait();
    // readBuffer is not a module name, it's a placeholder.
    readBufferAllocator =
        NettyUtils.getPooledByteBufAllocator(new TransportConf("readBuffer", conf), null, true);
    this.memoryManager = memoryManager;
    this.setName("Read-Buffer-Dispatcher");
    this.start();
  }

  public void addBufferRequest(ReadBufferRequest request) {
    requests.add(request);
  }

  public void recycle(ByteBuf buf) {
    buf.clear();
    idleBuffers.add(buf);
  }

  @Override
  public void run() {
    while (!stopFlag) {
      if (onTrim) {
        freeBuffersToSystem();
        onTrim = false;
      } else {
        dispatchBuffers();
      }
    }
  }

  private void freeBuffersToSystem() {
    while (!idleBuffers.isEmpty()) {
      ByteBuf buf = idleBuffers.poll();
      if (buf != null) {
        int bufferSize = buf.capacity();
        int refCnt = buf.refCnt();
        // If a reader failed, related read buffers will have more than one reference count
        if (refCnt > 0) {
          buf.release(refCnt);
        }
        allocatedReadBuffers.decrement();
        memoryManager.changeReadBufferCounter(-1 * bufferSize);
      }
    }
  }

  private void dispatchBuffers() {
    ReadBufferRequest request = null;
    try {
      request = requests.poll(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.info("Buffer dispatcher is closing");
    }

    List<ByteBuf> buffers = null;
    try {
      if (request != null) {
        long start = System.nanoTime();
        int bufferSize = request.getBufferSize();
        buffers = new ArrayList<>();
        while (buffers.size() < request.getNumber()) {
          ByteBuf buf = idleBuffers.poll();
          if (buf != null) {
            buffers.add(buf);
          } else {
            if (memoryManager.readBufferAvailable(bufferSize)) {
              buf = readBufferAllocator.buffer(bufferSize, bufferSize);
              buffers.add(buf);
              memoryManager.changeReadBufferCounter(bufferSize);
              allocatedReadBuffers.increment();
            } else {
              try {
                // If dispatcher can not allocate requested buffers, it will wait here until
                // necessary buffers are get.
                Thread.sleep(this.readBufferAllocationWait);
              } catch (InterruptedException e) {
                logger.info("Buffer dispatcher is closing");
              }
            }
          }
        }
        long end = System.nanoTime();
        logger.debug(
            "process read buffer request using {} ms", TimeUnit.NANOSECONDS.toMillis(end - start));
        request.getBufferListener().notifyBuffers(buffers, null);
      } else {
        // Free buffer pool memory to main direct memory when dispatcher is idle.
        readBufferAllocator.trimCurrentThreadCache();
      }
    } catch (Throwable e) {
      logger.error(e.getMessage(), e);
      // recycle all allocated buffers
      if (buffers != null) {
        buffers.forEach(this::recycle);
      }
    }
  }

  public int requestsLength() {
    return requests.size();
  }

  public int idleBuffersLength() {
    return idleBuffers.size();
  }

  public long getAllocatedReadBuffers() {
    return allocatedReadBuffers.sum();
  }

  public void close() {
    stopFlag = true;
    requests.clear();
  }

  @Override
  public void onTrim() {
    onTrim = true;
  }
}
