/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.celeborn.plugin.flink.tiered;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.ExceptionUtils;

public class CelebornChannelBufferManager implements BufferListener, BufferRecycler {

  /** The queue to hold the available buffer when the reader is waiting for buffers. */
  private final Queue<Buffer> bufferQueue;

  private final TieredStorageMemoryManager memoryManager;

  private final CelebornChannelBufferReader bufferReader;

  /** The tag indicates whether it is waiting for buffers from the buffer pool. */
  private boolean isWaitingForFloatingBuffers;

  /** The total number of required buffers for the respective input channel. */
  private int numRequiredBuffers = 0;

  public CelebornChannelBufferManager(
      TieredStorageMemoryManager memoryManager, CelebornChannelBufferReader bufferReader) {
    this.memoryManager = checkNotNull(memoryManager);
    this.bufferReader = checkNotNull(bufferReader);
    this.bufferQueue = new LinkedList<>();
  }

  @Override
  public boolean notifyBufferAvailable(Buffer buffer) {
    if (bufferReader.isClosed()) {
      return false;
    }
    int numBuffers = 0;
    boolean isBufferUsed = false;
    try {
      synchronized (bufferQueue) {
        checkState(
            isWaitingForFloatingBuffers, "This channel should be waiting for floating buffers.");
        isWaitingForFloatingBuffers = false;
        if (bufferReader.isClosed() || bufferQueue.size() >= numRequiredBuffers) {
          return false;
        }
        bufferQueue.add(buffer);
        isBufferUsed = true;
        numBuffers = 1 + tryRequestBuffers();
      }
      bufferReader.notifyAvailableCredits(numBuffers);
    } catch (Throwable t) {
      bufferReader.errorReceived(t.getLocalizedMessage());
    }
    return isBufferUsed;
  }

  public void decreaseRequiredCredits(int numCredits) {
    synchronized (bufferQueue) {
      numRequiredBuffers -= numCredits;
    }
  }

  @Override
  public void notifyBufferDestroyed() {
    // noop
  }

  @Override
  public void recycle(MemorySegment segment) {
    try {
      memoryManager.getBufferPool().recycle(segment);
    } catch (Throwable t) {
      ExceptionUtils.rethrow(t);
    }
  }

  Buffer requestBuffer() {
    synchronized (bufferQueue) {
      return bufferQueue.poll();
    }
  }

  int requestBuffers(int numRequired) {
    int numRequestedBuffers = 0;
    synchronized (bufferQueue) {
      if (bufferReader.isClosed()) {
        return numRequestedBuffers;
      }
      numRequiredBuffers += numRequired;
      numRequestedBuffers = tryRequestBuffers();
    }
    return numRequestedBuffers;
  }

  int tryRequestBuffersIfNeeded() {
    synchronized (bufferQueue) {
      if (numRequiredBuffers > 0 && !isWaitingForFloatingBuffers && bufferQueue.isEmpty()) {
        return tryRequestBuffers();
      }
      return 0;
    }
  }

  void close() {
    synchronized (bufferQueue) {
      for (Buffer buffer : bufferQueue) {
        buffer.recycleBuffer();
      }
      bufferQueue.clear();
    }
  }

  private int tryRequestBuffers() {
    assert Thread.holdsLock(bufferQueue);
    int numRequestedBuffers = 0;
    while (bufferQueue.size() < numRequiredBuffers && !isWaitingForFloatingBuffers) {
      BufferPool bufferPool = memoryManager.getBufferPool();
      Buffer buffer = bufferPool.requestBuffer();
      if (buffer != null) {
        bufferQueue.add(buffer);
        numRequestedBuffers++;
      } else if (bufferPool.addBufferListener(this)) {
        isWaitingForFloatingBuffers = true;
        break;
      }
    }
    return numRequestedBuffers;
  }
}
