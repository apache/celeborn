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

import static org.apache.commons.crypto.utils.Utils.checkArgument;
import static org.apache.hadoop.shaded.com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import io.netty.buffer.ByteBuf;

public class BufferQueue {

  private final Queue<ByteBuf> buffers = new ConcurrentLinkedQueue<>();

  private final MemoryManager memoryManager = MemoryManager.instance();

  /** Number of buffers occupied by this buffer queue (added but still not recycled). */
  private final AtomicInteger numBuffersOccupied = new AtomicInteger();

  /** Whether this buffer queue is released or not. */
  private volatile boolean isReleased = false;

  public BufferQueue() {}

  /** Returns the number of available buffers in this buffer queue. */
  public int size() {
    return buffers.size();
  }

  /**
   * Returns an available buffer from this buffer queue or returns null if no buffer is available
   * currently.
   */
  @Nullable
  public ByteBuf poll() {
    return buffers.poll();
  }

  /**
   * Adds a collection of available buffers to this buffer queue and will throw exception if this
   * buffer queue has been released.
   */
  public synchronized void add(Collection<ByteBuf> availableBuffers) {
    checkArgument(availableBuffers != null, "Must be not null.");
    checkState(!isReleased, "Buffer queue has been released.");

    buffers.addAll(availableBuffers);
    numBuffersOccupied.addAndGet(availableBuffers.size());
  }

  public synchronized void add(ByteBuf buffer) {
    buffers.add(buffer);
  }

  public int numBuffersOccupied() {
    return numBuffersOccupied.get();
  }

  public void recycle(ByteBuf buffer) {
    if (buffer == null) {
      return;
    }
    numBuffersOccupied.decrementAndGet();
    memoryManager.recycleReadBuffer(buffer);
  }

  /**
   * Releases this buffer queue and recycles all available buffers. After released, no buffer can be
   * added to or polled from this buffer queue.
   */
  public synchronized void release() {
    isReleased = true;
    numBuffersOccupied.addAndGet(buffers.size() * -1);
    buffers.forEach(memoryManager::recycleReadBuffer);
    buffers.clear();
  }

  /** Returns true is this buffer queue has been released. */
  public boolean isReleased() {
    return isReleased;
  }
}
