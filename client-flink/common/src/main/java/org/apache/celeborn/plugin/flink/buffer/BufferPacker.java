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

package org.apache.celeborn.plugin.flink.buffer;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.celeborn.plugin.flink.utils.BufferUtils;

/** Harness used to pack multiple partial buffers together as a full one. */
public class BufferPacker {

  public interface BiConsumerWithException<T, U, E extends Throwable> {
    void accept(T var1, U var2) throws E;
  }

  private final BiConsumerWithException<ByteBuf, Integer, InterruptedException> ripeBufferHandler;

  private Buffer cachedBuffer;

  private int currentSubIdx = -1;

  public BufferPacker(
      BiConsumerWithException<ByteBuf, Integer, InterruptedException> ripeBufferHandler) {
    this.ripeBufferHandler = ripeBufferHandler;
  }

  public void process(Buffer buffer, int subIdx) throws InterruptedException {
    if (buffer == null) {
      return;
    }

    if (buffer.readableBytes() == 0) {
      buffer.recycleBuffer();
      return;
    }

    if (cachedBuffer == null) {
      cachedBuffer = buffer;
      currentSubIdx = subIdx;
    } else if (currentSubIdx != subIdx) {
      Buffer dumpedBuffer = cachedBuffer;
      cachedBuffer = buffer;
      int targetSubIdx = currentSubIdx;
      currentSubIdx = subIdx;
      handleRipeBuffer(dumpedBuffer, targetSubIdx);
    } else {
      /**
       * this is an optimization。if cachedBuffer can contain other buffer, then other buffer can
       * reuse the same HEADER_LENGTH_PREFIX of the cachedBuffer, so cachedbuffer just read datas
       * whose length is buffer.readableBytes() - BufferUtils.HEADER_LENGTH_PREFIX
       */
      if (cachedBuffer.readableBytes() + buffer.readableBytes() - BufferUtils.HEADER_LENGTH_PREFIX
          <= cachedBuffer.getMaxCapacity()) {
        cachedBuffer
            .asByteBuf()
            .writeBytes(
                buffer.asByteBuf(),
                BufferUtils.HEADER_LENGTH_PREFIX,
                buffer.readableBytes() - BufferUtils.HEADER_LENGTH_PREFIX);
        buffer.recycleBuffer();
      } else {
        Buffer dumpedBuffer = cachedBuffer;
        cachedBuffer = buffer;
        handleRipeBuffer(dumpedBuffer, currentSubIdx);
      }
    }
  }

  public void drain() throws InterruptedException {
    if (cachedBuffer != null) {
      handleRipeBuffer(cachedBuffer, currentSubIdx);
    }
    cachedBuffer = null;
    currentSubIdx = -1;
  }

  private void handleRipeBuffer(Buffer buffer, int subIdx) throws InterruptedException {
    buffer.setCompressed(false);
    ripeBufferHandler.accept(buffer.asByteBuf(), subIdx);
  }

  public void close() {
    if (cachedBuffer != null) {
      cachedBuffer.recycleBuffer();
      cachedBuffer = null;
    }
    currentSubIdx = -1;
  }
}
