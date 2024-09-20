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

/**
 * Harness used to pack multiple partial buffers together as a full one. It used in Flink hybrid
 * shuffle integration strategy now.
 */
public class ReceivedNoHeaderBufferPacker extends BufferPacker {

  /** The flink buffer header of cached first buffer. */
  private BufferHeader firstBufferHeader;

  public ReceivedNoHeaderBufferPacker(
      BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException> ripeBufferHandler) {
    super(ripeBufferHandler);
  }

  @Override
  public void process(Buffer buffer, int subIdx) throws InterruptedException {
    if (buffer == null) {
      return;
    }

    if (buffer.readableBytes() == 0) {
      buffer.recycleBuffer();
      return;
    }

    if (cachedBuffer == null) {
      // cache the first buffer and record flink buffer header of first buffer
      cachedBuffer = buffer;
      currentSubIdx = subIdx;
      firstBufferHeader =
          new BufferHeader(subIdx, buffer.getDataType(), buffer.isCompressed(), buffer.getSize());
    } else if (currentSubIdx != subIdx) {
      // drain the previous cached buffer and cache current buffer
      Buffer dumpedBuffer = cachedBuffer;
      cachedBuffer = buffer;
      int targetSubIdx = currentSubIdx;
      currentSubIdx = subIdx;
      logBufferPack(false, dumpedBuffer.getDataType(), dumpedBuffer.readableBytes());
      handleRipeBuffer(
          dumpedBuffer, targetSubIdx, dumpedBuffer.getDataType(), dumpedBuffer.isCompressed());
      firstBufferHeader =
          new BufferHeader(subIdx, buffer.getDataType(), buffer.isCompressed(), buffer.getSize());
    } else {
      int bufferHeaderLength = BufferUtils.HEADER_LENGTH - BufferUtils.HEADER_LENGTH_PREFIX;
      if (cachedBuffer.readableBytes() + buffer.readableBytes() + bufferHeaderLength
          <= cachedBuffer.getMaxCapacity() - BufferUtils.HEADER_LENGTH) {
        // if the cache buffer can contain the current buffer, then pack the current buffer into
        // cache buffer
        ByteBuf byteBuf = cachedBuffer.asByteBuf();
        byteBuf.writeByte(buffer.getDataType().ordinal());
        byteBuf.writeBoolean(buffer.isCompressed());
        byteBuf.writeInt(buffer.getSize());
        byteBuf.writeBytes(buffer.asByteBuf(), 0, buffer.readableBytes());
        logBufferPack(false, buffer.getDataType(), buffer.readableBytes() + bufferHeaderLength);

        buffer.recycleBuffer();
      } else {
        // if the cache buffer cannot contain the current buffer, drain the cached buffer, and cache
        // the current buffer
        Buffer dumpedBuffer = cachedBuffer;
        cachedBuffer = buffer;
        logBufferPack(false, dumpedBuffer.getDataType(), dumpedBuffer.readableBytes());

        handleRipeBuffer(
            dumpedBuffer, currentSubIdx, dumpedBuffer.getDataType(), dumpedBuffer.isCompressed());
        firstBufferHeader =
            new BufferHeader(subIdx, buffer.getDataType(), buffer.isCompressed(), buffer.getSize());
      }
    }
  }

  @Override
  protected void handleRipeBuffer(
      Buffer buffer, int subIdx, Buffer.DataType dataType, boolean isCompressed)
      throws InterruptedException {
    if (buffer == null || buffer.readableBytes() == 0) {
      return;
    }
    // Always set the compress flag to false, because this buffer contains Celeborn header and
    // multiple flink data buffers.
    // It is crucial to keep this flag set to false because we need to slice this buffer to extract
    // flink data buffers
    // during the unpacking process, the flink {@link NetworkBuffer} cannot correctly slice
    // compressed buffer.
    buffer.setCompressed(false);
    ripeBufferHandler.accept(buffer.asByteBuf(), firstBufferHeader);
  }
}
