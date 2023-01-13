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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

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

  public static Queue<Buffer> unpack(ByteBuf byteBuf) {
    Queue<Buffer> buffers = new ArrayDeque<>();
    try {
      Utils.checkState(byteBuf instanceof Buffer, "Illegal buffer type.");

      Buffer buffer = (Buffer) byteBuf;
      int position = 0;
      int totalBytes = buffer.readableBytes();
      boolean isFirst = true;
      while (position < totalBytes) {
        BufferHeader bufferHeader;
        if (isFirst) {
          bufferHeader = BufferUtils.getBufferHeader(buffer, position, isFirst);
          position += BufferUtils.HEADER_LENGTH;
        } else {
          // in the remaining datas, the headlength is BufferUtils.HEADER_LENGTH -
          // BufferUtils.HEADER_LENGTH_PREFIX
          bufferHeader = BufferUtils.getBufferHeader(buffer, position);
          position += BufferUtils.HEADER_LENGTH - BufferUtils.HEADER_LENGTH_PREFIX;
        }

        Buffer slice = buffer.readOnlySlice(position, bufferHeader.getSize());
        position += bufferHeader.getSize();

        buffers.add(
            new UnpackSlicedBuffer(
                slice,
                bufferHeader.getDataType(),
                bufferHeader.isCompressed(),
                bufferHeader.getSize()));
        slice.retainBuffer();
        isFirst = false;
      }
      return buffers;
    } catch (Throwable throwable) {
      buffers.forEach(Buffer::recycleBuffer);
      throw throwable;
    } finally {
      byteBuf.release();
    }
  }

  private static class UnpackSlicedBuffer implements Buffer {

    private final Buffer buffer;

    private DataType dataType;

    private boolean isCompressed;

    private final int size;

    UnpackSlicedBuffer(Buffer buffer, DataType dataType, boolean isCompressed, int size) {
      this.buffer = buffer;
      this.dataType = dataType;
      this.isCompressed = isCompressed;
      this.size = size;
    }

    @Override
    public boolean isBuffer() {
      return dataType.isBuffer();
    }

    @Override
    public MemorySegment getMemorySegment() {
      return buffer.getMemorySegment();
    }

    @Override
    public int getMemorySegmentOffset() {
      return buffer.getMemorySegmentOffset();
    }

    @Override
    public BufferRecycler getRecycler() {
      return buffer.getRecycler();
    }

    @Override
    public void recycleBuffer() {
      buffer.recycleBuffer();
    }

    @Override
    public boolean isRecycled() {
      return buffer.isRecycled();
    }

    @Override
    public Buffer retainBuffer() {
      return buffer.retainBuffer();
    }

    @Override
    public Buffer readOnlySlice() {
      return buffer.readOnlySlice();
    }

    @Override
    public Buffer readOnlySlice(int i, int i1) {
      return buffer.readOnlySlice(i, i1);
    }

    @Override
    public int getMaxCapacity() {
      return buffer.getMaxCapacity();
    }

    @Override
    public int getReaderIndex() {
      return buffer.getReaderIndex();
    }

    @Override
    public void setReaderIndex(int i) throws IndexOutOfBoundsException {
      buffer.setReaderIndex(i);
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public void setSize(int i) {
      buffer.setSize(i);
    }

    @Override
    public int readableBytes() {
      return buffer.readableBytes();
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
      return buffer.getNioBufferReadable();
    }

    @Override
    public ByteBuffer getNioBuffer(int i, int i1) throws IndexOutOfBoundsException {
      return buffer.getNioBuffer(i, i1);
    }

    @Override
    public void setAllocator(ByteBufAllocator byteBufAllocator) {
      buffer.setAllocator(byteBufAllocator);
    }

    @Override
    public ByteBuf asByteBuf() {
      return buffer.asByteBuf();
    }

    @Override
    public boolean isCompressed() {
      return isCompressed;
    }

    @Override
    public void setCompressed(boolean b) {
      isCompressed = b;
    }

    @Override
    public DataType getDataType() {
      return dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
      this.dataType = dataType;
    }

    @Override
    public int refCnt() {
      return buffer.refCnt();
    }
  }
}
