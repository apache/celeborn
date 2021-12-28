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

package com.aliyun.emr.rss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.unsafe.Platform;

public final class FlushBuffer extends MinimalByteBuf {
  private static final Logger logger = LoggerFactory.getLogger(FlushBuffer.class);

  private final int id;
  private final long startAddress;
  private final long endAddress;
  private long currentAddress;

  public FlushBuffer(int id, long startAddress, long endAddress) {
    this.id = id;
    this.startAddress = startAddress;
    this.endAddress = endAddress;
    this.currentAddress = startAddress;
  }

  public int remaining() {
    return (int)(endAddress - currentAddress);
  }

  public void append(ByteBuf data) {
    final int length = data.readableBytes();
    final int dstIndex = (int) (currentAddress - startAddress);
    data.getBytes(data.readerIndex(), this, dstIndex, length);
    currentAddress += length;
  }

  public void reset() {
    currentAddress = startAddress;
  }

  public boolean hasData() {
    return (currentAddress > startAddress);
  }

  public int getId() {
    return id;
  }

  public long getStartAddress() {
    return startAddress;
  }

  public long getEndAddress() {
    return endAddress;
  }

  public long getCurrentAddress() {
    return currentAddress;
  }

  @Override
  public int capacity() {
    return (int) (endAddress - startAddress);
  }

  @Override
  public boolean hasMemoryAddress() {
    return true;
  }

  @Override
  public long memoryAddress() {
    return startAddress;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    if (src.hasMemoryAddress()) {
      Platform.copyMemory(null, src.memoryAddress() + srcIndex,
          null, startAddress + index, length);
    } if (src.hasArray()) {
      Platform.copyMemory(
          src.array(), Platform.BYTE_ARRAY_OFFSET + src.arrayOffset() + srcIndex,
          null, startAddress + index, length);
    } else {
      src.getBytes(srcIndex, this, index, length);
    }
    return this;
  }
}
