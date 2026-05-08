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

package org.apache.celeborn.common.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import io.netty.buffer.Unpooled;

/** A managed buffer assembled lazily from multiple child buffers. */
public class CompositeManagedBuffer extends ManagedBuffer {
  private final List<ManagedBuffer> buffers;
  private final long size;

  public CompositeManagedBuffer(List<ManagedBuffer> buffers) {
    this.buffers = buffers;
    long totalSize = 0;
    for (ManagedBuffer buffer : buffers) {
      totalSize += buffer.size();
    }
    this.size = totalSize;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    if (size > Integer.MAX_VALUE) {
      throw new IOException("Composite buffer is too large: " + size);
    }
    ByteBuffer merged = ByteBuffer.allocate((int) size);
    for (ManagedBuffer buffer : buffers) {
      merged.put(buffer.nioByteBuffer());
    }
    merged.flip();
    return merged;
  }

  @Override
  public InputStream createInputStream() throws IOException {
    List<InputStream> inputStreams = new ArrayList<>(buffers.size());
    try {
      for (ManagedBuffer buffer : buffers) {
        inputStreams.add(buffer.createInputStream());
      }
    } catch (IOException e) {
      for (InputStream inputStream : inputStreams) {
        inputStream.close();
      }
      throw e;
    }
    Enumeration<InputStream> streams = Collections.enumeration(inputStreams);
    return new java.io.SequenceInputStream(streams);
  }

  @Override
  public ManagedBuffer retain() {
    buffers.forEach(ManagedBuffer::retain);
    return this;
  }

  @Override
  public ManagedBuffer release() {
    buffers.forEach(ManagedBuffer::release);
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    return Unpooled.wrappedBuffer(nioByteBuffer());
  }

  @Override
  public Object convertToNettyForSsl() throws IOException {
    return Unpooled.wrappedBuffer(nioByteBuffer());
  }
}
