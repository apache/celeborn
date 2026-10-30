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

import java.util.concurrent.atomic.AtomicBoolean;

import scala.Tuple2;

import io.netty.buffer.CompositeByteBuf;

import org.apache.celeborn.common.meta.MemoryFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;

public class MemoryChunkBuffers extends ChunkBuffers {
  private final CompositeByteBuf buffer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * @param takeOwnership whether to take over the existing buffer reference. If false, retain the
   *     shared buffer and own the new reference.
   */
  public MemoryChunkBuffers(MemoryFileInfo memoryFileInfo, boolean takeOwnership) {
    super((ReduceFileMeta) memoryFileInfo.getFileMeta());
    CompositeByteBuf buffer = memoryFileInfo.getBuffer();
    this.buffer = takeOwnership ? buffer : buffer.retain();
  }

  @Override
  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    Tuple2<Long, Long> offsetLen = getChunkOffsetLength(chunkIndex, offset, len);
    return new NettyManagedBuffer(
        buffer.retainedSlice(offsetLen._1.intValue(), offsetLen._2.intValue()));
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      buffer.release();
    }
  }
}
