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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.MemoryFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;

public class MemoryChunkBuffersSuiteJ {

  private static final UserIdentifier USER = new UserIdentifier("tenant", "user");

  private static CompositeByteBuf newBuffer() {
    CompositeByteBuf buffer = Unpooled.compositeBuffer();
    buffer.addComponent(true, Unpooled.buffer(1024).writeZero(1024));
    return buffer;
  }

  private static MemoryChunkBuffers newChunkBuffers(CompositeByteBuf buffer) {
    ReduceFileMeta fileMeta = new ReduceFileMeta(1024);
    fileMeta.addChunkOffset(1024);
    return new MemoryChunkBuffers(new MemoryFileInfo(USER, false, fileMeta, buffer), true);
  }

  @Test
  public void chunkAfterCloseFailsCleanly() {
    CompositeByteBuf buffer = newBuffer();
    MemoryChunkBuffers buffers = newChunkBuffers(buffer);

    buffers.close();

    assertEquals(0, buffer.refCnt());
    assertThrows(IllegalStateException.class, () -> buffers.chunk(0, 0, 1024));
  }

  @Test
  public void closeKeepsInFlightChunkValid() {
    CompositeByteBuf buffer = newBuffer();
    MemoryChunkBuffers buffers = newChunkBuffers(buffer);

    ManagedBuffer chunk = buffers.chunk(0, 0, 1024);
    assertTrue(chunk.size() > 0);
    assertEquals(2, buffer.refCnt());

    buffers.close();
    // close releases the MemoryChunkBuffers' reference, but the in-flight
    // chunk keeps its retained slice valid.
    assertEquals(1, buffer.refCnt());
    assertTrue(chunk.size() > 0);

    chunk.release();
    assertEquals(0, buffer.refCnt());
  }
}
