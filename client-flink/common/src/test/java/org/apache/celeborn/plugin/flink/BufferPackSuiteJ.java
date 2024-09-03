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

package org.apache.celeborn.plugin.flink;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.EVENT_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.celeborn.plugin.flink.buffer.BufferHeader;
import org.apache.celeborn.plugin.flink.buffer.BufferPacker;
import org.apache.celeborn.plugin.flink.buffer.ReceivedNoHeaderBufferPacker;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;

@RunWith(Parameterized.class)
public class BufferPackSuiteJ {
  private static final int BUFFER_SIZE = 20 + 16;

  private NetworkBufferPool networkBufferPool;

  private BufferPool bufferPool;

  private boolean bufferPackerReceivedBufferHasHeader;

  public BufferPackSuiteJ(boolean bufferPackerReceivedBufferHasHeader) {
    this.bufferPackerReceivedBufferHasHeader = bufferPackerReceivedBufferHasHeader;
  }

  @Parameterized.Parameters
  public static Collection prepareData() {
    Object[][] object = {{true}, {false}};
    return Arrays.asList(object);
  }

  @Before
  public void setup() throws Exception {
    networkBufferPool = new NetworkBufferPool(10, BUFFER_SIZE);
    bufferPool = networkBufferPool.createBufferPool(10, 10);
  }

  @After
  public void tearDown() {
    bufferPool.lazyDestroy();
    assertEquals(10, networkBufferPool.getNumberOfAvailableMemorySegments());
    networkBufferPool.destroy();
  }

  @Test
  public void testPackEmptyBuffers() throws Exception {
    List<Buffer> buffers = requestBuffers(3);
    setCompressed(buffers, true, true, false);
    setDataType(buffers, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);

    Integer subIdx = 2;

    List<ByteBuf> output = new ArrayList<>();
    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) -> {
              assertEquals(subIdx, Integer.valueOf(header.getSubPartitionId()));
              output.add(ripe);
            };

    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    packer.process(buffers.get(0), subIdx);
    packer.process(buffers.get(1), subIdx);
    packer.process(buffers.get(2), subIdx);
    assertTrue(output.isEmpty());

    packer.drain();
    assertEquals(0, output.size());
  }

  @Test
  public void testPartialBuffersForSameSubIdx() throws Exception {
    List<Buffer> buffers = requestBuffers(3);
    setCompressed(buffers, true, true, false);
    setDataType(buffers, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);

    List<Pair<ByteBuf, Integer>> output = new ArrayList<>();
    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) ->
                output.add(
                    Pair.of(addBufferHeaderPossible(ripe, header), header.getSubPartitionId()));
    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    fillBuffers(buffers, 0, 1, 2);

    packer.process(buffers.get(0), 2);
    packer.process(buffers.get(1), 2);
    assertEquals(0, output.size());

    packer.process(buffers.get(2), 2);
    assertEquals(1, output.size());

    packer.drain();
    assertEquals(2, output.size());

    List<Buffer> unpacked = new ArrayList<>();
    output.forEach(
        pair -> {
          assertEquals(Integer.valueOf(2), pair.getRight());
          unpacked.addAll(BufferPacker.unpack(pair.getLeft()));
        });
    checkIfCompressed(unpacked, true, true, false);
    checkDataType(unpacked, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);
    verifyBuffers(unpacked, 0, 1, 2);
    unpacked.forEach(Buffer::recycleBuffer);
  }

  @Test
  public void testPartialBuffersForMultipleSubIdx() throws Exception {
    List<Buffer> buffers = requestBuffers(3);
    setCompressed(buffers, true, true, false);
    setDataType(buffers, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);

    List<Pair<ByteBuf, Integer>> output = new ArrayList<>();
    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) ->
                output.add(
                    Pair.of(addBufferHeaderPossible(ripe, header), header.getSubPartitionId()));
    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    fillBuffers(buffers, 0, 1, 2);

    packer.process(buffers.get(0), 0);
    packer.process(buffers.get(1), 1);
    assertEquals(1, output.size());

    packer.process(buffers.get(2), 1);
    assertEquals(1, output.size());

    packer.drain();
    assertEquals(2, output.size());

    List<Buffer> unpacked = new ArrayList<>();
    for (int i = 0; i < output.size(); i++) {
      Pair<ByteBuf, Integer> pair = output.get(i);
      assertEquals(Integer.valueOf(i), pair.getRight());
      unpacked.addAll(BufferPacker.unpack(pair.getLeft()));
    }

    checkIfCompressed(unpacked, true, true, false);
    checkDataType(unpacked, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);
    verifyBuffers(unpacked, 0, 1, 2);
    unpacked.forEach(Buffer::recycleBuffer);
  }

  @Test
  public void testUnpackedBuffers() throws Exception {
    List<Buffer> buffers = requestBuffers(3);
    setCompressed(buffers, true, true, false);
    setDataType(buffers, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);

    List<Pair<ByteBuf, Integer>> output = new ArrayList<>();
    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) ->
                output.add(
                    Pair.of(addBufferHeaderPossible(ripe, header), header.getSubPartitionId()));
    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    fillBuffers(buffers, 0, 1, 2);

    packer.process(buffers.get(0), 0);
    packer.process(buffers.get(1), 1);
    assertEquals(1, output.size());

    packer.process(buffers.get(2), 2);
    assertEquals(2, output.size());

    packer.drain();
    assertEquals(3, output.size());

    List<Buffer> unpacked = new ArrayList<>();
    for (int i = 0; i < output.size(); i++) {
      Pair<ByteBuf, Integer> pair = output.get(i);
      assertEquals(Integer.valueOf(i), pair.getRight());
      unpacked.addAll(BufferPacker.unpack(pair.getLeft()));
    }

    checkIfCompressed(unpacked, true, true, false);
    checkDataType(unpacked, EVENT_BUFFER, DATA_BUFFER, DATA_BUFFER);
    verifyBuffers(unpacked, 0, 1, 2);
    unpacked.forEach(Buffer::recycleBuffer);
  }

  @Test
  public void testPackMultipleBuffers() throws Exception {
    int numBuffers = 7;
    List<Buffer> buffers = new ArrayList<>();
    buffers.add(buildSomeBuffer(100));
    buffers.addAll(requestBuffers(numBuffers - 1));
    setCompressed(buffers, true, true, true, false, false, false, true);
    setDataType(
        buffers,
        EVENT_BUFFER,
        DATA_BUFFER,
        DATA_BUFFER,
        EVENT_BUFFER,
        DATA_BUFFER,
        DATA_BUFFER,
        EVENT_BUFFER);

    List<Pair<ByteBuf, Integer>> output = new ArrayList<>();
    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) ->
                output.add(
                    Pair.of(addBufferHeaderPossible(ripe, header), header.getSubPartitionId()));
    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    fillBuffers(buffers, 0, 1, 2, 3, 4, 5, 6, 7);

    for (int i = 0; i < buffers.size(); i++) {
      packer.process(buffers.get(i), 0);
    }
    packer.drain();

    List<Buffer> unpacked = new ArrayList<>();
    for (int i = 0; i < output.size(); i++) {
      Pair<ByteBuf, Integer> pair = output.get(i);
      assertEquals(Integer.valueOf(0), pair.getRight());
      unpacked.addAll(BufferPacker.unpack(pair.getLeft()));
    }
    assertEquals(7, unpacked.size());

    checkIfCompressed(unpacked, true, true, true, false, false, false, true);
    checkDataType(
        unpacked,
        EVENT_BUFFER,
        DATA_BUFFER,
        DATA_BUFFER,
        EVENT_BUFFER,
        DATA_BUFFER,
        DATA_BUFFER,
        EVENT_BUFFER);
    verifyBuffers(unpacked, 0, 1, 2, 3, 4, 5, 6, 7);
    unpacked.forEach(Buffer::recycleBuffer);
  }

  @Test
  public void testFailedToHandleRipeBufferAndClose() throws Exception {
    List<Buffer> buffers = requestBuffers(1);
    setCompressed(buffers, false);
    setDataType(buffers, DATA_BUFFER);
    fillBuffers(buffers, 0);

    BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
        ripeBufferHandler =
            (ripe, header) -> {
              // ripe.release();
              throw new RuntimeException("Test");
            };
    BufferPacker packer = createBufferPakcer(ripeBufferHandler);
    System.out.println(buffers.get(0).refCnt());
    packer.process(buffers.get(0), 0);
    try {
      packer.drain();
    } catch (RuntimeException ignored) {
    }

    // this should never throw any exception
    packer.close();
    assertEquals(0, bufferPool.bestEffortGetNumOfUsedBuffers());
  }

  private List<Buffer> requestBuffers(int n) {
    List<Buffer> buffers = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Buffer buffer = bufferPool.requestBuffer();
      buffers.add(buffer);
    }
    return buffers;
  }

  private void setCompressed(List<Buffer> buffers, boolean... values) {
    for (int i = 0; i < buffers.size(); i++) {
      buffers.get(i).setCompressed(values[i]);
    }
  }

  private void setDataType(List<Buffer> buffers, Buffer.DataType... values) {
    for (int i = 0; i < buffers.size(); i++) {
      buffers.get(i).setDataType(values[i]);
    }
  }

  private void checkIfCompressed(List<Buffer> buffers, boolean... values) {
    for (int i = 0; i < buffers.size(); i++) {
      assertEquals(values[i], buffers.get(i).isCompressed());
    }
  }

  private void checkDataType(List<Buffer> buffers, Buffer.DataType... values) {
    for (int i = 0; i < buffers.size(); i++) {
      assertEquals(values[i], buffers.get(i).getDataType());
    }
  }

  private void fillBuffers(List<Buffer> buffers, int... ints) {
    for (int i = 0; i < buffers.size(); i++) {
      Buffer buffer = buffers.get(i);
      ByteBuf target = buffer.asByteBuf();

      if (bufferPackerReceivedBufferHasHeader) {
        // If the buffer includes a header, we need to leave space for the header, so we should
        // update the writer index to BufferUtils.HEADER_LENGTH.
        BufferUtils.setBufferHeader(target, buffer.getDataType(), buffer.isCompressed(), 4);
        target.writerIndex(BufferUtils.HEADER_LENGTH);
      } else {
        // if the buffer does not have a header, we can directly write data starting from the
        // beginning of the buffer.
        target.writerIndex(0);
      }
      target.writeInt(ints[i]);
    }
  }

  private void verifyBuffers(List<Buffer> buffers, int... expects) {
    for (int i = 0; i < buffers.size(); i++) {
      ByteBuf actual = buffers.get(i).asByteBuf();
      assertEquals(expects[i], actual.getInt(0));
    }
  }

  public static Buffer buildSomeBuffer(int size) {
    final MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
    return new NetworkBuffer(seg, MemorySegment::free, Buffer.DataType.DATA_BUFFER, size);
  }

  public ByteBuf addBufferHeaderPossible(ByteBuf byteBuf, BufferHeader bufferHeader) {
    // Try to add buffer header if bufferPackerReceivedBufferHasHeader set to false in BufferPacker
    // drain process
    if (bufferPackerReceivedBufferHasHeader) {
      return byteBuf;
    }

    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    // create a small buffer headerBuf to write the buffer header
    ByteBuf headerBuf = Unpooled.buffer(BufferUtils.HEADER_LENGTH);

    // write celeborn buffer header (subpartitionid(4) + attemptId(4) + nextBatchId(4) +
    // compressedsize)
    headerBuf.writeInt(bufferHeader.getSubPartitionId());
    headerBuf.writeInt(0);
    headerBuf.writeInt(0);
    headerBuf.writeInt(
        byteBuf.readableBytes() + (BufferUtils.HEADER_LENGTH - BufferUtils.HEADER_LENGTH_PREFIX));

    // write flink buffer header (dataType(1) + isCompress(1) + size(4))
    headerBuf.writeByte(bufferHeader.getDataType().ordinal());
    headerBuf.writeBoolean(bufferHeader.isCompressed());
    headerBuf.writeInt(bufferHeader.getSize());

    // composite the headerBuf and data buffer together
    compositeByteBuf.addComponents(true, headerBuf, byteBuf);
    ByteBuf packedByteBuf = Unpooled.wrappedBuffer(compositeByteBuf.nioBuffer());
    byteBuf.writerIndex(0);
    byteBuf.writeBytes(packedByteBuf, 0, packedByteBuf.readableBytes());
    return byteBuf;
  }

  public BufferPacker createBufferPakcer(
      BufferPacker.BiConsumerWithException<ByteBuf, BufferHeader, InterruptedException>
          ripeBufferHandler) {
    if (bufferPackerReceivedBufferHasHeader) {
      return new BufferPacker(ripeBufferHandler);
    } else {
      return new ReceivedNoHeaderBufferPacker(ripeBufferHandler);
    }
  }
}
