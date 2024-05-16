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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.junit.Test;

import org.apache.celeborn.plugin.flink.buffer.BufferWithSubpartition;
import org.apache.celeborn.plugin.flink.buffer.DataBuffer;
import org.apache.celeborn.plugin.flink.buffer.SortBasedDataBuffer;

public class SortBasedDataBufferSuiteJ {
  @Test
  public void testWriteAndReadDataBuffer() throws Exception {
    int numSubpartitions = 10;
    int bufferSize = 1024;
    int bufferPoolSize = 1000;
    Random random = new Random(1111);

    // used to store data written to and read from data buffer for correctness check
    Queue<DataAndType>[] dataWritten = new Queue[numSubpartitions];
    Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
    for (int i = 0; i < numSubpartitions; ++i) {
      dataWritten[i] = new ArrayDeque<>();
      buffersRead[i] = new ArrayDeque<>();
    }

    int[] numBytesWritten = new int[numSubpartitions];
    int[] numBytesRead = new int[numSubpartitions];
    Arrays.fill(numBytesWritten, 0);
    Arrays.fill(numBytesRead, 0);

    // fill the data buffer with randomly generated data
    int totalBytesWritten = 0;
    DataBuffer dataBuffer =
        createDataBuffer(
            bufferPoolSize,
            bufferSize,
            numSubpartitions,
            getRandomSubpartitionOrder(numSubpartitions));
    while (true) {
      // record size may be larger than buffer size so a record may span multiple segments
      int recordSize = random.nextInt(bufferSize * 4 - 1) + 1;
      byte[] bytes = new byte[recordSize];

      // fill record with random value
      random.nextBytes(bytes);
      ByteBuffer record = ByteBuffer.wrap(bytes);

      // select a random subpartition to write
      int subpartition = random.nextInt(numSubpartitions);

      // select a random data type
      boolean isBuffer = random.nextBoolean() || recordSize > bufferSize;
      Buffer.DataType dataType =
          isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
      if (!dataBuffer.append(record, subpartition, dataType)) {
        dataBuffer.finish();
        break;
      }
      record.rewind();
      dataWritten[subpartition].add(new DataAndType(record, dataType));
      numBytesWritten[subpartition] += recordSize;
      totalBytesWritten += recordSize;
    }

    // read all data from the data buffer
    while (dataBuffer.hasRemaining()) {
      MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
      BufferWithSubpartition bufferWithSubpartition =
          dataBuffer.getNextBuffer(readBuffer, ignore -> {}, 0);
      int subpartition = bufferWithSubpartition.getSubpartitionIndex();
      buffersRead[subpartition].add(bufferWithSubpartition.getBuffer());
      numBytesRead[subpartition] += bufferWithSubpartition.getBuffer().readableBytes();
    }

    assertEquals(totalBytesWritten, dataBuffer.numTotalBytes());
    checkWriteReadResult(numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
  }

  public static void checkWriteReadResult(
      int numSubpartitions,
      int[] numBytesWritten,
      int[] numBytesRead,
      Queue<DataAndType>[] dataWritten,
      Collection<Buffer>[] buffersRead) {
    for (int subpartitionIndex = 0; subpartitionIndex < numSubpartitions; ++subpartitionIndex) {
      assertEquals(numBytesWritten[subpartitionIndex], numBytesRead[subpartitionIndex]);

      List<DataAndType> eventsWritten = new ArrayList<>();
      List<Buffer> eventsRead = new ArrayList<>();

      ByteBuffer subpartitionDataWritten = ByteBuffer.allocate(numBytesWritten[subpartitionIndex]);
      for (DataAndType dataAndType : dataWritten[subpartitionIndex]) {
        subpartitionDataWritten.put(dataAndType.data);
        dataAndType.data.rewind();
        if (dataAndType.dataType.isEvent()) {
          eventsWritten.add(dataAndType);
        }
      }

      ByteBuffer subpartitionDataRead = ByteBuffer.allocate(numBytesRead[subpartitionIndex]);
      for (Buffer buffer : buffersRead[subpartitionIndex]) {
        subpartitionDataRead.put(buffer.getNioBufferReadable());
        if (!buffer.isBuffer()) {
          eventsRead.add(buffer);
        }
      }

      subpartitionDataWritten.flip();
      subpartitionDataRead.flip();
      assertEquals(subpartitionDataWritten, subpartitionDataRead);

      assertEquals(eventsWritten.size(), eventsRead.size());
      for (int i = 0; i < eventsWritten.size(); ++i) {
        assertEquals(eventsWritten.get(i).dataType, eventsRead.get(i).getDataType());
        assertEquals(eventsWritten.get(i).data, eventsRead.get(i).getNioBufferReadable());
      }
    }
  }

  @Test
  public void testWriteReadWithEmptyChannel() throws Exception {
    int bufferPoolSize = 10;
    int bufferSize = 1024;
    int numSubpartitions = 5;

    ByteBuffer[] subpartitionRecords = {
      ByteBuffer.allocate(128), null, ByteBuffer.allocate(1536), null, ByteBuffer.allocate(1024)
    };

    DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, numSubpartitions);
    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
      ByteBuffer record = subpartitionRecords[subpartition];
      if (record != null) {
        dataBuffer.append(record, subpartition, Buffer.DataType.DATA_BUFFER);
        record.rewind();
      }
    }
    dataBuffer.finish();

    checkReadResult(dataBuffer, subpartitionRecords[0], 0, bufferSize);

    ByteBuffer expected1 = subpartitionRecords[2].duplicate();
    expected1.limit(bufferSize);
    checkReadResult(dataBuffer, expected1.slice(), 2, bufferSize);

    ByteBuffer expected2 = subpartitionRecords[2].duplicate();
    expected2.position(bufferSize);
    checkReadResult(dataBuffer, expected2.slice(), 2, bufferSize);

    checkReadResult(dataBuffer, subpartitionRecords[4], 4, bufferSize);
  }

  private void checkReadResult(
      DataBuffer dataBuffer, ByteBuffer expectedBuffer, int expectedSubpartition, int bufferSize) {
    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
    BufferWithSubpartition bufferWithSubpartition =
        dataBuffer.getNextBuffer(segment, ignore -> {}, 0);
    assertEquals(expectedSubpartition, bufferWithSubpartition.getSubpartitionIndex());
    assertEquals(expectedBuffer, bufferWithSubpartition.getBuffer().getNioBufferReadable());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteEmptyData() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);

    ByteBuffer record = ByteBuffer.allocate(1);
    record.position(1);

    dataBuffer.append(record, 0, Buffer.DataType.DATA_BUFFER);
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteFinishedDataBuffer() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
    dataBuffer.finish();

    dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteReleasedDataBuffer() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
    dataBuffer.release();

    dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
  }

  @Test
  public void testWriteMoreDataThanCapacity() throws Exception {
    int bufferPoolSize = 10;
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, 1);

    for (int i = 1; i < bufferPoolSize; ++i) {
      appendAndCheckResult(dataBuffer, bufferSize, true, bufferSize * i, i, true);
    }

    // append should fail for insufficient capacity
    int numRecords = bufferPoolSize - 1;
    appendAndCheckResult(dataBuffer, bufferSize, false, bufferSize * numRecords, numRecords, true);
  }

  @Test
  public void testWriteLargeRecord() throws Exception {
    int bufferPoolSize = 10;
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, 1);
    // append should fail for insufficient capacity
    appendAndCheckResult(dataBuffer, bufferPoolSize * bufferSize, false, 0, 0, false);
  }

  private void appendAndCheckResult(
      DataBuffer dataBuffer,
      int recordSize,
      boolean isSuccessful,
      long numBytes,
      long numRecords,
      boolean hasRemaining)
      throws IOException {
    ByteBuffer largeRecord = ByteBuffer.allocate(recordSize);

    assertEquals(isSuccessful, dataBuffer.append(largeRecord, 0, Buffer.DataType.DATA_BUFFER));
    assertEquals(numBytes, dataBuffer.numTotalBytes());
    assertEquals(numRecords, dataBuffer.numTotalRecords());
    assertEquals(hasRemaining, dataBuffer.hasRemaining());
  }

  @Test(expected = IllegalStateException.class)
  public void testReadUnfinishedDataBuffer() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
    dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);

    assertTrue(dataBuffer.hasRemaining());
    dataBuffer.getNextBuffer(
        MemorySegmentFactory.allocateUnpooledSegment(bufferSize), ignore -> {}, 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testReadReleasedDataBuffer() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
    dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
    dataBuffer.finish();
    assertTrue(dataBuffer.hasRemaining());

    dataBuffer.release();
    assertFalse(dataBuffer.hasRemaining());

    dataBuffer.getNextBuffer(
        MemorySegmentFactory.allocateUnpooledSegment(bufferSize), ignore -> {}, 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testReadEmptyDataBuffer() throws Exception {
    int bufferSize = 1024;

    DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
    dataBuffer.finish();

    assertFalse(dataBuffer.hasRemaining());
    dataBuffer.getNextBuffer(
        MemorySegmentFactory.allocateUnpooledSegment(bufferSize), ignore -> {}, 0);
  }

  @Test
  public void testReleaseDataBuffer() throws Exception {
    int bufferPoolSize = 10;
    int bufferSize = 1024;
    int recordSize = (bufferPoolSize - 1) * bufferSize;

    NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
    BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

    DataBuffer dataBuffer = new SortBasedDataBuffer(bufferPool, 1, bufferSize, null);
    dataBuffer.append(ByteBuffer.allocate(recordSize), 0, Buffer.DataType.DATA_BUFFER);

    assertEquals(bufferPoolSize, bufferPool.bestEffortGetNumOfUsedBuffers());
    assertTrue(dataBuffer.hasRemaining());
    assertEquals(1, dataBuffer.numTotalRecords());
    assertEquals(recordSize, dataBuffer.numTotalBytes());

    // should release all data and resources
    dataBuffer.release();
    assertEquals(0, bufferPool.bestEffortGetNumOfUsedBuffers());
    assertFalse(dataBuffer.hasRemaining());
    assertEquals(0, dataBuffer.numTotalRecords());
    assertEquals(0, dataBuffer.numTotalBytes());
  }

  private DataBuffer createDataBuffer(int bufferPoolSize, int bufferSize, int numSubpartitions)
      throws IOException {
    return createDataBuffer(bufferPoolSize, bufferSize, numSubpartitions, null);
  }

  private DataBuffer createDataBuffer(
      int bufferPoolSize, int bufferSize, int numSubpartitions, int[] customReadOrder)
      throws IOException {
    NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
    BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

    return new SortBasedDataBuffer(bufferPool, numSubpartitions, bufferSize, customReadOrder);
  }

  public static int[] getRandomSubpartitionOrder(int numSubpartitions) {
    Random random = new Random(1111);
    int[] subpartitionReadOrder = new int[numSubpartitions];
    int shift = random.nextInt(numSubpartitions);
    for (int i = 0; i < numSubpartitions; ++i) {
      subpartitionReadOrder[i] = (i + shift) % numSubpartitions;
    }
    return subpartitionReadOrder;
  }

  /** Data written and its {@link Buffer.DataType}. */
  public static class DataAndType {
    private final ByteBuffer data;
    private final Buffer.DataType dataType;

    DataAndType(ByteBuffer data, Buffer.DataType dataType) {
      this.data = data;
      this.dataType = dataType;
    }
  }
}
