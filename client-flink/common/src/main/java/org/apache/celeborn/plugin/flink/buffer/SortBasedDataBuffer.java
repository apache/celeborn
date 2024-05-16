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

import static org.apache.celeborn.plugin.flink.utils.Utils.checkArgument;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkNotNull;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * A {@link DataBuffer} implementation which sorts all appended records only by subpartition index.
 * Records of the same subpartition keep the appended order.
 *
 * <p>It maintains a list of {@link MemorySegment}s as a joint buffer. Data will be appended to the
 * joint buffer sequentially. When writing a record, an index entry will be appended first. An index
 * entry consists of 4 fields: 4 bytes for record length, 4 bytes for {@link Buffer.DataType} and 8
 * bytes for address pointing to the next index entry of the same subpartition which will be used to
 * index the next record to read when coping data from this {@link DataBuffer}. For simplicity, no
 * index entry can span multiple segments. The corresponding record data is seated right after its
 * index entry and different from the index entry, records have variable length thus may span
 * multiple segments.
 */
@NotThreadSafe
public class SortBasedDataBuffer implements DataBuffer {

  /**
   * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
   * pointer to next entry.
   */
  private static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

  /** A buffer pool to request memory segments from. */
  private final BufferPool bufferPool;

  /** A segment list as a joint buffer which stores all records and index entries. */
  private final ArrayList<MemorySegment> buffers = new ArrayList<>();

  /** Addresses of the first record's index entry for each subpartition. */
  private final long[] firstIndexEntryAddresses;

  /** Addresses of the last record's index entry for each subpartition. */
  private final long[] lastIndexEntryAddresses;
  /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
  private final int bufferSize;
  /** Data of different subpartitions in this data buffer will be read in this order. */
  private final int[] subpartitionReadOrder;

  // ---------------------------------------------------------------------------------------------
  // Statistics and states
  // ---------------------------------------------------------------------------------------------
  /** Total number of bytes already appended to this data buffer. */
  private long numTotalBytes;
  /** Total number of records already appended to this data buffer. */
  private long numTotalRecords;
  /** Total number of bytes already read from this data buffer. */
  private long numTotalBytesRead;
  /** Whether this data buffer is finished. One can only read a finished data buffer. */
  private boolean isFinished;

  // ---------------------------------------------------------------------------------------------
  // For writing
  // ---------------------------------------------------------------------------------------------
  /** Whether this data buffer is released. A released data buffer can not be used. */
  private boolean isReleased;
  /** Array index in the segment list of the current available buffer for writing. */
  private int writeSegmentIndex;

  // ---------------------------------------------------------------------------------------------
  // For reading
  // ---------------------------------------------------------------------------------------------
  /** Next position in the current available buffer for writing. */
  private int writeSegmentOffset;
  /** Index entry address of the current record or event to be read. */
  private long readIndexEntryAddress;

  /** Record bytes remaining after last copy, which must be read first in next copy. */
  private int recordRemainingBytes;

  /** Used to index the current available subpartition to read data from. */
  private int readOrderIndex = -1;

  public SortBasedDataBuffer(
      BufferPool bufferPool,
      int numSubpartitions,
      int bufferSize,
      @Nullable int[] customReadOrder) {
    checkArgument(bufferSize > INDEX_ENTRY_SIZE, "Buffer size is too small.");

    this.bufferPool = checkNotNull(bufferPool);
    this.bufferSize = bufferSize;
    this.firstIndexEntryAddresses = new long[numSubpartitions];
    this.lastIndexEntryAddresses = new long[numSubpartitions];

    // initialized with -1 means the corresponding subpartition has no data.
    Arrays.fill(firstIndexEntryAddresses, -1L);
    Arrays.fill(lastIndexEntryAddresses, -1L);

    this.subpartitionReadOrder = new int[numSubpartitions];
    if (customReadOrder != null) {
      checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
      System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
    } else {
      for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
        this.subpartitionReadOrder[subpartition] = subpartition;
      }
    }
  }

  @Override
  public boolean append(ByteBuffer source, int targetSubpartition, DataType dataType)
      throws IOException {
    checkArgument(source.hasRemaining(), "Cannot append empty data.");
    checkState(!isFinished, "Data buffer is already finished.");
    checkState(!isReleased, "Data buffer is already released.");

    int totalBytes = source.remaining();

    // return false directly if it can not allocate enough buffers for the given record
    if (!allocateBuffersForRecord(totalBytes)) {
      return false;
    }

    // write the index entry and record or event data
    writeIndex(targetSubpartition, totalBytes, dataType);
    writeRecord(source);

    ++numTotalRecords;
    numTotalBytes += totalBytes;

    return true;
  }

  private void writeIndex(int subpartitionIndex, int numRecordBytes, DataType dataType) {
    MemorySegment segment = buffers.get(writeSegmentIndex);

    // record length takes the high 32 bits and data type takes the low 32 bits
    segment.putLong(writeSegmentOffset, ((long) numRecordBytes << 32) | dataType.ordinal());

    // segment index takes the high 32 bits and segment offset takes the low 32 bits
    long indexEntryAddress = ((long) writeSegmentIndex << 32) | writeSegmentOffset;

    long lastIndexEntryAddress = lastIndexEntryAddresses[subpartitionIndex];
    lastIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;

    if (lastIndexEntryAddress >= 0) {
      // link the previous index entry of the given subpartition to the new index entry
      segment = buffers.get(getSegmentIndexFromPointer(lastIndexEntryAddress));
      segment.putLong(getSegmentOffsetFromPointer(lastIndexEntryAddress) + 8, indexEntryAddress);
    } else {
      firstIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;
    }

    // move the writer position forward to write the corresponding record
    updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
  }

  private void writeRecord(ByteBuffer source) {
    while (source.hasRemaining()) {
      MemorySegment segment = buffers.get(writeSegmentIndex);
      int toCopy = Math.min(bufferSize - writeSegmentOffset, source.remaining());
      segment.put(writeSegmentOffset, source, toCopy);

      // move the writer position forward to write the remaining bytes or next record
      updateWriteSegmentIndexAndOffset(toCopy);
    }
  }

  private boolean allocateBuffersForRecord(int numRecordBytes) throws IOException {
    int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
    int availableBytes = writeSegmentIndex == buffers.size() ? 0 : bufferSize - writeSegmentOffset;

    // return directly if current available bytes is adequate
    if (availableBytes >= numBytesRequired) {
      return true;
    }

    // skip the remaining free space if the available bytes is not enough for an index entry
    if (availableBytes < INDEX_ENTRY_SIZE) {
      updateWriteSegmentIndexAndOffset(availableBytes);
      availableBytes = 0;
    }

    // allocate exactly enough buffers for the appended record
    do {
      MemorySegment segment = requestBufferFromPool();
      if (segment == null) {
        // return false if we can not allocate enough buffers for the appended record
        return false;
      }

      availableBytes += bufferSize;
      addBuffer(segment);
    } while (availableBytes < numBytesRequired);

    return true;
  }

  private void addBuffer(MemorySegment segment) {
    if (segment.size() != bufferSize) {
      bufferPool.recycle(segment);
      throw new IllegalStateException("Illegal memory segment size.");
    }

    if (isReleased) {
      bufferPool.recycle(segment);
      throw new IllegalStateException("Data buffer is already released.");
    }

    buffers.add(segment);
  }

  private MemorySegment requestBufferFromPool() throws IOException {
    try {
      // blocking request buffers if there is still guaranteed memory
      if (buffers.size() < bufferPool.getNumberOfRequiredMemorySegments()) {
        return bufferPool.requestMemorySegmentBlocking();
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while requesting buffer.");
    }

    return bufferPool.requestMemorySegment();
  }

  private void updateWriteSegmentIndexAndOffset(int numBytes) {
    writeSegmentOffset += numBytes;

    // using the next available free buffer if the current is full
    if (writeSegmentOffset == bufferSize) {
      ++writeSegmentIndex;
      writeSegmentOffset = 0;
    }
  }

  @Override
  public BufferWithSubpartition getNextBuffer(
      MemorySegment transitBuffer, BufferRecycler recycler, int offset) {
    checkState(hasRemaining(), "No data remaining.");
    checkState(isFinished, "Should finish the data buffer first before coping any data.");
    checkState(!isReleased, "Data buffer is already released.");

    int numBytesCopied = 0;
    DataType bufferDataType = DataType.DATA_BUFFER;
    int subpartitionIndex = subpartitionReadOrder[readOrderIndex];

    do {
      int sourceSegmentIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
      int sourceSegmentOffset = getSegmentOffsetFromPointer(readIndexEntryAddress);
      MemorySegment sourceSegment = buffers.get(sourceSegmentIndex);

      long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
      int length = getSegmentIndexFromPointer(lengthAndDataType);
      DataType dataType = DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

      // return the data read directly if the next to read is an event
      if (dataType.isEvent() && numBytesCopied > 0) {
        break;
      }
      bufferDataType = dataType;

      // get the next index entry address and move the read position forward
      long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
      sourceSegmentOffset += INDEX_ENTRY_SIZE;

      // throws if the event is too big to be accommodated by a buffer.
      if (bufferDataType.isEvent() && transitBuffer.size() - offset < length) {
        throw new FlinkRuntimeException("Event is too big to be accommodated by a buffer");
      }

      numBytesCopied +=
          copyRecordOrEvent(
              transitBuffer,
              numBytesCopied + offset,
              sourceSegmentIndex,
              sourceSegmentOffset,
              length);

      if (recordRemainingBytes == 0) {
        // move to next subpartition if the current subpartition has been finished
        if (readIndexEntryAddress == lastIndexEntryAddresses[subpartitionIndex]) {
          updateReadChannelAndIndexEntryAddress();
          break;
        }
        readIndexEntryAddress = nextReadIndexEntryAddress;
      }
    } while (numBytesCopied < transitBuffer.size() - offset && bufferDataType.isBuffer());

    numTotalBytesRead += numBytesCopied;
    Buffer buffer =
        new NetworkBuffer(transitBuffer, recycler, bufferDataType, numBytesCopied + offset);
    return new BufferWithSubpartition(buffer, subpartitionIndex);
  }

  private int copyRecordOrEvent(
      MemorySegment targetSegment,
      int targetSegmentOffset,
      int sourceSegmentIndex,
      int sourceSegmentOffset,
      int recordLength) {
    if (recordRemainingBytes > 0) {
      // skip the data already read if there is remaining partial record after the previous
      // copy
      long position = (long) sourceSegmentOffset + (recordLength - recordRemainingBytes);
      sourceSegmentIndex = (int) (sourceSegmentIndex + (position / bufferSize));
      sourceSegmentOffset = (int) (position % bufferSize);
    } else {
      recordRemainingBytes = recordLength;
    }

    int targetSegmentSize = targetSegment.size();
    int numBytesToCopy = Math.min(targetSegmentSize - targetSegmentOffset, recordRemainingBytes);
    do {
      // move to next data buffer if all data of the current buffer has been copied
      if (sourceSegmentOffset == bufferSize) {
        ++sourceSegmentIndex;
        sourceSegmentOffset = 0;
      }

      int sourceRemainingBytes = Math.min(bufferSize - sourceSegmentOffset, recordRemainingBytes);
      int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
      MemorySegment sourceSegment = buffers.get(sourceSegmentIndex);
      sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);

      recordRemainingBytes -= numBytes;
      targetSegmentOffset += numBytes;
      sourceSegmentOffset += numBytes;
    } while (recordRemainingBytes > 0 && targetSegmentOffset < targetSegmentSize);

    return numBytesToCopy;
  }

  private void updateReadChannelAndIndexEntryAddress() {
    // skip the subpartitions without any data
    while (++readOrderIndex < firstIndexEntryAddresses.length) {
      int subpartitionIndex = subpartitionReadOrder[readOrderIndex];
      if ((readIndexEntryAddress = firstIndexEntryAddresses[subpartitionIndex]) >= 0) {
        break;
      }
    }
  }

  private int getSegmentIndexFromPointer(long value) {
    return (int) (value >>> 32);
  }

  private int getSegmentOffsetFromPointer(long value) {
    return (int) value;
  }

  @Override
  public long numTotalRecords() {
    return numTotalRecords;
  }

  @Override
  public long numTotalBytes() {
    return numTotalBytes;
  }

  @Override
  public boolean hasRemaining() {
    return numTotalBytesRead < numTotalBytes;
  }

  @Override
  public void finish() {
    checkState(!isFinished, DataBuffer.class.getCanonicalName() + " is already finished.");

    isFinished = true;

    // prepare for reading
    updateReadChannelAndIndexEntryAddress();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public void release() {
    // the data buffer can be released by other threads
    if (isReleased) {
      return;
    }

    isReleased = true;

    for (MemorySegment segment : buffers) {
      bufferPool.recycle(segment);
    }
    buffers.clear();

    numTotalBytes = 0;
    numTotalRecords = 0;
  }

  @Override
  public boolean isReleased() {
    return isReleased;
  }
}
