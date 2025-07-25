/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.plugin.flink.buffer.BufferWithSubpartition;
import org.apache.celeborn.plugin.flink.buffer.DataBuffer;
import org.apache.celeborn.plugin.flink.buffer.SortBasedDataBuffer;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

public class RemoteShuffleResultPartitionDelegation {
  public static final Logger LOG =
      LoggerFactory.getLogger(RemoteShuffleResultPartitionDelegation.class);

  /** Size of network buffer and write buffer. */
  public int networkBufferSize;

  /** {@link DataBuffer} for records sent by broadcastRecord. */
  public DataBuffer broadcastDataBuffer;

  /** {@link DataBuffer} for records sent by emitRecord. */
  public DataBuffer unicastDataBuffer;

  /** Utility to spill data to shuffle workers. */
  public RemoteShuffleOutputGate outputGate;

  /** Whether notifyEndOfData has been called or not. */
  private boolean endOfDataNotified;

  private final int numSubpartitions;
  private BufferPool bufferPool;
  private BufferCompressor bufferCompressor;
  private Function<Buffer, Boolean> canBeCompressed;
  private Runnable checkProducerState;
  private final BiConsumer<BufferWithSubpartition, Boolean> statisticsConsumer;

  public RemoteShuffleResultPartitionDelegation(
      int networkBufferSize,
      RemoteShuffleOutputGate outputGate,
      BiConsumer<BufferWithSubpartition, Boolean> statisticsConsumer,
      int numSubpartitions) {
    this.networkBufferSize = networkBufferSize;
    this.outputGate = outputGate;
    this.numSubpartitions = numSubpartitions;
    this.statisticsConsumer = statisticsConsumer;
  }

  public void setup(
      BufferPool bufferPool,
      BufferCompressor bufferCompressor,
      Function<Buffer, Boolean> canBeCompressed,
      Runnable checkProduceState)
      throws IOException {
    LOG.info("Setup {}", this);
    this.bufferPool = bufferPool;
    this.bufferCompressor = bufferCompressor;
    this.canBeCompressed = canBeCompressed;
    this.checkProducerState = checkProduceState;
    try {
      outputGate.setup();
    } catch (Throwable throwable) {
      LOG.error("Failed to setup remote output gate.", throwable);
      Utils.rethrowAsRuntimeException(throwable);
    }
  }

  public void emit(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws IOException {

    checkProducerState.run();
    if (isBroadcast) {
      Preconditions.checkState(
          targetSubpartition == 0, "Target subpartition index can only be 0 when broadcast.");
    }

    DataBuffer dataBuffer = isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();
    if (dataBuffer.append(record, targetSubpartition, dataType)) {
      incNumRecordsOut(dataType);
      return;
    }

    try {
      if (!dataBuffer.hasRemaining()) {
        // the record can not be appended to the free data buffer because it is too large
        dataBuffer.finish();
        dataBuffer.release();
        writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
        incNumRecordsOut(dataType);
        return;
      }
      flushDataBuffer(dataBuffer, isBroadcast);
    } catch (InterruptedException e) {
      LOG.error("Failed to flush the data buffer.", e);
      Utils.rethrowAsRuntimeException(e);
    }
    emit(record, targetSubpartition, dataType, isBroadcast);
  }

  private void incNumRecordsOut(Buffer.DataType dataType) {
    if (dataType.isBuffer()) {
      outputGate.shuffleIOMetricGroup.getNumRecordsOut().inc();
    }
  }

  @VisibleForTesting
  public DataBuffer getUnicastDataBuffer() throws IOException {
    flushBroadcastDataBuffer();

    if (unicastDataBuffer != null && !unicastDataBuffer.isFinished()) {
      return unicastDataBuffer;
    }

    unicastDataBuffer =
        new SortBasedDataBuffer(bufferPool, numSubpartitions, networkBufferSize, null);
    return unicastDataBuffer;
  }

  public DataBuffer getBroadcastDataBuffer() throws IOException {
    flushUnicastDataBuffer();

    if (broadcastDataBuffer != null && !broadcastDataBuffer.isFinished()) {
      return broadcastDataBuffer;
    }

    broadcastDataBuffer =
        new SortBasedDataBuffer(bufferPool, numSubpartitions, networkBufferSize, null);
    return broadcastDataBuffer;
  }

  public void flushBroadcastDataBuffer() throws IOException {
    flushDataBuffer(broadcastDataBuffer, true);
  }

  public void flushUnicastDataBuffer() throws IOException {
    flushDataBuffer(unicastDataBuffer, false);
  }

  @VisibleForTesting
  void flushDataBuffer(DataBuffer dataBuffer, boolean isBroadcast) throws IOException {
    if (dataBuffer == null || dataBuffer.isReleased()) {
      return;
    }
    dataBuffer.finish();
    if (dataBuffer.hasRemaining()) {
      try {
        outputGate.regionStart(isBroadcast);
        while (dataBuffer.hasRemaining()) {
          MemorySegment segment = outputGate.getBufferPool().requestMemorySegmentBlocking();
          BufferWithSubpartition bufferWithSubpartition;
          try {
            bufferWithSubpartition =
                dataBuffer.getNextBuffer(
                    segment, outputGate.getBufferPool(), BufferUtils.HEADER_LENGTH);
          } catch (Throwable t) {
            outputGate.getBufferPool().recycle(segment);
            throw new FlinkRuntimeException("Shuffle write failure.", t);
          }

          Buffer buffer = bufferWithSubpartition.getBuffer();
          int subpartitionIndex = bufferWithSubpartition.getSubpartitionIndex();
          statisticsConsumer.accept(bufferWithSubpartition, isBroadcast);
          writeCompressedBufferIfPossible(buffer, subpartitionIndex);
        }
        outputGate.regionFinish();
      } catch (InterruptedException e) {
        throw new IOException("Failed to flush the data buffer, broadcast=" + isBroadcast, e);
      }
    }
    releaseDataBuffer(dataBuffer);
  }

  public void writeCompressedBufferIfPossible(Buffer buffer, int targetSubpartition)
      throws InterruptedException {
    Buffer compressedBuffer = null;
    try {
      if (canBeCompressed.apply(buffer)) {
        Buffer dataBuffer =
            buffer.readOnlySlice(
                BufferUtils.HEADER_LENGTH, buffer.getSize() - BufferUtils.HEADER_LENGTH);
        compressedBuffer =
            Utils.checkNotNull(bufferCompressor).compressToIntermediateBuffer(dataBuffer);
      }
      BufferUtils.setCompressedDataWithHeader(buffer, compressedBuffer);
    } catch (Throwable throwable) {
      buffer.recycleBuffer();
      throw new RuntimeException("Shuffle write failure.", throwable);
    } finally {
      if (compressedBuffer != null && compressedBuffer.isCompressed()) {
        compressedBuffer.setReaderIndex(0);
        compressedBuffer.recycleBuffer();
      }
    }
    outputGate.write(buffer, targetSubpartition);
  }

  /** Spills the large record into {@link RemoteShuffleOutputGate}. */
  public void writeLargeRecord(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws InterruptedException {

    outputGate.regionStart(isBroadcast);
    while (record.hasRemaining()) {
      MemorySegment writeBuffer = outputGate.getBufferPool().requestMemorySegmentBlocking();
      int toCopy = Math.min(record.remaining(), writeBuffer.size() - BufferUtils.HEADER_LENGTH);
      writeBuffer.put(BufferUtils.HEADER_LENGTH, record, toCopy);
      NetworkBuffer buffer =
          new NetworkBuffer(
              writeBuffer,
              outputGate.getBufferPool(),
              dataType,
              toCopy + BufferUtils.HEADER_LENGTH);

      BufferWithSubpartition bufferWithSubpartition =
          new BufferWithSubpartition(buffer, targetSubpartition);
      statisticsConsumer.accept(bufferWithSubpartition, isBroadcast);
      writeCompressedBufferIfPossible(buffer, targetSubpartition);
    }
    outputGate.regionFinish();
  }

  public void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
    emit(record, 0, dataType, true);
  }

  public void releaseDataBuffer(DataBuffer dataBuffer) {
    if (dataBuffer != null) {
      dataBuffer.release();
    }
  }

  public void finish() throws IOException {
    Utils.checkState(
        unicastDataBuffer == null || unicastDataBuffer.isReleased(),
        "The unicast data buffer should be either null or released.");
    flushBroadcastDataBuffer();
    try {
      outputGate.finish();
    } catch (InterruptedException e) {
      throw new IOException("Output gate fails to finish.", e);
    }
  }

  public synchronized void close(Runnable closeHandler) {
    Throwable closeException;
    closeException =
        checkException(
            () -> releaseDataBuffer(unicastDataBuffer),
            null,
            "Failed to release unicast data buffer.");

    closeException =
        checkException(
            () -> releaseDataBuffer(broadcastDataBuffer),
            closeException,
            "Failed to release broadcast data buffer.");

    closeException =
        checkException(closeHandler, closeException, "Failed to call super#close() method.");

    try {
      outputGate.close();
    } catch (Throwable throwable) {
      closeException = closeException == null ? throwable : closeException;
      LOG.error("Failed to close remote shuffle output gate.", throwable);
    }

    if (closeException != null) {
      Utils.rethrowAsRuntimeException(closeException);
    }
  }

  public Throwable checkException(Runnable runnable, Throwable exception, String errorMessage) {
    Throwable newException = null;
    try {
      runnable.run();
    } catch (Throwable throwable) {
      newException = exception == null ? throwable : exception;
      LOG.error(errorMessage, throwable);
    }
    return newException;
  }

  public void flushAll() {
    try {
      flushUnicastDataBuffer();
      flushBroadcastDataBuffer();
    } catch (Throwable t) {
      LOG.error("Failed to flush the current data buffer.", t);
      Utils.rethrowAsRuntimeException(t);
    }
  }

  public RemoteShuffleOutputGate getOutputGate() {
    return outputGate;
  }

  public boolean isEndOfDataNotified() {
    return endOfDataNotified;
  }

  public void setEndOfDataNotified(boolean endOfDataNotified) {
    this.endOfDataNotified = endOfDataNotified;
  }
}
