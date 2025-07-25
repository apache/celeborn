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

package org.apache.spark.shuffle.celeborn;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

import javax.annotation.Nullable;

import scala.Option;
import scala.Product2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.PartitionIdPassthrough;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.write.DataPusher;
import org.apache.celeborn.client.write.PushTask;
import org.apache.celeborn.common.CelebornConf;

@Private
public class HashBasedShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(HashBasedShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final int PUSH_BUFFER_INIT_SIZE;
  private final int PUSH_BUFFER_MAX_SIZE;
  private final ShuffleDependency<K, V, C> dep;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final TaskContext taskContext;
  private final ShuffleClient shuffleClient;
  private final int numMappers;
  private final int numPartitions;

  @Nullable private MapStatus mapStatus;
  private long peakMemoryUsedBytes = 0;

  private final OpenByteArrayOutputStream serBuffer;
  private final SerializationStream serOutputStream;

  private byte[][] sendBuffers;
  private int[] sendOffsets;

  private final LongAdder[] mapStatusLengths;
  private final long[] mapStatusRecords;
  private final long[] tmpRecords;

  private SendBufferPool sendBufferPool;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private volatile boolean stopping = false;

  private DataPusher dataPusher;
  private final boolean unsafeRowFastWrite;

  // In order to facilitate the writing of unit test code, ShuffleClient needs to be passed in as
  // parameters. By the way, simplify the passed parameters.
  public HashBasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      int mapId,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      SendBufferPool sendBufferPool)
      throws IOException {
    this.mapId = mapId;
    this.dep = handle.dependency();
    this.shuffleId = shuffleId;
    this.encodedAttemptId = SparkCommonUtils.getEncodedAttemptNumber(taskContext);
    SerializerInstance serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.numMappers = handle.numMaps();
    this.numPartitions = dep.partitioner().numPartitions();

    this.shuffleClient = client;

    unsafeRowFastWrite = conf.clientPushUnsafeRowFastWrite();
    serBuffer = new OpenByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);

    mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }

    mapStatusRecords = new long[numPartitions];
    tmpRecords = new long[numPartitions];

    PUSH_BUFFER_INIT_SIZE = conf.clientPushBufferInitialSize();
    PUSH_BUFFER_MAX_SIZE = conf.clientPushBufferMaxSize();

    this.sendBufferPool = sendBufferPool;
    sendBuffers = sendBufferPool.acquireBuffer(numPartitions);
    sendOffsets = new int[numPartitions];

    try {
      LinkedBlockingQueue<PushTask> pushTaskQueue = sendBufferPool.acquirePushTaskQueue();
      dataPusher =
          new DataPusher(
              shuffleId,
              mapId,
              encodedAttemptId,
              taskContext.taskAttemptId(),
              numMappers,
              numPartitions,
              conf,
              shuffleClient,
              pushTaskQueue,
              writeMetrics::incBytesWritten,
              mapStatusLengths);
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    }
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    boolean needCleanupPusher = true;
    try {
      if (canUseFastWrite()) {
        fastWrite0(records);
      } else if (dep.mapSideCombine()) {
        if (dep.aggregator().isEmpty()) {
          throw new UnsupportedOperationException(
              "When using map side combine, an aggregator must be specified.");
        }
        write0(dep.aggregator().get().combineValuesByKey(records, taskContext));
      } else {
        write0(records);
      }
      close();
      needCleanupPusher = false;
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    } finally {
      if (needCleanupPusher) {
        cleanupPusher();
      }
    }
  }

  @VisibleForTesting
  boolean canUseFastWrite() {
    return unsafeRowFastWrite
        && dep.serializer() instanceof UnsafeRowSerializer
        && partitioner instanceof PartitionIdPassthrough;
  }

  private void fastWrite0(scala.collection.Iterator iterator)
      throws IOException, InterruptedException {
    final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

    SQLMetric dataSize =
        SparkUtils.getUnsafeRowSerializerDataSizeMetric((UnsafeRowSerializer) dep.serializer());
    while (records.hasNext()) {
      final Product2<Integer, UnsafeRow> record = records.next();
      final int partitionId = record._1();
      final UnsafeRow row = record._2();

      final int rowSize = row.getSizeInBytes();
      final int serializedRecordSize = 4 + rowSize;
      if (dataSize != null) {
        dataSize.add(serializedRecordSize);
      }

      if (serializedRecordSize > PUSH_BUFFER_MAX_SIZE) {
        byte[] giantBuffer = new byte[serializedRecordSize];
        Platform.putInt(giantBuffer, Platform.BYTE_ARRAY_OFFSET, Integer.reverseBytes(rowSize));
        Platform.copyMemory(
            row.getBaseObject(),
            row.getBaseOffset(),
            giantBuffer,
            Platform.BYTE_ARRAY_OFFSET + 4,
            rowSize);
        pushGiantRecord(partitionId, giantBuffer, serializedRecordSize);
      } else {
        int offset = getOrUpdateOffset(partitionId, serializedRecordSize);
        byte[] buffer = getOrCreateBuffer(partitionId);
        Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset, Integer.reverseBytes(rowSize));
        Platform.copyMemory(
            row.getBaseObject(),
            row.getBaseOffset(),
            buffer,
            Platform.BYTE_ARRAY_OFFSET + offset + 4,
            rowSize);
        sendOffsets[partitionId] = offset + serializedRecordSize;
      }
      tmpRecords[partitionId] += 1;
    }
  }

  private void write0(scala.collection.Iterator iterator) throws IOException, InterruptedException {
    final scala.collection.Iterator<Product2<K, ?>> records = iterator;

    while (records.hasNext()) {
      final Product2<K, ?> record = records.next();
      final K key = record._1();
      final int partitionId = partitioner.getPartition(key);
      serBuffer.reset();
      serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
      serOutputStream.flush();

      final int serializedRecordSize = serBuffer.size();
      assert (serializedRecordSize > 0);

      if (serializedRecordSize > PUSH_BUFFER_MAX_SIZE) {
        pushGiantRecord(partitionId, serBuffer.getBuf(), serializedRecordSize);
      } else {
        int offset = getOrUpdateOffset(partitionId, serializedRecordSize);
        byte[] buffer = getOrCreateBuffer(partitionId);
        System.arraycopy(serBuffer.getBuf(), 0, buffer, offset, serializedRecordSize);
        sendOffsets[partitionId] = offset + serializedRecordSize;
      }
      tmpRecords[partitionId] += 1;
    }
  }

  private byte[] getOrCreateBuffer(int partitionId) {
    byte[] buffer = sendBuffers[partitionId];
    if (buffer == null) {
      buffer = new byte[PUSH_BUFFER_INIT_SIZE];
      sendBuffers[partitionId] = buffer;
      peakMemoryUsedBytes += PUSH_BUFFER_INIT_SIZE;
    }
    return buffer;
  }

  private void pushGiantRecord(int partitionId, byte[] buffer, int numBytes) throws IOException {
    logger.debug("Push giant record for partition {}, size {}.", partitionId, numBytes);
    int bytesWritten =
        shuffleClient.pushData(
            shuffleId,
            mapId,
            encodedAttemptId,
            partitionId,
            buffer,
            0,
            numBytes,
            numMappers,
            numPartitions);
    mapStatusLengths[partitionId].add(bytesWritten);
    writeMetrics.incBytesWritten(bytesWritten);
  }

  private int getOrUpdateOffset(int partitionId, int serializedRecordSize)
      throws IOException, InterruptedException {
    int offset = sendOffsets[partitionId];
    byte[] buffer = getOrCreateBuffer(partitionId);
    while ((buffer.length - offset) < serializedRecordSize
        && buffer.length < PUSH_BUFFER_MAX_SIZE) {

      byte[] newBuffer = new byte[Math.min(buffer.length * 2, PUSH_BUFFER_MAX_SIZE)];
      peakMemoryUsedBytes += newBuffer.length - buffer.length;
      System.arraycopy(buffer, 0, newBuffer, 0, offset);
      sendBuffers[partitionId] = newBuffer;
      buffer = newBuffer;
    }

    if ((buffer.length - offset) < serializedRecordSize) {
      flushSendBuffer(partitionId, buffer, offset);
      updateMapStatus();
      offset = 0;
    }
    return offset;
  }

  private void flushSendBuffer(int partitionId, byte[] buffer, int size)
      throws IOException, InterruptedException {
    long start = System.nanoTime();
    logger.debug("Flush buffer for partition {}, size {}.", partitionId, size);
    dataPusher.addTask(partitionId, buffer, size);
    writeMetrics.incWriteTime(System.nanoTime() - start);
  }

  private void cleanupPusher() throws IOException {
    try {
      dataPusher.waitOnTermination();
      sendBufferPool.returnPushTaskQueue(dataPusher.getAndResetIdleQueue());
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    }
  }

  private void close() throws IOException, InterruptedException {
    // here we wait for all the in-flight batches to return which sent by dataPusher thread
    dataPusher.waitOnTermination();
    sendBufferPool.returnPushTaskQueue(dataPusher.getAndResetIdleQueue());
    shuffleClient.prepareForMergeData(shuffleId, mapId, encodedAttemptId);

    // merge and push residual data to reduce network traffic
    // NB: since dataPusher thread have no in-flight data at this point,
    //     we now push merged data by task thread will not introduce any contention
    for (int i = 0; i < sendBuffers.length; i++) {
      final int size = sendOffsets[i];
      if (size > 0) {
        int bytesWritten =
            shuffleClient.mergeData(
                shuffleId,
                mapId,
                encodedAttemptId,
                i,
                sendBuffers[i],
                0,
                size,
                numMappers,
                numPartitions);
        // free buffer
        sendBuffers[i] = null;
        mapStatusLengths[i].add(bytesWritten);
        writeMetrics.incBytesWritten(bytesWritten);
      }
    }
    shuffleClient.pushMergedData(shuffleId, mapId, encodedAttemptId);

    updateMapStatus();

    sendBufferPool.returnBuffer(sendBuffers);
    sendBuffers = null;
    sendOffsets = null;

    long waitStartTime = System.nanoTime();
    shuffleClient.mapperEnd(shuffleId, mapId, encodedAttemptId, numMappers, numPartitions);
    writeMetrics.incWriteTime(System.nanoTime() - waitStartTime);

    BlockManagerId bmId = SparkEnv.get().blockManager().shuffleServerId();
    mapStatus =
        SparkUtils.createMapStatus(bmId, SparkUtils.unwrap(mapStatusLengths), mapStatusRecords);
  }

  private void updateMapStatus() {
    long recordsWritten = 0;
    for (int i = 0; i < partitioner.numPartitions(); i++) {
      mapStatusRecords[i] += tmpRecords[i];
      recordsWritten += tmpRecords[i];
      tmpRecords[i] = 0;
    }
    writeMetrics.incRecordsWritten(recordsWritten);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes);

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      shuffleClient.cleanup(shuffleId, mapId, encodedAttemptId);
    }
  }
}
