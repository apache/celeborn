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
import java.util.concurrent.atomic.LongAdder;

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
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.util.Utils;

@Private
public class SortBasedShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(SortBasedShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final ShuffleDependency<K, V, C> dep;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final TaskContext taskContext;
  private final ShuffleClient shuffleClient;
  private final int numMappers;
  private final int numPartitions;

  private final long pushBufferMaxSize;

  private final SortBasedPusher pusher;
  private long peakMemoryUsedBytes = 0;

  private final OpenByteArrayOutputStream serBuffer;
  private final SerializationStream serOutputStream;

  private final LongAdder[] mapStatusLengths;
  private long tmpRecordsWritten = 0;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc. twice.
   */
  private volatile boolean stopping = false;

  private final boolean unsafeRowFastWrite;

  public SortBasedShuffleWriter(
      int shuffleId,
      ShuffleDependency<K, V, C> dep,
      int numMappers,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool)
      throws IOException {
    this(shuffleId, dep, numMappers, taskContext, conf, client, metrics, sendBufferPool, null);
  }

  // In order to facilitate the writing of unit test code, ShuffleClient needs to be passed in as
  // parameters. By the way, simplify the passed parameters.
  public SortBasedShuffleWriter(
      int shuffleId,
      ShuffleDependency<K, V, C> dep,
      int numMappers,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool,
      SortBasedPusher pusher)
      throws IOException {
    this.mapId = taskContext.partitionId();
    this.dep = dep;
    this.shuffleId = shuffleId;
    this.encodedAttemptId = SparkCommonUtils.getEncodedAttemptNumber(taskContext);
    SerializerInstance serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = metrics;
    this.taskContext = taskContext;
    this.numMappers = numMappers;
    this.numPartitions = dep.partitioner().numPartitions();
    this.shuffleClient = client;
    unsafeRowFastWrite = conf.clientPushUnsafeRowFastWrite();

    serBuffer = new OpenByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);

    this.mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      this.mapStatusLengths[i] = new LongAdder();
    }

    pushBufferMaxSize = conf.clientPushBufferMaxSize();

    if (pusher == null) {
      this.pusher =
          new SortBasedPusher(
              taskContext.taskMemoryManager(),
              shuffleClient,
              taskContext,
              shuffleId,
              mapId,
              encodedAttemptId,
              taskContext.taskAttemptId(),
              numMappers,
              numPartitions,
              conf,
              writeMetrics::incBytesWritten,
              mapStatusLengths,
              conf.clientPushSortMemoryThreshold(),
              sendBufferPool);
    } else {
      this.pusher = pusher;
    }
  }

  public SortBasedShuffleWriter(
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool)
      throws IOException {
    this(
        SparkUtils.celebornShuffleId(client, handle, taskContext, true),
        handle.dependency(),
        handle.numMappers(),
        taskContext,
        conf,
        client,
        metrics,
        sendBufferPool);
  }

  public SortBasedShuffleWriter(
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool,
      SortBasedPusher pusher)
      throws IOException {
    this(
        SparkUtils.celebornShuffleId(client, handle, taskContext, true),
        handle.dependency(),
        handle.numMappers(),
        taskContext,
        conf,
        client,
        metrics,
        sendBufferPool,
        pusher);
  }

  private void updatePeakMemoryUsed() {
    long mem = pusher.getPeakMemoryUsedBytes();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  void doWrite(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
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
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    boolean needCleanupPusher = true;
    try {
      doWrite(records);
      close();
      needCleanupPusher = false;
    } finally {
      if (needCleanupPusher) {
        cleanupPusher();
      }
    }
  }

  @VisibleForTesting
  boolean canUseFastWrite() {
    boolean keyIsPartitionId = false;
    if (unsafeRowFastWrite && dep.serializer() instanceof UnsafeRowSerializer) {
      // SPARK-39391 renames PartitionIdPassthrough's package
      String partitionerClassName = partitioner.getClass().getSimpleName();
      keyIsPartitionId = "PartitionIdPassthrough".equals(partitionerClassName);
    }
    return keyIsPartitionId;
  }

  private void fastWrite0(scala.collection.Iterator iterator) throws IOException {
    final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

    SQLMetric dataSize = SparkUtils.getDataSize((UnsafeRowSerializer) dep.serializer());
    while (records.hasNext()) {
      final Product2<Integer, UnsafeRow> record = records.next();
      final int partitionId = record._1();
      final UnsafeRow row = record._2();

      final int rowSize = row.getSizeInBytes();
      final int serializedRecordSize = 4 + rowSize;

      if (dataSize != null) {
        dataSize.add(serializedRecordSize);
      }

      if (serializedRecordSize > pushBufferMaxSize) {
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
        boolean success =
            pusher.insertRecord(
                row.getBaseObject(), row.getBaseOffset(), rowSize, partitionId, true);
        if (!success) {
          doPush();
          success =
              pusher.insertRecord(
                  row.getBaseObject(), row.getBaseOffset(), rowSize, partitionId, true);
          if (!success) {
            throw new CelebornIOException("Unable to push after switching pusher!");
          }
        }
      }
      tmpRecordsWritten++;
    }
  }

  private void doPush() throws IOException {
    long start = System.nanoTime();
    pusher.pushData(true);
    writeMetrics.incWriteTime(System.nanoTime() - start);
  }

  private void write0(scala.collection.Iterator iterator) throws IOException {
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

      if (serializedRecordSize > pushBufferMaxSize) {
        pushGiantRecord(partitionId, serBuffer.getBuf(), serializedRecordSize);
      } else {
        boolean success =
            pusher.insertRecord(
                serBuffer.getBuf(),
                Platform.BYTE_ARRAY_OFFSET,
                serializedRecordSize,
                partitionId,
                false);
        if (!success) {
          doPush();
          success =
              pusher.insertRecord(
                  serBuffer.getBuf(),
                  Platform.BYTE_ARRAY_OFFSET,
                  serializedRecordSize,
                  partitionId,
                  false);
          if (!success) {
            throw new IOException("Unable to push after switching pusher!");
          }
        }
      }
      tmpRecordsWritten++;
    }
  }

  private void pushGiantRecord(int partitionId, byte[] buffer, int numBytes) throws IOException {
    logger.debug("Push giant record, size {}.", Utils.bytesToString(numBytes));
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

  private void cleanupPusher() throws IOException {
    if (pusher != null) {
      pusher.close(false);
    }
  }

  private void close() throws IOException {
    logger.info("Memory used {}", Utils.bytesToString(pusher.getUsed()));
    long pushStartTime = System.nanoTime();
    pusher.pushData(false);
    pusher.close(true);

    shuffleClient.pushMergedData(shuffleId, mapId, encodedAttemptId);
    writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
    writeMetrics.incRecordsWritten(tmpRecordsWritten);

    long waitStartTime = System.nanoTime();
    shuffleClient.mapperEnd(shuffleId, mapId, encodedAttemptId, numMappers);
    writeMetrics.incWriteTime(System.nanoTime() - waitStartTime);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.empty();
      } else {
        stopping = true;
        if (success) {
          BlockManagerId bmId = SparkEnv.get().blockManager().shuffleServerId();
          MapStatus mapStatus =
              SparkUtils.createMapStatus(
                  bmId, SparkUtils.unwrap(mapStatusLengths), taskContext.taskAttemptId());
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.empty();
        }
      }
    } finally {
      shuffleClient.cleanup(shuffleId, mapId, encodedAttemptId);
    }
  }

  // Added in SPARK-32917, for Spark 3.2 and above
  @SuppressWarnings("MissingOverride")
  public long[] getPartitionLengths() {
    throw new UnsupportedOperationException(
        "Celeborn is not compatible with push-based shuffle, please set spark.shuffle.push.enabled to false");
  }
}
