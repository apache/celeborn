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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.ListBuffer;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkVersionUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.columnar.CelebornBatchBuilder;
import org.apache.spark.sql.execution.columnar.CelebornColumnarBatchSerializer;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.celeborn.client.DummyShuffleClient;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.reflect.DynConstructors;

public class ColumnarHashBasedShuffleWriterSuiteJ extends CelebornShuffleWriterSuiteBase {

  private final StructType schema =
      new StructType().add("key", IntegerType$.MODULE$).add("value", StringType$.MODULE$);

  @Test
  public void createColumnarShuffleWriter() throws Exception {
    Mockito.doReturn(new HashPartitioner(numPartitions)).when(dependency).partitioner();
    final CelebornConf conf = new CelebornConf();
    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    final DummyShuffleClient client = new DummyShuffleClient(conf, tempFile);
    client.initReducePartitionMap(shuffleId, numPartitions, 1);

    // Create ColumnarHashBasedShuffleWriter with handle of which dependency has null schema.
    Mockito.doReturn(new KryoSerializer(sparkConf)).when(dependency).serializer();
    ShuffleWriter<Integer, String> writer =
        createShuffleWriterWithoutSchema(
            new CelebornShuffleHandle<>(
                "appId", "host", 0, this.userIdentifier, 0, false, 10, this.dependency),
            taskContext,
            conf,
            client,
            metrics.shuffleWriteMetrics());
    assertTrue(writer instanceof ColumnarHashBasedShuffleWriter);
    assertFalse(((ColumnarHashBasedShuffleWriter<?, ?, ?>) writer).isColumnarShuffle());

    // Create ColumnarHashBasedShuffleWriter with handle of which dependency has non-null schema.
    Mockito.doReturn(new UnsafeRowSerializer(2, null)).when(dependency).serializer();
    writer =
        createShuffleWriter(
            new CelebornShuffleHandle<>(
                "appId", "host", 0, this.userIdentifier, 0, false, 10, this.dependency),
            taskContext,
            conf,
            client,
            metrics.shuffleWriteMetrics());
    assertTrue(((ColumnarHashBasedShuffleWriter<?, ?, ?>) writer).isColumnarShuffle());
  }

  @Test
  public void testPushBatchWhenSizeInBytesThresholdReached() throws Exception {
    // A (key, 100-byte value) UnsafeRow takes 8 + 2 * 8 + 104 = 128 bytes, so a 1k threshold
    // pushes a batch every 8 rows.
    final CelebornConf conf =
        new CelebornConf()
            .set(CelebornConf.COLUMNAR_SHUFFLE_BATCH_SIZE_IN_BYTES_ENABLED().key(), "true")
            .set(CelebornConf.COLUMNAR_SHUFFLE_BATCH_SIZE_IN_BYTES_THRESHOLD().key(), "1k");
    assertArrayEquals(new int[] {8, 8, 8, 6}, writeAndGetBatchRowCounts(conf, 30));

    // Without it, only the row limit applies and all rows stay in one batch.
    assertArrayEquals(new int[] {30}, writeAndGetBatchRowCounts(new CelebornConf(), 30));

    // The row limit still applies when byte-based batching is enabled.
    conf.set(CelebornConf.COLUMNAR_SHUFFLE_BATCH_SIZE().key(), "3");
    assertArrayEquals(new int[] {3, 3, 3, 1}, writeAndGetBatchRowCounts(conf, 10));
  }

  @Test
  public void testInterleavedByteLimitedBatchesRoundTrip() throws Exception {
    // Uneven partition sizes exercise independent byte counts, resets and final partial batches.
    final int[] partitions = new int[28];
    for (int i = 0; i < partitions.length; i++) {
      partitions[i] = i % 3 == 0 ? 1 : 0;
    }
    for (String mode : new String[] {"plain", "codegen", "dictionary"}) {
      final CelebornConf conf =
          new CelebornConf()
              .set(CelebornConf.COLUMNAR_SHUFFLE_BATCH_SIZE_IN_BYTES_ENABLED().key(), "true")
              .set(CelebornConf.COLUMNAR_SHUFFLE_BATCH_SIZE_IN_BYTES_THRESHOLD().key(), "1k")
              .set(
                  CelebornConf.COLUMNAR_SHUFFLE_CODEGEN_ENABLED().key(),
                  Boolean.toString(mode.equals("codegen")))
              .set(
                  CelebornConf.COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_ENABLED().key(),
                  Boolean.toString(mode.equals("dictionary")));
      Map<Integer, List<byte[]>> batches = writeAndCheckBatches(conf, partitions);
      assertArrayEquals(mode, new int[] {8, 8, 2}, batchRowCounts(batches.get(0)));
      assertArrayEquals(mode, new int[] {8, 2}, batchRowCounts(batches.get(1)));
    }
  }

  private int[] batchRowCounts(List<byte[]> batches) {
    return batches.stream().mapToInt(batch -> ByteBuffer.wrap(batch).getInt()).toArray();
  }

  private int[] writeAndGetBatchRowCounts(CelebornConf conf, int numRows) throws Exception {
    return batchRowCounts(writeAndCheckBatches(conf, new int[numRows]).get(0));
  }

  private Map<Integer, List<byte[]>> writeAndCheckBatches(CelebornConf conf, int[] partitions)
      throws Exception {
    final String partitionIdPassthroughClazz =
        SparkVersionUtil.isGreaterThan(3, 3)
            ? "org.apache.spark.PartitionIdPassthrough"
            : "org.apache.spark.sql.execution.PartitionIdPassthrough";
    final Partitioner partitioner =
        DynConstructors.builder()
            .impl(partitionIdPassthroughClazz, int.class)
            .<Partitioner>build()
            .newInstance(numPartitions);
    Mockito.doReturn(partitioner).when(dependency).partitioner();
    Mockito.doReturn(new UnsafeRowSerializer(2, null)).when(dependency).serializer();

    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    final DummyShuffleClient client = new DummyShuffleClient(conf, tempFile);
    client.initReducePartitionMap(shuffleId, numPartitions, 1);
    final ShuffleWriter<Integer, String> writer =
        createShuffleWriter(
            new CelebornShuffleHandle<>(
                appId, host, port, userIdentifier, shuffleId, false, numMaps, dependency),
            taskContext,
            conf,
            client,
            metrics.shuffleWriteMetrics());

    final UnsafeProjection projection = UnsafeProjection.create(schema);
    final Map<Integer, List<UnsafeRow>> expected = new HashMap<>();
    final ListBuffer<Product2<Integer, UnsafeRow>> records = new ListBuffer<>();
    for (int i = 0; i < partitions.length; i++) {
      // Repeated 100-byte strings keep rows at 128 bytes and allow dictionary encoding.
      UTF8String value = UTF8String.fromString(String.format("%0100d", i % 2));
      UnsafeRow row = projection.apply(new GenericInternalRow(new Object[] {i, value})).copy();
      records.$plus$eq(new Tuple2<>(partitions[i], row));
      expected.computeIfAbsent(partitions[i], ignored -> new ArrayList<>()).add(row);
    }
    writer.write((Iterator) records.toIterator());
    writer.stop(true);
    client.shutdown();

    // mapId=0 and attemptId=0 from the mock TaskContext.
    Map<Integer, List<byte[]>> batches =
        client.getPushDataByPartition(Utils.makeMapKey(shuffleId, 0, 0));
    assertEquals(expected.keySet(), batches.keySet());
    for (Map.Entry<Integer, List<byte[]>> partition : batches.entrySet()) {
      List<UnsafeRow> actual = new ArrayList<>();
      for (byte[] batch : partition.getValue()) {
        try (DeserializationStream stream =
            newSerializerInstance(new UnsafeRowSerializer(2, null))
                .deserializeStream(new ByteArrayInputStream(batch))) {
          Iterator<Tuple2<Object, Object>> rows = stream.asKeyValueIterator();
          while (rows.hasNext()) {
            actual.add(((UnsafeRow) rows.next()._2()).copy());
          }
        }
      }
      assertEquals(expected.get(partition.getKey()), actual);
    }
    return batches;
  }

  @Override
  protected SerializerInstance newSerializerInstance(Serializer serializer) {
    if (serializer instanceof UnsafeRowSerializer
        && CelebornBatchBuilder.supportsColumnarType(schema)) {
      CelebornConf conf = new CelebornConf();
      return new CelebornColumnarBatchSerializer(schema, conf.columnarShuffleOffHeapEnabled(), null)
          .newInstance();
    } else {
      return serializer.newInstance();
    }
  }

  @Override
  protected ShuffleWriter<Integer, String> createShuffleWriter(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics) {
    try (MockedStatic<CustomShuffleDependencyUtils> utils =
        Mockito.mockStatic(CustomShuffleDependencyUtils.class)) {
      utils
          .when(() -> CustomShuffleDependencyUtils.getSchema(handle.dependency()))
          .thenReturn(schema);
      return SparkUtils.createColumnarHashBasedShuffleWriter(
          SparkUtils.celebornShuffleId(client, handle, context, true),
          handle,
          context,
          conf,
          client,
          metrics,
          SendBufferPool.get(1, 30, 60));
    }
  }

  private ShuffleWriter<Integer, String> createShuffleWriterWithoutSchema(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics) {
    return SparkUtils.createColumnarHashBasedShuffleWriter(
        SparkUtils.celebornShuffleId(client, handle, context, true),
        handle,
        context,
        conf,
        client,
        metrics,
        SendBufferPool.get(1, 30, 60));
  }
}
