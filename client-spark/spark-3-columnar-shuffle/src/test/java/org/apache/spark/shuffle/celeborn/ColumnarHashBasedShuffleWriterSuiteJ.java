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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;

import org.apache.spark.HashPartitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.columnar.CelebornBatchBuilder;
import org.apache.spark.sql.execution.columnar.CelebornColumnarBatchSerializer;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.celeborn.client.DummyShuffleClient;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

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
                "appId", "host", 0, this.userIdentifier, 0, 10, this.dependency),
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
                "appId", "host", 0, this.userIdentifier, 0, 10, this.dependency),
            taskContext,
            conf,
            client,
            metrics.shuffleWriteMetrics());
    assertTrue(((ColumnarHashBasedShuffleWriter<?, ?, ?>) writer).isColumnarShuffle());
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
          handle, context, conf, client, metrics, SendBufferPool.get(1, 30, 60));
    }
  }

  private ShuffleWriter<Integer, String> createShuffleWriterWithoutSchema(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics) {
    return SparkUtils.createColumnarHashBasedShuffleWriter(
        handle, context, conf, client, metrics, SendBufferPool.get(1, 30, 60));
  }
}
