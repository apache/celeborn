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

import org.apache.spark.TaskContext;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public class ColumnarHashBasedShuffleWriterSuiteJ extends CelebornShuffleWriterSuiteBase {

  private final StructType schema =
      new StructType().add("key", IntegerType$.MODULE$).add("value", StringType$.MODULE$);

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
}
