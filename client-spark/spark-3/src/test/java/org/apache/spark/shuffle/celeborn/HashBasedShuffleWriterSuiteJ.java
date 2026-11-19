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

import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public class HashBasedShuffleWriterSuiteJ extends CelebornShuffleWriterSuiteBase {

  private SendBufferPool sendBufferPool = SendBufferPool.get(1, 30, 60);

  private void useFreshBuffers() {
    sendBufferPool = Mockito.mock(SendBufferPool.class);
    Mockito.when(sendBufferPool.acquireBuffer(numPartitions))
        .thenAnswer(ignored -> new byte[numPartitions][]);
  }

  private void checkBufferGrowth(CelebornConf conf, Serializer serializer) throws Exception {
    metrics = new TaskMetrics();
    Mockito.doReturn(metrics).when(taskContext).taskMetrics();
    check(10000, conf, serializer);
  }

  @Test(timeout = 10000)
  public void testGrowEmptyBuffer() throws Exception {
    useFreshBuffers();
    CelebornConf conf = Mockito.spy(new CelebornConf());
    // Exercise the growth fallback independently of configuration validation.
    Mockito.doReturn(0).when(conf).clientPushBufferInitialSize();
    checkBufferGrowth(conf, new KryoSerializer(sparkConf));
    checkBufferGrowth(conf, new UnsafeRowSerializer(2, null));
  }

  @Test
  public void testBufferSizeBoundaries() throws Exception {
    useFreshBuffers();
    for (int initialSize : new int[] {1, 63, 64, 128}) {
      CelebornConf conf =
          new CelebornConf()
              .set(
                  CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(), String.valueOf(initialSize))
              .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "64");
      checkBufferGrowth(conf, new KryoSerializer(sparkConf));
      checkBufferGrowth(conf, new UnsafeRowSerializer(2, null));
    }
  }

  @Override
  protected ShuffleWriter<Integer, String> createShuffleWriter(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics)
      throws IOException {
    return new HashBasedShuffleWriter<Integer, String, String>(
        SparkUtils.celebornShuffleId(client, handle, context, true),
        handle,
        context,
        conf,
        client,
        metrics,
        sendBufferPool);
  }
}
