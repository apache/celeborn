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

package org.apache.tez.runtime.library.sort;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Test;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;

public class CelebornSortBasedPusherSuiteJ {

  @Test
  public void testSinglePartitionBytes() throws Exception {
    checkPartitionBytes(1);
  }

  @Test
  public void testMultiplePartitionBytes() throws Exception {
    checkPartitionBytes(3);
  }

  private void checkPartitionBytes(int numPartitions) throws Exception {
    CelebornTezWriter writer = mock(CelebornTezWriter.class);
    when(writer.getNumPartitions()).thenReturn(numPartitions);
    SerializationFactory serialization = new SerializationFactory(new Configuration());
    TezCounters counters = new TezCounters();
    TezCounter byteCounter = counters.findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter recordCounter = counters.findCounter(TaskCounter.OUTPUT_RECORDS);
    CelebornSortBasedPusher<IntWritable, BytesWritable> pusher =
        new CelebornSortBasedPusher<>(
            serialization.getSerializer(IntWritable.class),
            serialization.getSerializer(BytesWritable.class),
            4096,
            4096,
            null,
            byteCounter,
            recordCounter,
            writer,
            new CelebornConf(),
            false);

    // Each record has a four-byte IntWritable and a four-byte BytesWritable length prefix.
    pusher.insert(new IntWritable(1), new BytesWritable(new byte[2]), 0);
    pusher.insert(new IntWritable(2), new BytesWritable(new byte[5]), 0);
    if (numPartitions > 1) {
      pusher.insert(new IntWritable(3), new BytesWritable(new byte[9]), 1);
    }
    pusher.close();
    if (numPartitions > 1) {
      assertArrayEquals(new long[] {23, 17, 0}, pusher.getBytesPerPartition());
      assertArrayEquals(new int[] {2, 1, 0}, pusher.getRecordsPerPartition());
      assertEquals(40, byteCounter.getValue());
      assertEquals(3, recordCounter.getValue());
    } else {
      assertArrayEquals(new long[] {23}, pusher.getBytesPerPartition());
      assertArrayEquals(new int[] {2}, pusher.getRecordsPerPartition());
      assertEquals(23, byteCounter.getValue());
      assertEquals(2, recordCounter.getValue());
    }
  }
}
