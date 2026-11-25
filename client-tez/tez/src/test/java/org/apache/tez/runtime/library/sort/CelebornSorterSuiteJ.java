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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.Test;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;

public class CelebornSorterSuiteJ {

  @Test
  public void testSpillThresholdUsesAssignedBytes() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class, Object.class);
    conf.setClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, BytesWritable.class, Object.class);
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT, 0.5f);

    OutputContext context = mock(OutputContext.class);
    when(context.getCounters()).thenReturn(new TezCounters());
    when(context.getUniqueIdentifier()).thenReturn("attempt_1");
    CelebornTezWriter writer = mock(CelebornTezWriter.class);
    when(writer.getNumPartitions()).thenReturn(1);

    int recordBytes = 256 * 1024;
    int assignedBytes = 3 * recordBytes * 2;
    CelebornSorter sorter =
        new CelebornSorter(context, conf, 1, assignedBytes, writer, new CelebornConf());
    IntWritable key = new IntWritable(1);
    // The serialized IntWritable and BytesWritable length each occupy four bytes.
    BytesWritable value = new BytesWritable(new byte[recordBytes - 8]);

    sorter.write(key, value);
    sorter.write(key, value);
    sorter.write(key, value);
    verify(writer, never()).mergeData(anyInt(), any(byte[].class), anyInt());
    verify(writer, never()).pushData(anyInt(), any(byte[].class), anyInt());

    // The fourth record triggers a flush of the three records at the 50% threshold.
    sorter.write(key, value);
    verify(writer).mergeData(eq(0), any(byte[].class), anyInt());
  }
}
