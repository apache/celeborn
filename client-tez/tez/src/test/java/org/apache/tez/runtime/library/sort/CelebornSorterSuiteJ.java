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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;

public class CelebornSorterSuiteJ {

  private static final int NUM_PARTITIONS = 3;
  private static final long[] PARTITION_STATS = {1016, 2016, 0};

  @Test
  public void testPartitionStatsReportingEnabled() throws Exception {
    CelebornTezWriter writer = createWriter();
    when(writer.getPartitionStats()).thenReturn(PARTITION_STATS);

    CelebornSorter sorter = createSorter(ReportPartitionStats.PRECISE, writer);
    sorter.flush();

    assertArrayEquals(PARTITION_STATS, sorter.getPartitionStats());
    InOrder inOrder = inOrder(writer);
    inOrder.verify(writer).close();
    inOrder.verify(writer).getPartitionStats();
  }

  @Test
  public void testPartitionStatsReportingEnabledByDefault() throws Exception {
    CelebornTezWriter writer = createWriter();
    when(writer.getPartitionStats()).thenReturn(PARTITION_STATS);

    CelebornSorter sorter = createSorter(writer);
    sorter.flush();

    assertArrayEquals(PARTITION_STATS, sorter.getPartitionStats());
    InOrder inOrder = inOrder(writer);
    inOrder.verify(writer).close();
    inOrder.verify(writer).getPartitionStats();
  }

  @Test
  public void testPartitionStatsReportingDisabled() throws Exception {
    CelebornTezWriter writer = createWriter();

    CelebornSorter sorter = createSorter(ReportPartitionStats.NONE, writer);
    sorter.flush();

    assertNull(sorter.getPartitionStats());
    verify(writer).close();
    verify(writer, never()).getPartitionStats();
  }

  private static CelebornSorter createSorter(
      ReportPartitionStats reportPartitionStats, CelebornTezWriter writer) throws Exception {
    return createSorter(createConf(reportPartitionStats), writer);
  }

  private static CelebornSorter createSorter(CelebornTezWriter writer) throws Exception {
    return createSorter(createConf(), writer);
  }

  private static CelebornSorter createSorter(Configuration conf, CelebornTezWriter writer)
      throws Exception {
    return new CelebornSorter(
        createOutputContext(), conf, NUM_PARTITIONS, 1024, writer, new CelebornConf());
  }

  private static CelebornTezWriter createWriter() {
    CelebornTezWriter writer = mock(CelebornTezWriter.class);
    when(writer.getNumPartitions()).thenReturn(NUM_PARTITIONS);
    return writer;
  }

  private static Configuration createConf(ReportPartitionStats reportPartitionStats) {
    Configuration conf = createConf();
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS, reportPartitionStats.getType());
    return conf;
  }

  private static Configuration createConf() {
    Configuration conf = new Configuration();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    return conf;
  }

  private static OutputContext createOutputContext() {
    OutputContext outputContext = mock(OutputContext.class);
    when(outputContext.getCounters()).thenReturn(new TezCounters());
    when(outputContext.getStatisticsReporter()).thenReturn(mock(OutputStatisticsReporter.class));
    when(outputContext.getInputOutputVertexNames()).thenReturn("source -> destination");
    when(outputContext.getUniqueIdentifier()).thenReturn("attempt_1");
    when(outputContext.getDagIdentifier()).thenReturn(1);
    return outputContext;
  }
}
