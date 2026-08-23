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

package org.apache.celeborn.client;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import org.mockito.InOrder;

import org.apache.celeborn.client.write.DataPusher;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.write.PushState;

public class CelebornTezWriterSuiteJ {

  private static final int SHUFFLE_ID = 1;
  private static final int MAP_ID = 2;
  private static final int ATTEMPT_ID = 3;
  private static final int NUM_MAPPERS = 4;
  private static final int NUM_PARTITIONS = 3;

  @Test
  public void testSnapshotPartitionStats() {
    LongAdder[] mapStatusLengths = new LongAdder[3];
    for (int i = 0; i < mapStatusLengths.length; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    mapStatusLengths[0].add(1016);
    mapStatusLengths[1].add(2016);
    mapStatusLengths[0].add(3016);

    long[] partitionStats = CelebornTezWriter.snapshotPartitionStats(mapStatusLengths);
    assertArrayEquals(new long[] {4032, 2016, 0}, partitionStats);

    mapStatusLengths[2].add(4016);
    assertArrayEquals(new long[] {4032, 2016, 0}, partitionStats);
    assertArrayEquals(
        new long[] {4032, 2016, 4016}, CelebornTezWriter.snapshotPartitionStats(mapStatusLengths));
  }

  @Test
  public void testMergeDataUsesBytesWrittenByShuffleClient() throws Exception {
    CelebornConf conf = createConf();
    ShuffleClient shuffleClient = createShuffleClient(conf);
    when(shuffleClient.mergeData(
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(byte[].class),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              int partitionId = invocation.getArgument(3);
              int length = invocation.getArgument(6);
              return partitionId * 1000 + length;
            });

    CelebornTezWriter writer = createWriter(conf, shuffleClient);
    writer.mergeData(0, new byte[10], 10);
    writer.mergeData(1, new byte[20], 20);
    writer.mergeData(0, new byte[30], 30);

    assertArrayEquals(new long[] {40, 1020, 0}, writer.getPartitionStats());
    writer.close();
  }

  @Test
  public void testAsyncPushIsIncludedInPartitionStatsAfterClose() throws Exception {
    CelebornConf conf = createConf();
    ShuffleClient shuffleClient = createShuffleClient(conf);
    when(shuffleClient.pushData(
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(byte[].class),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              int partitionId = invocation.getArgument(3);
              int length = invocation.getArgument(6);
              return partitionId * 1000 + length;
            });

    CelebornTezWriter writer = createWriter(conf, shuffleClient);
    writer.pushData(2, new byte[50], 50);
    writer.close();

    assertArrayEquals(new long[] {0, 0, 2050}, writer.getPartitionStats());
    InOrder inOrder = inOrder(shuffleClient);
    inOrder
        .verify(shuffleClient)
        .pushData(
            eq(SHUFFLE_ID),
            eq(MAP_ID),
            eq(ATTEMPT_ID),
            eq(2),
            any(byte[].class),
            eq(0),
            eq(50),
            eq(NUM_MAPPERS),
            eq(NUM_PARTITIONS));
    inOrder.verify(shuffleClient).pushMergedData(SHUFFLE_ID, MAP_ID, ATTEMPT_ID);
    inOrder
        .verify(shuffleClient)
        .mapperEnd(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, NUM_MAPPERS, NUM_PARTITIONS);
  }

  @Test
  public void testCloseWaitsForDataPusherBeforeFinalizingMapper() throws Exception {
    ShuffleClient shuffleClient = mock(ShuffleClient.class);
    DataPusher dataPusher = mock(DataPusher.class);
    CelebornTezWriter writer =
        new CelebornTezWriter(
            SHUFFLE_ID, MAP_ID, ATTEMPT_ID, NUM_MAPPERS, NUM_PARTITIONS, shuffleClient, dataPusher);

    writer.close();

    InOrder inOrder = inOrder(dataPusher, shuffleClient);
    inOrder.verify(dataPusher).waitOnTermination();
    inOrder.verify(shuffleClient).pushMergedData(SHUFFLE_ID, MAP_ID, ATTEMPT_ID);
    inOrder
        .verify(shuffleClient)
        .mapperEnd(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, NUM_MAPPERS, NUM_PARTITIONS);
  }

  private static CelebornConf createConf() {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_PUSH_QUEUE_CAPACITY().key(), "2");
    conf.set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "1024");
    return conf;
  }

  private static ShuffleClient createShuffleClient(CelebornConf conf) throws Exception {
    ShuffleClient shuffleClient = mock(ShuffleClient.class);
    when(shuffleClient.getPushState(anyString())).thenReturn(new PushState(conf));
    when(shuffleClient.getPartitionLocation(anyInt(), anyInt(), anyInt())).thenReturn(null);
    doAnswer(invocation -> null)
        .when(shuffleClient)
        .computeBatchCRC(
            anyInt(), anyInt(), anyInt(), anyInt(), any(byte[].class), anyInt(), anyInt());
    return shuffleClient;
  }

  private static CelebornTezWriter createWriter(CelebornConf conf, ShuffleClient shuffleClient) {
    return new CelebornTezWriter(
        SHUFFLE_ID, MAP_ID, ATTEMPT_ID, 4L, NUM_MAPPERS, NUM_PARTITIONS, conf, shuffleClient);
  }
}
