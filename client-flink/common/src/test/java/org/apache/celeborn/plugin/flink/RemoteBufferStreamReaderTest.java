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

package org.apache.celeborn.plugin.flink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.celeborn.common.network.protocol.BufferStreamEnd;
import org.apache.celeborn.plugin.flink.client.CelebornBufferStream;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;

public class RemoteBufferStreamReaderTest {

  private static final int SHUFFLE_ID = 1;
  private static final int PARTITION_ID = 2;
  private static final int START_SUB = 0;
  private static final int END_SUB = 3;
  private static final long STREAM_ID = 99L;

  /** An opened reader whose last-partition stream end reports and the report RPC throws. */
  private RemoteBufferStreamReader openedReaderWithFailingReport(
      FlinkShuffleClientImpl client, List<Throwable> failures) throws IOException {
    CelebornBufferStream stream = mock(CelebornBufferStream.class);
    when(stream.hasRemainingPartitions()).thenReturn(false);
    when(client.readBufferedPartition(anyInt(), anyInt(), anyInt(), anyInt(), anyBoolean()))
        .thenReturn(stream);
    doThrow(new IOException("report failed"))
        .when(client)
        .readReducerPartitionEnd(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong());
    ShuffleResourceDescriptor descriptor =
        new ShuffleResourceDescriptor(SHUFFLE_ID, 0, 0, PARTITION_ID);
    RemoteBufferStreamReader reader =
        new RemoteBufferStreamReader(
            client, descriptor, START_SUB, END_SUB, null, buffer -> {}, failures::add, true);
    reader.open(0);
    return reader;
  }

  @Test
  public void onStreamEndNotifiesFailureListenerWhenOpen() throws IOException {
    FlinkShuffleClientImpl client = mock(FlinkShuffleClientImpl.class);
    List<Throwable> failures = new ArrayList<>();
    RemoteBufferStreamReader reader = openedReaderWithFailingReport(client, failures);

    reader.onStreamEnd(new BufferStreamEnd(STREAM_ID));

    assertEquals(1, failures.size());
    assertTrue(failures.get(0) instanceof IOException);
  }

  @Test
  public void onStreamEndSkipsFailureListenerAfterClose() throws IOException {
    FlinkShuffleClientImpl client = mock(FlinkShuffleClientImpl.class);
    List<Throwable> failures = new ArrayList<>();
    RemoteBufferStreamReader reader = openedReaderWithFailingReport(client, failures);
    // A close racing the stream end must swallow the report failure, not fail a dead channel.
    reader.close();

    reader.onStreamEnd(new BufferStreamEnd(STREAM_ID));

    assertTrue(failures.isEmpty());
  }
}
