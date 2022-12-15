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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.client.ShuffleClientImpl;
import org.apache.celeborn.common.protocol.PartitionLocation;

public class RemoteShuffleOutputGateSuiteJ {
  private RemoteShuffleOutputGate remoteShuffleOutputGate = mock(RemoteShuffleOutputGate.class);
  private ShuffleClientImpl shuffleClient = mock(ShuffleClientImpl.class);

  @Before
  public void setup() {
    remoteShuffleOutputGate.shuffleWriteClient = shuffleClient;
  }

  @Test
  public void TestSimpleWriteData() throws IOException, InterruptedException {

    PartitionLocation partitionLocation =
        new PartitionLocation(1, 0, "localhost", 123, 245, 789, 238, PartitionLocation.Mode.MASTER);
    when(shuffleClient.registerMapPartitionTask(any(), anyInt(), anyInt(), anyInt(), anyInt()))
        .thenAnswer(t -> partitionLocation);
    doNothing()
        .when(remoteShuffleOutputGate.shuffleWriteClient)
        .pushDataHandShake(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), any());

    remoteShuffleOutputGate.handshake();

    when(remoteShuffleOutputGate.shuffleWriteClient.regionStart(
            any(), anyInt(), anyInt(), anyInt(), any(), anyInt(), anyBoolean()))
        .thenAnswer(t -> Optional.empty());
    remoteShuffleOutputGate.regionStart(false);

    remoteShuffleOutputGate.write(createBuffer(), 0);

    doNothing()
        .when(remoteShuffleOutputGate.shuffleWriteClient)
        .regionFinish(any(), anyInt(), anyInt(), anyInt(), any());
    remoteShuffleOutputGate.regionFinish();

    doNothing()
        .when(remoteShuffleOutputGate.shuffleWriteClient)
        .mapperEnd(any(), anyInt(), anyInt(), anyInt(), anyInt());
    remoteShuffleOutputGate.finish();

    doNothing().when(remoteShuffleOutputGate.shuffleWriteClient).shutDown();
    remoteShuffleOutputGate.close();
  }

  private Buffer createBuffer() throws IOException, InterruptedException {
    int segmentSize = 32 * 1024;
    NetworkBufferPool networkBufferPool =
        new NetworkBufferPool(256, 32 * 1024, Duration.ofMillis(30000L));
    BufferPool bufferPool =
        networkBufferPool.createBufferPool(
            8 * 1024 * 1024 / segmentSize, 8 * 1024 * 1024 / segmentSize);

    MemorySegment memorySegment = bufferPool.requestMemorySegmentBlocking();

    memorySegment.put(0, new byte[] {1, 2, 3});

    Buffer buffer = new NetworkBuffer(memorySegment, bufferPool);
    return buffer;
  }
}
