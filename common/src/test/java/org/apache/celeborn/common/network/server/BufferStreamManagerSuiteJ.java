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

package org.apache.celeborn.common.network.server;

import static org.apache.celeborn.common.network.TestUtils.timeOutOrMeetCondition;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.server.memory.MemoryManager;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;

public class BufferStreamManagerSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(BufferStreamManagerSuiteJ.class);
  private static File tempDir =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn");

  @BeforeClass
  public static void beforeAll() {
    MemoryManager.initialize(0.8, 0.9, 0.5, 0.6, 0.1, 0.1, 10, 10);
  }

  private File createTemporaryFileWithIndexFile() throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    File indexFile = new File(tempDir, filename + ".index");
    temporaryFile.createNewFile();
    indexFile.createNewFile();
    return temporaryFile;
  }

  @Test
  public void testStreamRegisterAndCleanup() throws Exception {
    BufferStreamManager bufferStreamManager = new BufferStreamManager(10, 10, 1);
    Channel channel = Mockito.mock(Channel.class);
    FileInfo fileInfo =
        new FileInfo(createTemporaryFileWithIndexFile(), new UserIdentifier("default", "default"));
    fileInfo.setNumSubpartitions(10);
    Consumer<Long> streamIdConsumer = streamId -> Assert.assertTrue(streamId > 0);

    long registerStream1 =
        bufferStreamManager.registerStream(streamIdConsumer, channel, 0, 1, 1, fileInfo);
    Assert.assertTrue(registerStream1 > 0);
    Assert.assertEquals(1, bufferStreamManager.numStreamStates());

    long registerStream2 =
        bufferStreamManager.registerStream(streamIdConsumer, channel, 0, 1, 1, fileInfo);
    Assert.assertNotEquals(registerStream1, registerStream2);
    Assert.assertEquals(2, bufferStreamManager.numStreamStates());

    bufferStreamManager.registerStream(streamIdConsumer, channel, 0, 1, 1, fileInfo);
    bufferStreamManager.registerStream(streamIdConsumer, channel, 0, 1, 1, fileInfo);

    BufferStreamManager.MapDataPartition mapDataPartition1 =
        bufferStreamManager.getServingStreams().get(registerStream1);
    BufferStreamManager.MapDataPartition mapDataPartition2 =
        bufferStreamManager.getServingStreams().get(registerStream2);
    Assert.assertEquals(mapDataPartition1, mapDataPartition2);

    mapDataPartition1.getStreamReader(registerStream1).recycle();

    timeOutOrMeetCondition(() -> bufferStreamManager.numRecycleStreams() == 0);
    Assert.assertEquals(bufferStreamManager.numRecycleStreams(), 0);
    Assert.assertEquals(3, bufferStreamManager.numStreamStates());

    // registerStream2 can't be cleaned as registerStream2 is not finished
    AtomicInteger numInFlightRequests =
        mapDataPartition2.getStreamReader(registerStream2).getNumInFlightRequests();
    numInFlightRequests.incrementAndGet();

    bufferStreamManager.cleanResource(registerStream2);
    Assert.assertEquals(bufferStreamManager.numRecycleStreams(), 1);
    Assert.assertEquals(3, bufferStreamManager.numStreamStates());

    // recycle all channel
    numInFlightRequests.decrementAndGet();
    bufferStreamManager.connectionTerminated(channel);
    timeOutOrMeetCondition(() -> bufferStreamManager.numRecycleStreams() == 0);
    Assert.assertEquals(bufferStreamManager.numStreamStates(), 0);
  }

  @AfterClass
  public static void afterAll() {
    if (tempDir != null) {
      try {
        JavaUtils.deleteRecursively(tempDir);
        tempDir = null;
      } catch (IOException e) {
        LOG.error("Failed to delete temp dir.", e);
      }
    }
  }
}
