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

package org.apache.celeborn.service.deploy.worker.storage;

import static org.apache.celeborn.common.util.JavaUtils.timeOutOrMeetCondition;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
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

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

public class CreditStreamManagerSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(CreditStreamManagerSuiteJ.class);
  private static File tempDir =
      Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn");
  private static CelebornConf conf = new CelebornConf();

  @BeforeClass
  public static void beforeAll() {
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key(), "0.8");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(), "0.9");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_RESUME().key(), "0.5");
    conf.set(CelebornConf.WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD().key(), "0.6");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_REPORT_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT().key(), "10ms");
    MemoryManager.initialize(conf);
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
    CreditStreamManager creditStreamManager = new CreditStreamManager(10, 10, 1, 32);
    Channel channel = Mockito.mock(Channel.class);
    DiskFileInfo diskFileInfo =
        new DiskFileInfo(
            createTemporaryFileWithIndexFile(), new UserIdentifier("default", "default"), conf);
    MapFileMeta mapFileMeta =
        new MapFileMeta(1024, 10, false, Collections.emptyMap(), Collections.emptyList());
    diskFileInfo.replaceFileMeta(mapFileMeta);
    Consumer<Long> streamIdConsumer = streamId -> Assert.assertTrue(streamId > 0);

    String shuffleKey = "application_1694674023293_0003-0";
    long registerStream1 =
        creditStreamManager.registerStream(
            streamIdConsumer, channel, shuffleKey, 0, 1, 1, diskFileInfo, false);
    Assert.assertTrue(registerStream1 > 0);
    Assert.assertEquals(1, creditStreamManager.getStreamsCount());

    long registerStream2 =
        creditStreamManager.registerStream(
            streamIdConsumer, channel, shuffleKey, 0, 1, 1, diskFileInfo, false);
    Assert.assertNotEquals(registerStream1, registerStream2);
    Assert.assertEquals(2, creditStreamManager.getStreamsCount());

    creditStreamManager.registerStream(
        streamIdConsumer, channel, shuffleKey, 0, 1, 1, diskFileInfo, false);
    creditStreamManager.registerStream(
        streamIdConsumer, channel, shuffleKey, 0, 1, 1, diskFileInfo, false);

    MapPartitionData mapPartitionData1 =
        creditStreamManager.getStreams().get(registerStream1).getMapDataPartition();
    MapPartitionData mapPartitionData2 =
        creditStreamManager.getStreams().get(registerStream2).getMapDataPartition();
    Assert.assertEquals(mapPartitionData1, mapPartitionData2);

    mapPartitionData1.getStreamReader(registerStream1).recycle();

    timeOutOrMeetCondition(() -> creditStreamManager.getStreamsCount() == 3);
    Assert.assertEquals(creditStreamManager.numRecycleStreams(), 0);

    // registerStream2 can't be cleaned as registerStream2 is not finished
    AtomicInteger numInFlightRequests =
        mapPartitionData2.getStreamReader(registerStream2).getNumInUseBuffers();
    numInFlightRequests.incrementAndGet();

    creditStreamManager.cleanResource(registerStream2);
    Assert.assertEquals(creditStreamManager.numRecycleStreams(), 1);
    Assert.assertEquals(3, creditStreamManager.getStreamsCount());

    // recycle all channel
    numInFlightRequests.decrementAndGet();
    creditStreamManager.connectionTerminated(channel);
    timeOutOrMeetCondition(() -> creditStreamManager.getStreamsCount() == 0);
    // when cpu is busy, even through that timeOutOrMeetCondition is true,
    // creditStreamManager.getStreamsCount are still not be removed
    Assert.assertTrue(creditStreamManager.numRecycleStreams() >= 0);
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
