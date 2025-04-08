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

package org.apache.celeborn.service.deploy.worker.storage.local;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import scala.Function0;
import scala.collection.mutable.ListBuffer;

import io.netty.buffer.Unpooled;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.*;

public class DiskMapPartitionDataWriterSuiteJ {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMapPartitionDataWriterSuiteJ.class);

  private static final CelebornConf CONF = new CelebornConf();
  public static final Long SPLIT_THRESHOLD = 256 * 1024 * 1024L;
  public static final PartitionSplitMode splitMode = PartitionSplitMode.HARD;

  private static File tempDir = null;
  private static LocalFlusher localFlusher = null;
  private static WorkerSource source = null;

  private final UserIdentifier userIdentifier = new UserIdentifier("mock-tenantId", "mock-name");
  private static StoragePolicy storagePolicy;

  @BeforeClass
  public static void beforeAll() {
    tempDir =
        Utils.createTempDir(
            System.getProperty("java.io.tmpdir"), "celeborn" + System.currentTimeMillis());

    source = Mockito.mock(WorkerSource.class);
    storagePolicy = Mockito.mock(StoragePolicy.class);
    Mockito.doAnswer(
            invocationOnMock -> {
              Function0<?> function = (Function0<?>) invocationOnMock.getArguments()[2];
              return function.apply();
            })
        .when(source)
        .sample(Mockito.anyString(), Mockito.anyString(), Mockito.any(Function0.class));

    ListBuffer<File> dirs = new ListBuffer<>();
    dirs.$plus$eq(tempDir);
    localFlusher =
        new LocalFlusher(
            source,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            1,
            NettyUtils.getByteBufAllocator(new TransportConf("test", CONF), null, true),
            256,
            "disk1",
            StorageInfo.Type.HDD,
            null);

    CelebornConf conf = new CelebornConf();
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

  @Test
  public void testMultiThreadWrite() throws IOException {
    PartitionLocation partitionLocation = Mockito.mock(PartitionLocation.class);
    PartitionDataWriterContext context =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            partitionLocation,
            "app1",
            1,
            userIdentifier,
            PartitionType.MAP,
            false,
            false);
    PartitionDataWriter fileWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, false, CONF, storagePolicy, context),
            CONF,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context,
            PartitionType.MAP);
    fileWriter.handleEvents(
        PbPushDataHandShake.newBuilder().setNumPartitions(2).setBufferSize(32).build());
    fileWriter.handleEvents(
        PbRegionStart.newBuilder().setCurrentRegionIndex(0).setIsBroadcast(false).build());
    byte[] partData0 = generateData(0);
    byte[] partData1 = generateData(1);
    AtomicLong length = new AtomicLong(0);
    try {
      fileWriter.write(Unpooled.wrappedBuffer(partData0));
      length.addAndGet(partData0.length);
      fileWriter.write(Unpooled.wrappedBuffer(partData1));
      length.addAndGet(partData1.length);

      fileWriter.handleEvents(PbRegionFinish.newBuilder().build());
    } catch (IOException e) {
      LOG.error("Failed to write buffer.", e);
    }
    long bytesWritten = fileWriter.close();

    assertEquals(length.get(), bytesWritten);
    assertEquals(new File(fileWriter.getFilePath()).length(), bytesWritten);
  }

  private byte[] generateData(int partitionId) {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    byte[] hello = "hello, world".getBytes(StandardCharsets.UTF_8);
    int headerLength = 16;
    int tempLen = rand.nextInt(256 * 1024) + 128 * 1024 - headerLength;
    int len = (int) (Math.ceil(1.0 * tempLen / hello.length) * hello.length) + headerLength;

    byte[] data = new byte[len];
    ByteBuffer byteBuffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    byteBuffer.putInt(0, partitionId);
    byteBuffer.putInt(4, 0);
    byteBuffer.putInt(8, rand.nextInt());
    byteBuffer.putInt(12, len);

    for (int i = headerLength; i < len; i += hello.length) {
      System.arraycopy(hello, 0, data, i, hello.length);
    }
    return data;
  }
}
