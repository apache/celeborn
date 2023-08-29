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

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

public class PartitionFilesSorterSuiteJ {

  private static Logger logger = LoggerFactory.getLogger(PartitionFilesSorterSuiteJ.class);

  private Random random = new Random();
  private File shuffleFile;
  private FileInfo fileInfo;
  private String originFileName;
  private long originFileLen;
  private FileWriter fileWriter;
  private UserIdentifier userIdentifier = new UserIdentifier("mock-tenantId", "mock-name");

  private static final int MAX_MAP_ID = 50;

  public long[] prepare(int mapCount) throws IOException {
    long[] partitionSize = new long[MAX_MAP_ID];
    byte[] batchHeader = new byte[16];
    shuffleFile = File.createTempFile("Celeborn", "sort-suite");

    originFileName = shuffleFile.getAbsolutePath();
    fileInfo = new FileInfo(shuffleFile, userIdentifier);
    FileOutputStream fileOutputStream = new FileOutputStream(shuffleFile);
    FileChannel channel = fileOutputStream.getChannel();
    Map<Integer, Integer> batchIds = new HashMap<>();

    for (int i = 0; i < mapCount; i++) {
      int mapId = random.nextInt(MAX_MAP_ID);
      int currentAttemptId = 0;
      int batchId =
          batchIds.compute(
              mapId,
              (k, v) -> {
                if (v == null) {
                  v = 0;
                } else {
                  v++;
                }
                return v;
              });
      // [63.9k, 192k + 63.9k]
      int dataSize = random.nextInt(192 * 1024) + 65525;
      byte[] mockedData = new byte[dataSize];
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET, mapId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 4, currentAttemptId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12, dataSize);
      ByteBuffer buf1 = ByteBuffer.wrap(batchHeader);
      while (buf1.hasRemaining()) {
        channel.write(buf1);
      }
      random.nextBytes(mockedData);
      ByteBuffer buf2 = ByteBuffer.wrap(mockedData);
      while (buf2.hasRemaining()) {
        channel.write(buf2);
      }
      partitionSize[mapId] = partitionSize[mapId] + batchHeader.length + mockedData.length;
    }
    originFileLen = channel.size();
    fileInfo.getChunkOffsets().add(originFileLen);
    fileInfo.updateBytesFlushed(originFileLen);
    logger.info(shuffleFile.getAbsolutePath() + " filelen: " + Utils.bytesToString(originFileLen));

    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key(), "0.8");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(), "0.9");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_RESUME().key(), "0.5");
    conf.set(CelebornConf.PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD().key(), "0.6");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_SHUFFLE_STORAGE().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_REPORT_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT().key(), "10ms");
    MemoryManager.initialize(conf);
    fileWriter = Mockito.mock(FileWriter.class);
    when(fileWriter.getFile()).thenAnswer(i -> shuffleFile);
    when(fileWriter.getFileInfo()).thenAnswer(i -> fileInfo);
    return partitionSize;
  }

  public void clean() throws IOException {
    // origin file
    JavaUtils.deleteRecursively(shuffleFile);
    // sorted file
    JavaUtils.deleteRecursively(new File(shuffleFile.getPath() + ".sorted"));
    // index file
    JavaUtils.deleteRecursively(new File(shuffleFile.getPath() + ".index"));
  }

  private void check(int mapCount, int startMapIndex, int endMapIndex) throws IOException {
    try {
      long[] partitionSize = prepare(mapCount);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.SHUFFLE_CHUNK_SIZE().key(), "8m");
      PartitionFilesSorter partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf));
      FileInfo info =
          partitionFilesSorter.getSortedFileInfo(
              "application-1",
              originFileName,
              fileWriter.getFileInfo(),
              startMapIndex,
              endMapIndex);
      long totalSizeToFetch = 0;
      for (int i = startMapIndex; i < endMapIndex; i++) {
        totalSizeToFetch += partitionSize[i];
      }
      long numChunks = totalSizeToFetch / conf.shuffleChunkSize() + 1;
      Assert.assertTrue(0 < info.numChunks() && info.numChunks() <= numChunks);
      long actualTotalChunkSize = info.getLastChunkOffset() - info.getChunkOffsets().get(0);
      Assert.assertTrue(totalSizeToFetch == actualTotalChunkSize);
    } finally {
      clean();
    }
  }

  @Test
  public void testSmallFile() throws IOException {
    int startMapIndex = random.nextInt(5);
    int endMapIndex = startMapIndex + random.nextInt(5) + 5;
    check(1000, startMapIndex, endMapIndex);
  }

  @Test
  public void testLargeFile() throws IOException {
    int startMapIndex = random.nextInt(5);
    int endMapIndex = startMapIndex + random.nextInt(5) + 5;
    check(15000, startMapIndex, endMapIndex);
  }

  @Test
  public void testLevelDB() {
    if (Utils.isMacOnAppleSilicon()) {
      logger.info("Skip on Apple Silicon platform");
      return;
    }
    File recoverPath = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "recover_path");
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_GRACEFUL_SHUTDOWN_ENABLED().key(), "true");
    conf.set(CelebornConf.WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH(), recoverPath.getPath());
    PartitionFilesSorter partitionFilesSorter =
        new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf));
    partitionFilesSorter.initSortedShuffleFiles("application-1-1");
    partitionFilesSorter.updateSortedShuffleFiles("application-1-1", "0-0-1", 0);
    partitionFilesSorter.updateSortedShuffleFiles("application-1-1", "0-0-2", 0);
    partitionFilesSorter.updateSortedShuffleFiles("application-1-1", "0-0-3", 0);
    partitionFilesSorter.initSortedShuffleFiles("application-2-1");
    partitionFilesSorter.updateSortedShuffleFiles("application-2-1", "0-0-1", 0);
    partitionFilesSorter.updateSortedShuffleFiles("application-2-1", "0-0-2", 0);
    partitionFilesSorter.initSortedShuffleFiles("application-3-1");
    partitionFilesSorter.updateSortedShuffleFiles("application-3-1", "0-0-1", 0);
    partitionFilesSorter.deleteSortedShuffleFiles("application-2-1");
    partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
    PartitionFilesSorter partitionFilesSorter2 =
        new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf));
    Assert.assertEquals(
        partitionFilesSorter2.getSortedShuffleFiles("application-1-1").toString(),
        "[0-0-3, 0-0-2, 0-0-1]");
    Assert.assertEquals(partitionFilesSorter2.getSortedShuffleFiles("application-2-1"), null);
    Assert.assertEquals(
        partitionFilesSorter2.getSortedShuffleFiles("application-3-1").toString(), "[0-0-1]");
    partitionFilesSorter2.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
    recoverPath.delete();
  }
}
