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

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ShuffleBlockInfoUtils.ShuffleBlockInfo;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.PartitionFilesSorter;

public class DiskPartitionFilesSorterSuiteJ {

  private static final Logger logger =
      LoggerFactory.getLogger(DiskPartitionFilesSorterSuiteJ.class);

  private final Random random = new Random();
  private File shuffleFile;
  private DiskFileInfo fileInfo;
  private String originFileName;
  private PartitionDataWriter partitionDataWriter;
  private final UserIdentifier userIdentifier = new UserIdentifier("mock-tenantId", "mock-name");

  private static final int MAX_MAP_ID = 50;

  public long[] prepare(int mapCount) throws IOException {
    long[] partitionSize = new long[MAX_MAP_ID];
    byte[] batchHeader = new byte[16];
    shuffleFile = File.createTempFile("Celeborn", "sort-suite");

    originFileName = shuffleFile.getAbsolutePath();
    fileInfo = new DiskFileInfo(shuffleFile, userIdentifier, new CelebornConf());
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
    long originFileLen = channel.size();
    fileInfo.getReduceFileMeta().getChunkOffsets().add(originFileLen);
    fileInfo.updateBytesFlushed(originFileLen);
    logger.info(shuffleFile.getAbsolutePath() + " filelen: " + Utils.bytesToString(originFileLen));

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
    partitionDataWriter = Mockito.mock(PartitionDataWriter.class);
    when(partitionDataWriter.getDiskFileInfo()).thenAnswer(i -> fileInfo);
    when(partitionDataWriter.getDiskFileInfo()).thenAnswer(i -> fileInfo);
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

  private DiskFileInfo copyCurrentShuffleFile(File copiedShuffleFile) throws IOException {
    Files.copy(
        shuffleFile.toPath(), copiedShuffleFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    DiskFileInfo copiedFileInfo =
        new DiskFileInfo(copiedShuffleFile, userIdentifier, new CelebornConf());
    copiedFileInfo.getReduceFileMeta().getChunkOffsets().add(copiedShuffleFile.length());
    copiedFileInfo.updateBytesFlushed(copiedShuffleFile.length());
    return copiedFileInfo;
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
              partitionDataWriter.getDiskFileInfo(),
              startMapIndex,
              endMapIndex);
      long totalSizeToFetch = 0;
      for (int i = startMapIndex; i < endMapIndex; i++) {
        totalSizeToFetch += partitionSize[i];
      }
      long numChunks = totalSizeToFetch / conf.shuffleChunkSize() + 1;
      Assert.assertTrue(
          0 < ((ReduceFileMeta) info.getFileMeta()).getNumChunks()
              && ((ReduceFileMeta) info.getFileMeta()).getNumChunks() <= numChunks);
      long actualTotalChunkSize =
          ((ReduceFileMeta) info.getFileMeta()).getLastChunkOffset()
              - ((ReduceFileMeta) info.getFileMeta()).getChunkOffsets().get(0);
      Assert.assertEquals(totalSizeToFetch, actualTotalChunkSize);
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
  public void testAsyncSortedFileWaitersShareOneInFlightSort() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    CountDownLatch allowWriteIndex = new CountDownLatch(1);
    try {
      prepare(100);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.SHUFFLE_CHUNK_SIZE().key(), "8m");
      CountDownLatch writeIndexStarted = new CountDownLatch(1);
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf)) {
            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              writeIndexStarted.countDown();
              try {
                Assert.assertTrue(allowWriteIndex.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to write the index.", e);
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }
          };

      CompletableFuture<FileInfo> firstWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-1", originFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);
      CompletableFuture<FileInfo> secondWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-1", originFileName, partitionDataWriter.getDiskFileInfo(), 10, 15);

      Assert.assertTrue(writeIndexStarted.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(firstWaiter.isDone());
      Assert.assertFalse(secondWaiter.isDone());
      Assert.assertEquals(2, partitionFilesSorter.getSortedFileWaiterCount());
      Assert.assertEquals(1, partitionFilesSorter.getSortingCount());

      allowWriteIndex.countDown();
      Assert.assertNotNull(firstWaiter.get(10, TimeUnit.SECONDS));
      Assert.assertNotNull(secondWaiter.get(10, TimeUnit.SECONDS));
      Assert.assertEquals(0, partitionFilesSorter.getSortedFileWaiterCount());
    } finally {
      allowWriteIndex.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      clean();
    }
  }

  @Test
  public void testAsyncSortedFileRequestFailsAfterSorterClose() throws Exception {
    try {
      prepare(100);
      CelebornConf conf = new CelebornConf();
      PartitionFilesSorter partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf));
      partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());

      CompletableFuture<FileInfo> sortedFileInfo =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-closed", originFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);

      Assert.assertTrue(sortedFileInfo.isCompletedExceptionally());
      try {
        sortedFileInfo.get(1, TimeUnit.SECONDS);
        Assert.fail("Expected closed partition sorter to reject the request.");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
        Assert.assertTrue(e.getCause().getMessage().contains("closed"));
      }
    } finally {
      clean();
    }
  }

  @Test
  public void testAsyncSortedFileRequestCanRetryAfterWaitTimeout() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    CountDownLatch allowWriteIndex = new CountDownLatch(1);
    try {
      prepare(100);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.WORKER_PARTITION_SORTER_SORT_TIMEOUT().key(), "500ms");
      CountDownLatch writeIndexStarted = new CountDownLatch(1);
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf)) {
            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              writeIndexStarted.countDown();
              try {
                Assert.assertTrue(allowWriteIndex.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to write the index.", e);
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }
          };

      CompletableFuture<FileInfo> timedOutWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-timeout", originFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);
      PartitionFilesSorter activeSorter = partitionFilesSorter;
      CompletableFuture<FileInfo> retryWaiter =
          timedOutWaiter
              .handle(
                  (ignored, error) -> {
                    if (error == null) {
                      throw new IllegalStateException("Expected the first sort wait to time out.");
                    }
                    return activeSorter.getSortedFileInfoAsync(
                        "application-timeout",
                        originFileName,
                        partitionDataWriter.getDiskFileInfo(),
                        5,
                        10);
                  })
              .thenCompose(retry -> retry);
      Assert.assertTrue(writeIndexStarted.await(10, TimeUnit.SECONDS));
      try {
        timedOutWaiter.get(5, TimeUnit.SECONDS);
        Assert.fail("Expected asynchronous sorted-file request to time out.");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
        Assert.assertTrue(e.getCause().getMessage().contains("timeout"));
      }

      allowWriteIndex.countDown();
      Assert.assertNotNull(retryWaiter.get(5, TimeUnit.SECONDS));
    } finally {
      allowWriteIndex.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      clean();
    }
  }

  @Test
  public void testAsyncSortedFileWaitersDoNotCollideAcrossShuffleKeys() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    File secondShuffleFile = null;
    CountDownLatch allowFirstWrite = new CountDownLatch(1);
    CountDownLatch allowSecondWrite = new CountDownLatch(1);
    try {
      prepare(100);
      secondShuffleFile = File.createTempFile("Celeborn", "future-key-collision-suite");
      DiskFileInfo secondFileInfo = copyCurrentShuffleFile(secondShuffleFile);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.WORKER_PARTITION_SORTER_THREADS().key(), "2");
      String firstShuffleKey = "application-1";
      String firstFileName = "2-3";
      String secondShuffleKey = "application-1-2";
      String secondFileName = "3";
      CountDownLatch bothWritesStarted = new CountDownLatch(2);
      String firstIndexFilePath =
          Utils.getIndexFilePath(partitionDataWriter.getDiskFileInfo().getFilePath());
      String secondIndexFilePath = Utils.getIndexFilePath(secondShuffleFile.getAbsolutePath());
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf)) {
            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              bothWritesStarted.countDown();
              try {
                if (indexFilePath.equals(firstIndexFilePath)) {
                  Assert.assertTrue(allowFirstWrite.await(10, TimeUnit.SECONDS));
                } else if (indexFilePath.equals(secondIndexFilePath)) {
                  Assert.assertTrue(allowSecondWrite.await(10, TimeUnit.SECONDS));
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to write the index.", e);
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }
          };

      CompletableFuture<FileInfo> firstWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              firstShuffleKey, firstFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);
      CompletableFuture<FileInfo> secondWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              secondShuffleKey, secondFileName, secondFileInfo, 5, 10);

      Assert.assertTrue(bothWritesStarted.await(10, TimeUnit.SECONDS));
      allowFirstWrite.countDown();
      Assert.assertNotNull(firstWaiter.get(10, TimeUnit.SECONDS));
      Assert.assertFalse(secondWaiter.isDone());

      allowSecondWrite.countDown();
      Assert.assertNotNull(secondWaiter.get(10, TimeUnit.SECONDS));
    } finally {
      allowFirstWrite.countDown();
      allowSecondWrite.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      if (secondShuffleFile != null) {
        JavaUtils.deleteRecursively(secondShuffleFile);
        JavaUtils.deleteRecursively(new File(secondShuffleFile.getPath() + ".sorted"));
        JavaUtils.deleteRecursively(new File(secondShuffleFile.getPath() + ".index"));
      }
      clean();
    }
  }

  @Test
  public void testCleanupOnlyFailsWaitersForExactShuffleKey() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    File liveShuffleFile = null;
    CountDownLatch allowWriteIndex = new CountDownLatch(1);
    try {
      prepare(100);
      liveShuffleFile = File.createTempFile("Celeborn", "live-sort-suite");
      DiskFileInfo liveFileInfo = copyCurrentShuffleFile(liveShuffleFile);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.WORKER_PARTITION_SORTER_THREADS().key(), "2");
      CountDownLatch writeIndexStarted = new CountDownLatch(2);
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf)) {
            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              writeIndexStarted.countDown();
              try {
                Assert.assertTrue(allowWriteIndex.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to write the index.", e);
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }
          };

      CompletableFuture<FileInfo> expiredShuffleWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-1", originFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);
      CompletableFuture<FileInfo> liveShuffleWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-1-2", liveShuffleFile.getAbsolutePath(), liveFileInfo, 5, 10);

      Assert.assertTrue(writeIndexStarted.await(10, TimeUnit.SECONDS));
      HashSet<String> expiredShuffleKeys = new HashSet<>();
      expiredShuffleKeys.add("application-1");
      partitionFilesSorter.cleanup(expiredShuffleKeys);

      Assert.assertTrue(expiredShuffleWaiter.isCompletedExceptionally());
      Assert.assertFalse(liveShuffleWaiter.isDone());

      allowWriteIndex.countDown();
      Assert.assertNotNull(liveShuffleWaiter.get(10, TimeUnit.SECONDS));
    } finally {
      allowWriteIndex.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      if (liveShuffleFile != null) {
        JavaUtils.deleteRecursively(liveShuffleFile);
        JavaUtils.deleteRecursively(new File(liveShuffleFile.getPath() + ".sorted"));
        JavaUtils.deleteRecursively(new File(liveShuffleFile.getPath() + ".index"));
      }
      clean();
    }
  }

  @Test
  public void testWaiterResolutionDoesNotBlockUnrelatedWaiter() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    File secondShuffleFile = null;
    CountDownLatch allowFirstResolve = new CountDownLatch(1);
    try {
      prepare(100);
      secondShuffleFile = File.createTempFile("Celeborn", "second-sort-suite");
      DiskFileInfo secondFileInfo = copyCurrentShuffleFile(secondShuffleFile);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.WORKER_PARTITION_SORTER_THREADS().key(), "1");
      String shuffleKey = "application-resolve";
      String firstFileId = shuffleKey + "-" + originFileName;
      String secondIndexFilePath = Utils.getIndexFilePath(secondShuffleFile.getAbsolutePath());
      CountDownLatch firstResolveStarted = new CountDownLatch(1);
      CountDownLatch secondWriteIndexStarted = new CountDownLatch(1);
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf)) {
            @Override
            public DiskFileInfo resolve(
                String shuffleKey,
                String fileId,
                UserIdentifier userIdentifier,
                String sortedFilePath,
                String indexFilePath,
                int startMapIndex,
                int endMapIndex)
                throws IOException {
              if (fileId.equals(firstFileId)) {
                firstResolveStarted.countDown();
                try {
                  Assert.assertTrue(allowFirstResolve.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException("Interrupted while waiting to resolve the index.", e);
                }
              }
              return super.resolve(
                  shuffleKey,
                  fileId,
                  userIdentifier,
                  sortedFilePath,
                  indexFilePath,
                  startMapIndex,
                  endMapIndex);
            }

            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              if (indexFilePath.equals(secondIndexFilePath)) {
                secondWriteIndexStarted.countDown();
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }
          };

      CompletableFuture<FileInfo> firstWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              shuffleKey, originFileName, partitionDataWriter.getDiskFileInfo(), 5, 10);
      CompletableFuture<FileInfo> secondWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              shuffleKey, secondShuffleFile.getAbsolutePath(), secondFileInfo, 5, 10);

      Assert.assertTrue(firstResolveStarted.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(secondWriteIndexStarted.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(firstWaiter.isDone());
      Assert.assertNotNull(secondWaiter.get(10, TimeUnit.SECONDS));

      allowFirstResolve.countDown();
      Assert.assertNotNull(firstWaiter.get(10, TimeUnit.SECONDS));
    } finally {
      allowFirstResolve.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      if (secondShuffleFile != null) {
        JavaUtils.deleteRecursively(secondShuffleFile);
        JavaUtils.deleteRecursively(new File(secondShuffleFile.getPath() + ".sorted"));
        JavaUtils.deleteRecursively(new File(secondShuffleFile.getPath() + ".index"));
      }
      clean();
    }
  }

  @Test
  public void testFailedSortRetryAlwaysEnqueuesReplacementSorter() throws Exception {
    PartitionFilesSorter partitionFilesSorter = null;
    CountDownLatch allowFailurePublicationToReturn = new CountDownLatch(1);
    CountDownLatch allowReplacementWriteIndexToReturn = new CountDownLatch(1);
    try {
      prepare(100);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.WORKER_PARTITION_SORTER_SORT_TIMEOUT().key(), "30s");
      AtomicInteger writeIndexAttempts = new AtomicInteger();
      CountDownLatch failurePublished = new CountDownLatch(1);
      CountDownLatch replacementWriteIndexStarted = new CountDownLatch(1);
      CountDownLatch failedSorterExited = new CountDownLatch(1);
      WorkerSource workerSource =
          new WorkerSource(conf) {
            @Override
            public void stopTimer(String metricName, String key) {
              super.stopTimer(metricName, key);
              if (WorkerSource.SORT_TIME().equals(metricName)) {
                failedSorterExited.countDown();
              }
            }
          };
      partitionFilesSorter =
          new PartitionFilesSorter(MemoryManager.instance(), conf, workerSource) {
            @Override
            protected void writeIndex(
                Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isDfs)
                throws IOException {
              int attempt = writeIndexAttempts.getAndIncrement();
              if (attempt == 0) {
                throw new IOException("Injected first sort failure.");
              }
              if (attempt == 1) {
                replacementWriteIndexStarted.countDown();
                try {
                  Assert.assertTrue(allowReplacementWriteIndexToReturn.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException("Interrupted while waiting to write the index.", e);
                }
              }
              super.writeIndex(indexMap, indexFilePath, isDfs);
            }

            @Override
            protected void failSortCompletionFuture(String shuffleKey, String fileId, Exception e) {
              super.failSortCompletionFuture(shuffleKey, fileId, e);
              failurePublished.countDown();
              try {
                Assert.assertTrue(allowFailurePublicationToReturn.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                    "Interrupted while waiting after sort failure.", interrupted);
              }
            }
          };

      CompletableFuture<FileInfo> firstWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-failed-sort",
              originFileName,
              partitionDataWriter.getDiskFileInfo(),
              5,
              10);
      Assert.assertTrue(failurePublished.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(firstWaiter.isCompletedExceptionally());

      CompletableFuture<FileInfo> retryWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-failed-sort",
              originFileName,
              partitionDataWriter.getDiskFileInfo(),
              5,
              10);
      Assert.assertTrue(replacementWriteIndexStarted.await(10, TimeUnit.SECONDS));
      allowFailurePublicationToReturn.countDown();
      Assert.assertTrue(failedSorterExited.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(1, partitionFilesSorter.getSortingCount());

      CompletableFuture<FileInfo> thirdWaiter =
          partitionFilesSorter.getSortedFileInfoAsync(
              "application-failed-sort",
              originFileName,
              partitionDataWriter.getDiskFileInfo(),
              5,
              10);
      Assert.assertFalse(thirdWaiter.isDone());
      allowReplacementWriteIndexToReturn.countDown();

      Assert.assertNotNull(retryWaiter.get(10, TimeUnit.SECONDS));
      Assert.assertNotNull(thirdWaiter.get(10, TimeUnit.SECONDS));
      Assert.assertEquals(2, writeIndexAttempts.get());
    } finally {
      allowFailurePublicationToReturn.countDown();
      allowReplacementWriteIndexToReturn.countDown();
      if (partitionFilesSorter != null) {
        partitionFilesSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
      }
      clean();
    }
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
    Assert.assertNull(partitionFilesSorter2.getSortedShuffleFiles("application-2-1"));
    Assert.assertEquals(
        partitionFilesSorter2.getSortedShuffleFiles("application-3-1").toString(), "[0-0-1]");
    partitionFilesSorter2.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
    recoverPath.delete();
  }
}
