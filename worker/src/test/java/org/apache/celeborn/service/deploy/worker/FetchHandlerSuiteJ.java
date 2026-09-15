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

package org.apache.celeborn.service.deploy.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.ChunkFetchSuccess;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.OpenStream;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.RpcResponse;
import org.apache.celeborn.common.network.protocol.StreamHandle;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbChunkFetchRequest;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbOpenStreamList;
import org.apache.celeborn.common.protocol.PbOpenStreamListResponse;
import org.apache.celeborn.common.protocol.PbStreamChunkSlice;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ShuffleBlockInfoUtils.ShuffleBlockInfo;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.PartitionFilesSorter;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class FetchHandlerSuiteJ {

  private static final Logger LOG = LoggerFactory.getLogger(FetchHandlerSuiteJ.class);

  private static final CelebornConf conf = new CelebornConf();

  private static final Random random = new Random();

  private static final UserIdentifier userIdentifier =
      new UserIdentifier("mock-tenantId", "mock-name");
  private static final int MAX_MAP_ID = 50;
  private static final int DATA_SIZE_PER_BATCH = 256 * 1024 - 16; // 256k - 16byte

  public FileInfo prepare(int batchCountPerMap) throws IOException {
    byte[] batchHeader = new byte[16];
    File shuffleFile = File.createTempFile("celeborn", UUID.randomUUID().toString());

    DiskFileInfo fileInfo = new DiskFileInfo(shuffleFile, userIdentifier, conf);
    FileOutputStream fileOutputStream = new FileOutputStream(shuffleFile);
    FileChannel channel = fileOutputStream.getChannel();
    Map<Integer, Integer> batchIds = new HashMap<>();
    ArrayList<Integer> mapIds = generateMapIds(batchCountPerMap);

    for (int mapId : mapIds) {
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
      byte[] mockedData = new byte[DATA_SIZE_PER_BATCH];
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET, mapId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 4, currentAttemptId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12, DATA_SIZE_PER_BATCH);
      ByteBuffer buf1 = ByteBuffer.wrap(batchHeader);
      while (buf1.hasRemaining()) {
        channel.write(buf1);
      }
      random.nextBytes(mockedData);
      ByteBuffer buf2 = ByteBuffer.wrap(mockedData);
      while (buf2.hasRemaining()) {
        channel.write(buf2);
      }
    }
    long originFileLen = channel.size();
    // update original fileInfo chunk offsets
    for (long offset = conf.shuffleChunkSize();
        offset <= originFileLen;
        offset += conf.shuffleChunkSize()) {
      (fileInfo.getReduceFileMeta()).addChunkOffset(offset);
    }
    // update sorted fileInfo chunk offsets
    fileInfo.updateBytesFlushed(originFileLen);
    return fileInfo;
  }

  public void cleanup(FileInfo fileInfo) throws IOException {
    if (fileInfo != null) {
      // origin file
      JavaUtils.deleteRecursively(((DiskFileInfo) fileInfo).getFile());
      // sorted file
      JavaUtils.deleteRecursively(
          new File(((DiskFileInfo) fileInfo).getFile().getPath() + ".sorted"));
      // index file
      JavaUtils.deleteRecursively(
          new File(((DiskFileInfo) fileInfo).getFile().getPath() + ".index"));
    }
  }

  @BeforeClass
  public static void beforeAll() {
    MemoryManager.initialize(conf);
  }

  @AfterClass
  public static void afterAll() {
    MemoryManager.reset();
  }

  @Test
  public void testFetchOriginFile() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler streamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);

      fetchChunkAndCheck(client, channel, fetchHandler, streamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testFetchSortFile() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler streamHandler = openStreamAndCheck(client, channel, fetchHandler, 5, 10);

      fetchChunkAndCheck(client, channel, fetchHandler, streamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testOpenStreamDoesNotBlockWhileSortedFileInfoIsPending() throws Exception {
    FileInfo fileInfo = null;
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    try {
      fileInfo = prepare(1);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);
      PartitionFilesSorter partitionFilesSorter = mock(PartitionFilesSorter.class);
      CompletableFuture<FileInfo> pendingSortedFileInfo = new CompletableFuture<>();
      when(partitionFilesSorter.getSortedFileInfoAsync(
              anyString(), anyString(), eq(fileInfo), anyInt(), anyInt()))
          .thenReturn(pendingSortedFileInfo);
      fetchHandler.setPartitionsSorter(partitionFilesSorter);

      PbOpenStream request =
          PbOpenStream.newBuilder()
              .setShuffleKey(shuffleKey)
              .setFileName(fileName)
              .setStartIndex(5)
              .setEndIndex(10)
              .build();
      Future<?> receiveTask =
          requestExecutor.submit(
              () ->
                  fetchHandler.receive(
                      client,
                      new RpcRequest(
                          dummyRequestId,
                          new NioManagedBuffer(
                              new TransportMessage(MessageType.OPEN_STREAM, request.toByteArray())
                                  .toByteBuffer())),
                      createRpcResponseCallback(channel)));

      receiveTask.get(1, TimeUnit.SECONDS);
      assertNull(channel.readOutbound());

      pendingSortedFileInfo.complete(fileInfo);

      assertTrue(waitForOutbound(channel) instanceof RpcResponse);
      verify(partitionFilesSorter)
          .getSortedFileInfoAsync(anyString(), anyString(), eq(fileInfo), anyInt(), anyInt());
    } finally {
      requestExecutor.shutdownNow();
      cleanup(fileInfo);
    }
  }

  @Test
  public void testBatchOpenStreamDoesNotBlockAndPreservesRequestOrder() throws Exception {
    FileInfo fileInfo = null;
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    try {
      fileInfo = prepare(1);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);
      PartitionFilesSorter partitionFilesSorter = mock(PartitionFilesSorter.class);
      CompletableFuture<FileInfo> firstSortedFileInfo = new CompletableFuture<>();
      CompletableFuture<FileInfo> secondSortedFileInfo = new CompletableFuture<>();
      String firstFileName = fileName + "-first";
      String secondFileName = fileName + "-second";
      when(partitionFilesSorter.getSortedFileInfoAsync(
              eq(shuffleKey), eq(firstFileName), eq(fileInfo), anyInt(), anyInt()))
          .thenReturn(firstSortedFileInfo);
      when(partitionFilesSorter.getSortedFileInfoAsync(
              eq(shuffleKey), eq(secondFileName), eq(fileInfo), anyInt(), anyInt()))
          .thenReturn(secondSortedFileInfo);
      fetchHandler.setPartitionsSorter(partitionFilesSorter);

      PbOpenStreamList request =
          PbOpenStreamList.newBuilder()
              .setShuffleKey(shuffleKey)
              .addFileName(firstFileName)
              .addFileName(secondFileName)
              .addStartIndex(5)
              .addStartIndex(10)
              .addEndIndex(10)
              .addEndIndex(15)
              .addReadLocalShuffle(false)
              .addReadLocalShuffle(false)
              .build();
      Future<?> receiveTask =
          requestExecutor.submit(
              () ->
                  fetchHandler.receive(
                      client,
                      new RpcRequest(
                          dummyRequestId,
                          new NioManagedBuffer(
                              new TransportMessage(
                                      MessageType.BATCH_OPEN_STREAM, request.toByteArray())
                                  .toByteBuffer())),
                      createRpcResponseCallback(channel)));

      receiveTask.get(1, TimeUnit.SECONDS);
      assertNull(channel.readOutbound());

      secondSortedFileInfo.complete(fileInfo);
      assertNull(channel.readOutbound());
      firstSortedFileInfo.complete(fileInfo);

      RpcResponse result = (RpcResponse) waitForOutbound(channel);
      PbOpenStreamListResponse response =
          TransportMessage.fromByteBuffer(result.body().nioByteBuffer()).getParsedPayload();
      assertEquals(2, response.getStreamHandlerOptCount());
      assertTrue(
          response.getStreamHandlerOpt(0).getStreamHandler().getStreamId()
              < response.getStreamHandlerOpt(1).getStreamHandler().getStreamId());
    } finally {
      requestExecutor.shutdownNow();
      cleanup(fileInfo);
    }
  }

  @Test
  public void testBatchOpenStreamRejectsMismatchedRequestFieldLengths() {
    EmbeddedChannel channel = new EmbeddedChannel();
    TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
    FetchHandler fetchHandler = mockFetchHandler(null);
    PbOpenStreamList validRequest =
        PbOpenStreamList.newBuilder()
            .setShuffleKey(shuffleKey)
            .addFileName(fileName)
            .addStartIndex(5)
            .addEndIndex(10)
            .addReadLocalShuffle(false)
            .build();
    PbOpenStreamList[] malformedRequests = {
      validRequest.toBuilder().clearStartIndex().build(),
      validRequest.toBuilder().clearEndIndex().build(),
      validRequest.toBuilder().clearReadLocalShuffle().build(),
      validRequest.toBuilder().addEndIndex(15).build()
    };

    for (PbOpenStreamList malformedRequest : malformedRequests) {
      fetchHandler.receive(
          client,
          new RpcRequest(
              dummyRequestId,
              new NioManagedBuffer(
                  new TransportMessage(
                          MessageType.BATCH_OPEN_STREAM, malformedRequest.toByteArray())
                      .toByteBuffer())),
          createRpcResponseCallback(channel));

      Object response = channel.readOutbound();
      assertTrue(response instanceof RpcFailure);
      assertTrue(((RpcFailure) response).errorString.contains("Invalid open stream list"));
      assertNull(channel.readOutbound());
    }

    verify(fetchHandler.workerSource(), never()).startTimer(anyString(), anyString());
    verify(fetchHandler, never()).getRawFileInfo(anyString(), anyString());
  }

  @Test
  public void testOpenStreamReportsAsynchronousSortFailure() throws Exception {
    FileInfo fileInfo = null;
    try {
      fileInfo = prepare(1);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);
      PartitionFilesSorter partitionFilesSorter = mock(PartitionFilesSorter.class);
      CompletableFuture<FileInfo> pendingSortedFileInfo = new CompletableFuture<>();
      when(partitionFilesSorter.getSortedFileInfoAsync(
              anyString(), anyString(), eq(fileInfo), anyInt(), anyInt()))
          .thenReturn(pendingSortedFileInfo);
      fetchHandler.setPartitionsSorter(partitionFilesSorter);

      PbOpenStream request =
          PbOpenStream.newBuilder()
              .setShuffleKey(shuffleKey)
              .setFileName(fileName)
              .setStartIndex(5)
              .setEndIndex(10)
              .build();
      fetchHandler.receive(
          client,
          new RpcRequest(
              dummyRequestId,
              new NioManagedBuffer(
                  new TransportMessage(MessageType.OPEN_STREAM, request.toByteArray())
                      .toByteBuffer())),
          createRpcResponseCallback(channel));

      assertNull(channel.readOutbound());
      pendingSortedFileInfo.completeExceptionally(new IOException("sort failed"));

      assertTrue(waitForOutbound(channel) instanceof RpcFailure);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testLegacyOpenStream() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      legacyOpenStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);

    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testWorkerReadSortFileOnceOriginalFileBeDeleted() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler rangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 5, 10);
      checkOriginFileBeDeleted(fileInfo);
      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);
      fetchChunkAndCheck(client, channel, fetchHandler, nonRangeReadStreamHandler);
      fetchChunkAndCheck(client, channel, fetchHandler, rangeReadStreamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testLocalReadSortFileOnceOriginalFileBeDeleted() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      // read local shuffle
      openStreamAndCheck(client, channel, fetchHandler, 5, 10, true);
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testDoNotDeleteOriginalFileWhenNonRangeWorkerReadWorkInProgress() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);
      PbStreamHandler rangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 5, 10);
      fetchChunkAndCheck(client, channel, fetchHandler, nonRangeReadStreamHandler);
      fetchChunkAndCheck(client, channel, fetchHandler, rangeReadStreamHandler);

      // non-range fetch finished.
      bufferStreamEnd(client, fetchHandler, nonRangeReadStreamHandler.getStreamId());
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testDoNotDeleteOriginalFileWhenNonRangeLocalReadWorkInProgress() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      // read local shuffle
      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE, true);
      openStreamAndCheck(client, channel, fetchHandler, 5, 10);

      // non-range fetch finished.
      bufferStreamEnd(client, fetchHandler, nonRangeReadStreamHandler.getStreamId());
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testCleanupFailsPendingResolveWithoutRecreatingCacheOrStream() throws Exception {
    FileInfo fileInfo = null;
    PartitionFilesSorter sorter = null;
    FetchHandler fetchHandler = null;
    EmbeddedChannel channel = new EmbeddedChannel();
    CountDownLatch indexRead = new CountDownLatch(1);
    CountDownLatch allowIndexPublication = new CountDownLatch(1);
    HashSet<String> expiredShuffleKeys = new HashSet<>(Collections.singleton(shuffleKey));
    try {
      fileInfo = prepare(1);
      CelebornConf sorterConf = new CelebornConf();
      sorterConf.set(CelebornConf.WORKER_PARTITION_SORTER_RESOLVE_THREADS().key(), "1");
      sorter =
          new PartitionFilesSorter(MemoryManager.instance(), sorterConf, mock(WorkerSource.class)) {
            @Override
            protected Map<Integer, List<ShuffleBlockInfo>> readIndex(String indexFilePath)
                throws IOException {
              Map<Integer, List<ShuffleBlockInfo>> indexes = super.readIndex(indexFilePath);
              indexRead.countDown();
              try {
                assertTrue(
                    "Index reader was never released",
                    allowIndexPublication.await(30, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while holding the resolver", e);
              }
              return indexes;
            }
          };
      fetchHandler = mockFetchHandler(fileInfo, sorter);
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      Map<?, ?> indexCacheNames = getSorterField(sorter, "indexCacheNames", Map.class);
      Cache<?, ?> indexCache = getSorterField(sorter, "indexCache", Cache.class);
      ExecutorService resolverExecutor =
          getSorterField(sorter, "sortedFileResolveExecutors", ExecutorService.class);
      String fileId = shuffleKey + "-" + fileName;
      receiveRangeOpenStream(fetchHandler, client);

      assertTrue("Resolver did not read the index", indexRead.await(10, TimeUnit.SECONDS));
      assertEquals(1, sorter.getSortedFileWaiterCount());
      assertNull(channel.readOutbound());

      // Worker.cleanup performs these in this order, then schedules physical storage cleanup.
      // Keep the files until finally to model that storage-cleanup task not having run yet.
      fetchHandler.cleanupExpiredShuffleKey(expiredShuffleKeys);
      sorter.cleanup(expiredShuffleKeys);
      assertEquals(0, sorter.getSortedFileWaiterCount());
      assertTrue(waitForOutbound(channel) instanceof RpcFailure);
      assertEquals(0, fetchHandler.chunkStreamManager().getStreamsCount());
      assertFalse(indexCacheNames.containsKey(shuffleKey));
      assertFalse(indexCache.asMap().containsKey(fileId));

      // Drain the single resolver after releasing it, without closing the sorter and clearing its
      // cache. An already-failed request must not hide late cache or stream publication.
      Future<?> resolveFinished = resolverExecutor.submit(() -> {});
      allowIndexPublication.countDown();
      resolveFinished.get(10, TimeUnit.SECONDS);
      assertFalse(indexCacheNames.containsKey(shuffleKey));
      assertFalse(indexCache.asMap().containsKey(fileId));
      assertEquals(0, fetchHandler.chunkStreamManager().getStreamsCount());
      assertNull(channel.readOutbound());
    } finally {
      allowIndexPublication.countDown();
      try {
        if (sorter != null) {
          sorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
        }
      } finally {
        if (fetchHandler != null) {
          fetchHandler.chunkStreamManager().cleanupExpiredShuffleKey(expiredShuffleKeys);
        }
        channel.finishAndReleaseAll();
        cleanup(fileInfo);
      }
    }
  }

  @Test
  public void testCleanupRejectsOpenStreamAfterFileLookup() throws Exception {
    FileInfo fileInfo = null;
    PartitionFilesSorter sorter = null;
    FetchHandler fetchHandler = null;
    EmbeddedChannel channel = new EmbeddedChannel();
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch fileInfoCaptured = new CountDownLatch(1);
    CountDownLatch allowFileLookup = new CountDownLatch(1);
    HashSet<String> expiredShuffleKeys = new HashSet<>(Collections.singleton(shuffleKey));
    try {
      fileInfo = prepare(1);
      sorter =
          spy(new PartitionFilesSorter(MemoryManager.instance(), conf, mock(WorkerSource.class)));
      FetchHandler handler = mockFetchHandler(fileInfo, sorter);
      fetchHandler = handler;
      when(handler.storageManager().getFileInfo(shuffleKey, fileName)).thenReturn(fileInfo);
      doAnswer(
              invocation -> {
                FileInfo capturedFileInfo = (FileInfo) invocation.callRealMethod();
                fileInfoCaptured.countDown();
                try {
                  assertTrue(
                      "File lookup was never released",
                      allowFileLookup.await(30, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException("Interrupted while holding file lookup", e);
                }
                return capturedFileInfo;
              })
          .when(handler)
          .getRawFileInfo(shuffleKey, fileName);
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      Future<?> receiveTask = requestExecutor.submit(() -> receiveRangeOpenStream(handler, client));

      assertTrue("File lookup did not start", fileInfoCaptured.await(10, TimeUnit.SECONDS));
      handler.cleanupExpiredShuffleKey(expiredShuffleKeys);
      sorter.cleanup(expiredShuffleKeys);
      // Keep returning the captured metadata until physical cleanup runs. Expiry, rather than a
      // missing file, must prevent this old request from starting another sort.
      allowFileLookup.countDown();
      receiveTask.get(5, TimeUnit.SECONDS);
      verify(sorter, never())
          .getSortedFileInfoAsync(
              anyString(), anyString(), any(FileInfo.class), anyInt(), anyInt());
      assertTrue(waitForOutbound(channel) instanceof RpcFailure);
      assertEquals(0, handler.chunkStreamManager().getStreamsCount());
    } finally {
      allowFileLookup.countDown();
      requestExecutor.shutdownNow();
      try {
        requestExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } finally {
        try {
          if (sorter != null) {
            sorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
          }
        } finally {
          if (fetchHandler != null) {
            fetchHandler.chunkStreamManager().cleanupExpiredShuffleKey(expiredShuffleKeys);
          }
          channel.finishAndReleaseAll();
          cleanup(fileInfo);
        }
      }
    }
  }

  @Test
  public void testDfsSorterPreparationDoesNotBlockReceiveCaller() throws Exception {
    Map<StorageInfo.Type, FileSystem> previousFileSystems = StorageManager.hadoopFs();
    FileSystem metadataFileSystem = mock(FileSystem.class);
    CountDownLatch metadataEntered = new CountDownLatch(1);
    CountDownLatch releaseMetadata = new CountDownLatch(1);
    AtomicReference<Thread> receiveThread = new AtomicReference<>();
    AtomicReference<Thread> metadataThread = new AtomicReference<>();
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    EmbeddedChannel channel = new EmbeddedChannel();
    PartitionFilesSorter actualSorter = null;
    try {
      StorageManager.hadoopFs_$eq(
          Collections.singletonMap(StorageInfo.Type.HDFS, metadataFileSystem));
      when(metadataFileSystem.exists(any(Path.class)))
          .thenAnswer(
              invocation -> {
                metadataThread.set(Thread.currentThread());
                metadataEntered.countDown();
                try {
                  if (!releaseMetadata.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting to release DFS metadata probe.");
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException("Interrupted DFS metadata probe.", e);
                }
                // Fail before opening any files, so the test never accesses a real DFS.
                throw new IOException("Injected DFS metadata failure after release.");
              });
      DiskFileInfo fileInfo =
          new DiskFileInfo(
              userIdentifier,
              true,
              new ReduceFileMeta(conf.shuffleChunkSize()),
              "hdfs://test.invalid/shuffle/partition",
              StorageInfo.Type.HDFS);
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);
      // This fixture creates a real sorter; keep and close it instead of mocking its async API.
      actualSorter = fetchHandler.partitionsSorter();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      Future<?> receiveTask =
          requestExecutor.submit(
              () -> {
                receiveThread.set(Thread.currentThread());
                receiveRangeOpenStream(fetchHandler, client);
              });

      assertTrue("DFS metadata probe was not reached.", metadataEntered.await(5, TimeUnit.SECONDS));
      assertNotSame(
          "DFS exists must not run on the receive caller.",
          receiveThread.get(),
          metadataThread.get());
      receiveTask.get(5, TimeUnit.SECONDS);
      Future<?> sentinelTask = requestExecutor.submit(() -> {});
      sentinelTask.get(5, TimeUnit.SECONDS);
      assertNull(channel.readOutbound());

      releaseMetadata.countDown();
      assertTrue(waitForOutbound(channel) instanceof RpcFailure);
    } finally {
      releaseMetadata.countDown();
      requestExecutor.shutdownNow();
      try {
        requestExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } finally {
        try {
          if (actualSorter != null) {
            actualSorter.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
          }
        } finally {
          StorageManager.hadoopFs_$eq(previousFileSystems);
          channel.finishAndReleaseAll();
        }
      }
    }
  }

  private void receiveRangeOpenStream(FetchHandler fetchHandler, TransportClient client) {
    PbOpenStream request =
        PbOpenStream.newBuilder()
            .setShuffleKey(shuffleKey)
            .setFileName(fileName)
            .setStartIndex(5)
            .setEndIndex(10)
            .build();
    fetchHandler.receive(
        client,
        new RpcRequest(
            dummyRequestId,
            new NioManagedBuffer(
                new TransportMessage(MessageType.OPEN_STREAM, request.toByteArray())
                    .toByteBuffer())),
        createRpcResponseCallback(client.getChannel()));
  }

  private static <T> T getSorterField(PartitionFilesSorter sorter, String name, Class<T> fieldType)
      throws ReflectiveOperationException {
    Field field = PartitionFilesSorter.class.getDeclaredField(name);
    field.setAccessible(true);
    return fieldType.cast(field.get(sorter));
  }

  private FetchHandler mockFetchHandler(FileInfo fileInfo) {
    return mockFetchHandler(fileInfo, null);
  }

  private FetchHandler mockFetchHandler(
      FileInfo fileInfo, PartitionFilesSorter partitionFilesSorter) {
    WorkerSource workerSource = mock(WorkerSource.class);
    TransportConf transportConf =
        Utils.fromCelebornConf(conf, TransportModuleConstants.FETCH_MODULE, 4);
    FetchHandler fetchHandler0 = new FetchHandler(conf, transportConf, workerSource);
    Worker worker = mock(Worker.class);
    if (partitionFilesSorter == null) {
      partitionFilesSorter = new PartitionFilesSorter(MemoryManager.instance(), conf, workerSource);
    }

    StorageManager storageManager = mock(StorageManager.class);
    Mockito.doReturn(storageManager).when(worker).storageManager();
    Mockito.doReturn(workerSource).when(worker).workerSource();
    Mockito.doReturn(partitionFilesSorter).when(worker).partitionsSorter();
    fetchHandler0.init(worker);
    FetchHandler fetchHandler = spy(fetchHandler0);
    Mockito.doReturn(fileInfo).when(fetchHandler).getRawFileInfo(anyString(), anyString());
    return fetchHandler;
  }

  private final String shuffleKey = "dummyShuffleKey-123";
  private final String fileName = "dummyFileName";
  private final long dummyRequestId = 0;

  @Deprecated
  private void legacyOpenStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex)
      throws IOException {
    ByteBuffer openStreamByteBuffer =
        new OpenStream(shuffleKey, fileName, startIndex, endIndex).toByteBuffer();
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(openStreamByteBuffer)),
        createRpcResponseCallback(channel));
    RpcResponse result = (RpcResponse) waitForOutbound(channel);
    StreamHandle streamHandler = (StreamHandle) Message.decode(result.body().nioByteBuffer());
    if (endIndex == Integer.MAX_VALUE) {
      assertEquals(50, streamHandler.numChunks);
    } else {
      assertEquals(endIndex - startIndex, streamHandler.numChunks);
    }
  }

  private PbStreamHandler openStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex)
      throws IOException {
    return openStreamAndCheck(client, channel, fetchHandler, startIndex, endIndex, false);
  }

  private PbStreamHandler openStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex,
      Boolean readLocalShuffle)
      throws IOException {
    ByteBuffer openStreamByteBuffer =
        new TransportMessage(
                MessageType.OPEN_STREAM,
                PbOpenStream.newBuilder()
                    .setShuffleKey(shuffleKey)
                    .setFileName(fileName)
                    .setStartIndex(startIndex)
                    .setEndIndex(endIndex)
                    .setReadLocalShuffle(readLocalShuffle)
                    .build()
                    .toByteArray())
            .toByteBuffer();
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(openStreamByteBuffer)),
        createRpcResponseCallback(channel));
    RpcResponse result = (RpcResponse) waitForOutbound(channel);
    PbStreamHandler streamHandler =
        TransportMessage.fromByteBuffer(result.body().nioByteBuffer()).getParsedPayload();
    if (endIndex == Integer.MAX_VALUE) {
      assertEquals(50, streamHandler.getNumChunks());
    } else {
      assertEquals(endIndex - startIndex, streamHandler.getNumChunks());
    }
    return streamHandler;
  }

  private void fetchChunkAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      PbStreamHandler streamHandler) {
    for (int chunkIndex = 0; chunkIndex < streamHandler.getNumChunks(); chunkIndex++) {
      fetchHandler.receive(
          client,
          new RpcRequest(
              TransportClient.requestId(),
              new NioManagedBuffer(
                  new TransportMessage(
                          MessageType.CHUNK_FETCH_REQUEST,
                          PbChunkFetchRequest.newBuilder()
                              .setStreamChunkSlice(
                                  PbStreamChunkSlice.newBuilder()
                                      .setStreamId(streamHandler.getStreamId())
                                      .setChunkIndex(chunkIndex)
                                      .setOffset(0)
                                      .setLen(Integer.MAX_VALUE))
                              .build()
                              .toByteArray())
                      .toByteBuffer())),
          createRpcResponseCallback(channel));
      ChunkFetchSuccess chunkFetchSuccess = channel.readOutbound();
      chunkFetchSuccess.body().retain();
      // chunk size 8m
      assertEquals(chunkFetchSuccess.body().size(), 8 * 1024 * 1024);
      chunkFetchSuccess.body().release();
    }
  }

  private void bufferStreamEnd(TransportClient client, FetchHandler fetchHandler, long streamId) {
    TransportMessage bufferStreamEnd =
        new TransportMessage(
            MessageType.BUFFER_STREAM_END,
            PbBufferStreamEnd.newBuilder()
                .setStreamId(streamId)
                .setStreamType(StreamType.ChunkStream)
                .build()
                .toByteArray());
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(bufferStreamEnd.toByteBuffer())),
        createRpcResponseCallback(client.getChannel()));
  }

  private void checkOriginFileBeDeleted(FileInfo fileInfo) {
    assertFalse(((DiskFileInfo) fileInfo).getFilePath().endsWith(".sorted"));
    long startTs = System.currentTimeMillis();
    boolean deleted = false;
    long timeout = 5 * 1000; // 5s
    while (!deleted) {
      deleted = !((DiskFileInfo) fileInfo).getFile().exists();
      if (System.currentTimeMillis() - startTs > timeout) {
        fail("Origin file was not deleted within the expected timeout of 5 seconds.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("catch Exception when checking origin file states.", e);
      }
    }
    assertTrue(deleted);
  }

  private ArrayList<Integer> generateMapIds(int batchCountPerMap) {
    int mapCount = batchCountPerMap * MAX_MAP_ID;
    ArrayList<Integer> ids = new ArrayList<>(mapCount);
    for (int i = 0; i < mapCount; i++) {
      ids.add(i / batchCountPerMap);
    }
    Collections.shuffle(ids);
    return ids;
  }

  private RpcResponseCallback createRpcResponseCallback(Channel channel) {
    return new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        channel.writeAndFlush(new RpcResponse(dummyRequestId, new NioManagedBuffer(response)));
      }

      @Override
      public void onFailure(Throwable e) {
        channel.writeAndFlush(new RpcFailure(dummyRequestId, Throwables.getStackTraceAsString(e)));
      }
    };
  }

  private Object waitForOutbound(EmbeddedChannel channel) {
    Object outbound = null;
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (outbound == null && System.nanoTime() < deadlineNanos) {
      outbound = channel.readOutbound();
      if (outbound == null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Interrupted while waiting for an outbound message.", e);
        }
      }
    }
    assertNotNull("Timed out waiting for an outbound message.", outbound);
    return outbound;
  }
}
