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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import scala.Function0;
import scala.collection.mutable.ListBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
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
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.FetchHandler;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.*;

public class DiskReducePartitionDataWriterSuiteJ {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskReducePartitionDataWriterSuiteJ.class);

  private static final CelebornConf CONF = new CelebornConf();
  public static final Long SPLIT_THRESHOLD = 256 * 1024 * 1024L;
  public static final PartitionSplitMode splitMode = PartitionSplitMode.HARD;

  private static File tempDir = null;
  private static LocalFlusher localFlusher = null;
  private static WorkerSource source = null;

  private TransportContext transportContext;
  private TransportServer server;
  private TransportClientFactory clientFactory;
  private static long streamId;
  private static int numChunks;
  private final UserIdentifier userIdentifier = new UserIdentifier("mock-tenantId", "mock-name");
  private static StoragePolicy storagePolicy;

  @BeforeClass
  public static void beforeAll() {
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn");
    CONF.set(CelebornConf.SHUFFLE_CHUNK_SIZE().key(), "1k");
    storagePolicy = Mockito.mock(StoragePolicy.class);

    source = Mockito.mock(WorkerSource.class);
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

  protected TransportConf createModuleTransportConf(String module) {
    return new TransportConf(module, new CelebornConf());
  }

  public void setupChunkServer(DiskFileInfo info) throws IOException {
    TransportConf transConf = createModuleTransportConf("shuffle");

    FetchHandler handler =
        new FetchHandler(transConf.getCelebornConf(), transConf, mock(WorkerSource.class)) {
          @Override
          public StorageManager storageManager() {
            return new StorageManager(CONF, source);
          }

          @Override
          public FileInfo getRawFileInfo(String shuffleKey, String fileName) {
            return info;
          }

          @Override
          public WorkerSource workerSource() {
            return source;
          }

          @Override
          public boolean checkRegistered() {
            return true;
          }
        };
    PartitionFilesSorter sorter = mock(PartitionFilesSorter.class);
    Mockito.doReturn(info)
        .when(sorter)
        .getSortedFileInfo(anyString(), anyString(), eq(info), anyInt(), anyInt());
    handler.setPartitionsSorter(sorter);
    transportContext = new TransportContext(transConf, handler);
    server = transportContext.createServer();

    clientFactory = transportContext.createClientFactory();
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

  public void closeChunkServer() {
    server.close();
    clientFactory.close();
    transportContext.close();
  }

  static class FetchResult {
    public Set<Integer> successChunks;
    public Set<Integer> failedChunks;
    public List<ManagedBuffer> buffers;

    public void releaseBuffers() {
      for (ManagedBuffer buffer : buffers) {
        buffer.release();
      }
    }
  }

  public ByteBuffer createOpenMessage() {
    TransportMessage message =
        new TransportMessage(
            MessageType.OPEN_STREAM,
            PbOpenStream.newBuilder()
                .setShuffleKey("shuffleKey-123")
                .setFileName("location")
                .setStartIndex(0)
                .setEndIndex(Integer.MAX_VALUE)
                .build()
                .toByteArray());

    return message.toByteBuffer();
  }

  private void setUpConn(TransportClient client) throws IOException {
    ByteBuffer resp = client.sendRpcSync(createOpenMessage(), 10000);
    PbStreamHandler streamHandler = TransportMessage.fromByteBuffer(resp).getParsedPayload();
    streamId = streamHandler.getStreamId();
    numChunks = streamHandler.getNumChunks();
  }

  private FetchResult fetchChunks(TransportClient client, List<Integer> chunkIndices)
      throws Exception {
    final Semaphore sem = new Semaphore(0);

    final FetchResult res = new FetchResult();
    res.successChunks = Collections.synchronizedSet(new HashSet<>());
    res.failedChunks = Collections.synchronizedSet(new HashSet<>());
    res.buffers = Collections.synchronizedList(new LinkedList<>());

    ChunkReceivedCallback callback =
        new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            buffer.retain();
            res.successChunks.add(chunkIndex);
            res.buffers.add(buffer);
            sem.release();
          }

          @Override
          public void onFailure(int chunkIndex, Throwable e) {
            res.failedChunks.add(chunkIndex);
            sem.release();
          }
        };

    for (int chunkIndex : chunkIndices) {
      client.fetchChunk(streamId, chunkIndex, 10000, callback);
    }
    if (!sem.tryAcquire(chunkIndices.size(), 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }

    client.close();
    return res;
  }

  @Test
  public void testMultiThreadWrite() throws IOException, ExecutionException, InterruptedException {
    final int threadsNum = 8;

    PartitionDataWriterContext context =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    PartitionDataWriter partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, CONF, storagePolicy, context),
            CONF,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context,
            PartitionType.REDUCE);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-1");
    AtomicLong length = new AtomicLong(0);

    for (int i = 0; i < threadsNum; ++i) {
      futures.add(
          es.submit(
              () -> {
                byte[] bytes = generateData();
                length.addAndGet(bytes.length);
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                try {
                  partitionDataWriter.write(buf);
                } catch (IOException e) {
                  LOG.error("Failed to write buffer.", e);
                }
              }));
    }
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = partitionDataWriter.close();

    assertEquals(length.get(), bytesWritten);
    assertEquals(new File(partitionDataWriter.getFilePath()).length(), bytesWritten);
  }

  @Test
  public void testMultiThreadWriteDuringClose()
      throws IOException, ExecutionException, InterruptedException {
    final int threadsNum = 8;
    PartitionDataWriterContext context =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    PartitionDataWriter partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, CONF, storagePolicy, context),
            CONF,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context,
            PartitionType.REDUCE);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-1");
    AtomicLong length = new AtomicLong(0);
    AtomicLong bytesWritten = new AtomicLong(0);

    for (int i = 0; i < threadsNum; ++i) {
      futures.add(
          es.submit(
              () -> {
                byte[] bytes = generateData();
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                try {
                  partitionDataWriter.write(buf);
                  length.addAndGet(bytes.length);
                  bytesWritten.set(partitionDataWriter.close());
                } catch (IOException e) {
                  LOG.debug("File is already closed.", e);
                }
              }));
    }
    for (Future<?> future : futures) {
      future.get();
    }

    assertEquals(length.get(), bytesWritten.get());
    assertEquals(new File(partitionDataWriter.getFilePath()).length(), bytesWritten.get());
    assertEquals(partitionDataWriter.getDiskFileInfo().getFileLength(), bytesWritten.get());
  }

  @Test
  public void testAfterStressfulWriteWillReadCorrect()
      throws IOException, ExecutionException, InterruptedException {
    final int threadsNum = Runtime.getRuntime().availableProcessors();
    PartitionDataWriterContext context =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    PartitionDataWriter partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, CONF, storagePolicy, context),
            CONF,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context,
            PartitionType.REDUCE);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-2");
    AtomicLong length = new AtomicLong(0);
    for (int i = 0; i < threadsNum; ++i) {
      futures.add(
          es.submit(
              () -> {
                for (int j = 0; j < 100; ++j) {
                  byte[] bytes = generateData();
                  length.addAndGet(bytes.length);
                  ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                  try {
                    partitionDataWriter.write(buf);
                  } catch (IOException e) {
                    LOG.error("Failed to write buffer.", e);
                  }
                }
              }));
    }
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = partitionDataWriter.close();
    assertEquals(length.get(), bytesWritten);
  }

  @Test
  public void testHugeBufferQueueSize() throws IOException {
    File file = getTemporaryFile();
    ListBuffer<File> dirs = new ListBuffer<>();
    dirs.$plus$eq(file);
    localFlusher =
        new LocalFlusher(
            source,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            1,
            NettyUtils.getByteBufAllocator(new TransportConf("test", CONF), null, true),
            256,
            "disk2",
            StorageInfo.Type.HDD,
            null);
  }

  @Test
  public void testWriteAndChunkRead() throws Exception {
    final int threadsNum = 16;
    PartitionDataWriterContext context =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    PartitionDataWriter partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, CONF, storagePolicy, context),
            CONF,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context,
            PartitionType.REDUCE);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-2");
    AtomicLong length = new AtomicLong(0);
    futures.add(
        es.submit(
            () -> {
              for (int j = 0; j < 1000; ++j) {
                byte[] bytes = generateData();
                length.addAndGet(bytes.length);
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                buf.retain();
                try {
                  partitionDataWriter.incrementPendingWrites();
                  partitionDataWriter.write(buf);
                } catch (IOException e) {
                  LOG.error("Failed to write buffer.", e);
                }
                buf.release();
              }
            }));
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = partitionDataWriter.close();
    assertEquals(length.get(), bytesWritten);

    setupChunkServer(partitionDataWriter.getDiskFileInfo());

    TransportClient client =
        clientFactory.createClient(InetAddress.getLocalHost().getHostAddress(), server.getPort());

    setUpConn(client);

    int chunkNums = numChunks;
    List<Integer> indices = new ArrayList<>();

    for (int i = 0; i < chunkNums; i++) {
      indices.add(i);
    }

    FetchResult result = fetchChunks(client, indices);

    Thread.sleep(3000);

    assertEquals(result.buffers.size(), chunkNums);
    assertEquals(result.successChunks.size(), chunkNums);

    result.releaseBuffers();

    closeChunkServer();
  }

  @Test
  public void testCompositeBufClear() {
    ByteBuf buf = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf2 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf3 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf4 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf5 = buf4.duplicate();

    buf5.retain();
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true, buf);
    compositeByteBuf.addComponent(true, buf2);
    compositeByteBuf.addComponent(true, buf3);
    compositeByteBuf.addComponent(true, buf4);
    compositeByteBuf.addComponent(true, buf5);

    assertEquals(1, buf.refCnt());
    assertEquals(1, buf2.refCnt());
    assertEquals(1, buf3.refCnt());
    assertEquals(2, buf4.refCnt());
    assertEquals(2, buf5.refCnt());
    assertNotEquals(0, compositeByteBuf.readableBytes());

    compositeByteBuf.removeComponents(0, compositeByteBuf.numComponents());
    compositeByteBuf.clear();

    assertEquals(0, compositeByteBuf.readableBytes());

    assertEquals(0, buf.refCnt());
    assertEquals(0, buf2.refCnt());
    assertEquals(0, buf3.refCnt());
    assertEquals(0, buf4.refCnt());
    assertEquals(0, buf5.refCnt());
    assertEquals(0, compositeByteBuf.numComponents());
  }

  @Test
  public void testChunkSize() throws IOException {
    // NOTE: SHUFFLE_CHUNK_SIZE and WORKER_FLUSHER_BUFFER_SIZE are set to 1K/128B
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.SHUFFLE_CHUNK_SIZE().key(), "1k");
    conf.set(CelebornConf.WORKER_FLUSHER_BUFFER_SIZE().key(), "128B");

    // case 1: write 8MiB
    PartitionDataWriterContext context1 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    PartitionDataWriter partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context1),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context1,
            PartitionType.REDUCE);
    partitionDataWriter.write(generateData(8 * 1024 * 1024));
    partitionDataWriter.close();
    ReduceFileMeta reduceFileMeta =
        (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 1);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 8 * 1024 * 1024);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0),
        8 * 1024 * 1024);

    // case 2: write 1024B
    PartitionDataWriterContext context2 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context2),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context2,
            PartitionType.REDUCE);
    for (int i = 0; i < 8; i++) {
      partitionDataWriter.write(generateData(128));
    }
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 1);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 1024);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0), 1024);

    // case 3: write 1023B
    PartitionDataWriterContext context3 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context3),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context3,
            PartitionType.REDUCE);
    partitionDataWriter.write(generateData(1020));
    partitionDataWriter.write(generateData(3));
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 1);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 1023);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0), 1023);

    // case 4: write 1025B
    PartitionDataWriterContext context4 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context4),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context4,
            PartitionType.REDUCE);
    for (int i = 0; i < 8; i++) {
      partitionDataWriter.write(generateData(128));
    }
    partitionDataWriter.write(generateData(1));
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 2);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 1025);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0), 1024);

    // case 5: write 2048B
    PartitionDataWriterContext context5 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context5),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context5,
            PartitionType.REDUCE);
    for (int i = 0; i < 16; i++) {
      partitionDataWriter.write(generateData(128));
    }
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 2);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 2048);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0), 1024);

    // case 5.1: write 2048B with trim; without PR #1702 this case will fail
    PartitionDataWriterContext context6 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context6),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context6,
            PartitionType.REDUCE);
    for (int i = 0; i < 16; i++) {
      partitionDataWriter.write(generateData(128));
    }
    // mock trim
    partitionDataWriter.flush();
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 2);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 2048);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(1) - reduceFileMeta.getChunkOffsets().get(0), 1024);

    // case 6: write 2049B
    PartitionDataWriterContext context7 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context7),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context7,
            PartitionType.REDUCE);
    for (int i = 0; i < 16; i++) {
      partitionDataWriter.write(generateData(128));
    }
    partitionDataWriter.write(generateData(1));
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 3);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 2049);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(2) - reduceFileMeta.getChunkOffsets().get(1), 1024);

    // case 7: write 4097B with 3 chunks
    PartitionDataWriterContext context8 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context8),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context8,
            PartitionType.REDUCE);
    partitionDataWriter.write(generateData(1024));
    for (int i = 0; i < 9; i++) {
      partitionDataWriter.write(generateData(128));
    }
    partitionDataWriter.write(generateData(1920));
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 3);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 4096);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(3) - reduceFileMeta.getChunkOffsets().get(2), 2048);

    // case 7.2: write 4097B with 3 chunks with trim; without PR #1702 this case will fail
    PartitionDataWriterContext context9 =
        new PartitionDataWriterContext(
            SPLIT_THRESHOLD,
            splitMode,
            false,
            new PartitionLocation(
                1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY, null),
            "app1-1",
            1,
            userIdentifier,
            PartitionType.REDUCE,
            false,
            false);
    partitionDataWriter =
        new PartitionDataWriter(
            PartitionDataWriterSuiteUtils.prepareDiskFileTestEnvironment(
                tempDir, userIdentifier, localFlusher, true, conf, storagePolicy, context9),
            conf,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            context9,
            PartitionType.REDUCE);
    partitionDataWriter.write(generateData(1024));
    for (int i = 0; i < 9; i++) {
      partitionDataWriter.write(generateData(128));
      partitionDataWriter.flush();
    }
    partitionDataWriter.write(generateData(1920));
    // mock trim
    partitionDataWriter.flush();
    partitionDataWriter.close();
    reduceFileMeta = (ReduceFileMeta) partitionDataWriter.getDiskFileInfo().getFileMeta();
    assertEquals(reduceFileMeta.getNumChunks(), 3);
    assertEquals(reduceFileMeta.getLastChunkOffset(), 4096);
    assertEquals(
        reduceFileMeta.getChunkOffsets().get(3) - reduceFileMeta.getChunkOffsets().get(2), 2048);
  }

  private File getTemporaryFile() throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    temporaryFile.createNewFile();
    return temporaryFile;
  }

  private byte[] generateData() {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    byte[] hello = "hello, world".getBytes(StandardCharsets.UTF_8);
    int tempLen = rand.nextInt(256 * 1024) + 128 * 1024;
    int len = (int) (Math.ceil(1.0 * tempLen / hello.length) * hello.length);

    byte[] data = new byte[len];
    for (int i = 0; i < len; i += hello.length) {
      System.arraycopy(hello, 0, data, i, hello.length);
    }
    return data;
  }

  private ByteBuf generateData(int len) {
    byte[] data = new byte[len];
    Arrays.fill(data, (byte) 'a');
    return Unpooled.wrappedBuffer(data);
  }
}
