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

package org.apache.celeborn.client.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.util.ShuffleBlockInfoUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;

public class DfsPartitionReader implements PartitionReader {
  private static Logger logger = LoggerFactory.getLogger(DfsPartitionReader.class);
  private CelebornConf conf;
  PartitionLocation location;
  private final long shuffleChunkSize;
  private final int fetchMaxReqsInFlight;
  private final LinkedBlockingQueue<Pair<Integer, ByteBuf>> results;
  private final AtomicReference<Exception> exception = new AtomicReference<>();
  private volatile boolean closed = false;
  private ExecutorService fetchExecutor;
  private boolean fetchThreadStarted;
  private FSDataInputStream dfsInputStream;
  private int numChunks = 0;
  private int lastReturnedChunkId = -1;
  private int returnedChunks = 0;
  private int startChunkIndex;
  private int endChunkIndex;
  private final List<Long> chunkOffsets = new ArrayList<>();
  private TransportClient client;
  private PbStreamHandler streamHandler;
  private MetricsCallback metricsCallback;
  private long partitionReaderWaitLogThreshold;
  private FileSystem hadoopFs;
  private Path dataFilePath;
  private Optional<PartitionReaderCheckpointMetadata> partitionReaderCheckpointMetadata;
  private int threadNum;
  private String shuffleKey;

  public DfsPartitionReader(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      PbStreamHandler pbStreamHandler,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex,
      MetricsCallback metricsCallback,
      int startChunkIndex,
      int endChunkIndex,
      Optional<PartitionReaderCheckpointMetadata> checkpointMetadata)
      throws IOException {
    this.shuffleKey = shuffleKey;
    this.conf = conf;
    shuffleChunkSize = conf.dfsReadChunkSize();
    fetchMaxReqsInFlight = conf.clientFetchMaxReqsInFlight();
    results = new LinkedBlockingQueue<>();

    this.metricsCallback = metricsCallback;
    this.partitionReaderWaitLogThreshold = conf.clientPartitionReaderWaitLogThreshold();
    this.location = location;
    if (location.getStorageInfo() != null
        && location.getStorageInfo().getType() == StorageInfo.Type.S3) {
      this.hadoopFs = ShuffleClient.getHadoopFs(conf).get(StorageInfo.Type.S3);
    } else if (location.getStorageInfo() != null
        && location.getStorageInfo().getType() == StorageInfo.Type.OSS) {
      this.hadoopFs = ShuffleClient.getHadoopFs(conf).get(StorageInfo.Type.OSS);
    } else {
      this.hadoopFs = ShuffleClient.getHadoopFs(conf).get(StorageInfo.Type.HDFS);
    }

    long fetchTimeoutMs = conf.clientFetchTimeoutMs();
    try {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());
      if (pbStreamHandler == null) {
        TransportMessage openStream =
            new TransportMessage(
                MessageType.OPEN_STREAM,
                PbOpenStream.newBuilder()
                    .setShuffleKey(shuffleKey)
                    .setFileName(location.getFileName())
                    .setStartIndex(startMapIndex)
                    .setEndIndex(endMapIndex)
                    .build()
                    .toByteArray());
        ByteBuffer response = client.sendRpcSync(openStream.toByteBuffer(), fetchTimeoutMs);
        streamHandler = TransportMessage.fromByteBuffer(response).getParsedPayload();
        // Parse this message to ensure sort is done.
      } else {
        streamHandler = pbStreamHandler;
      }
    } catch (IOException | InterruptedException e) {
      throw new IOException(
          "read shuffle file from DFS failed, filePath: " + location.getStorageInfo().getFilePath(),
          e);
    }
    if (endMapIndex != Integer.MAX_VALUE && endMapIndex != -1) {
      dataFilePath = new Path(Utils.getSortedFilePath(location.getStorageInfo().getFilePath()));
      dfsInputStream = hadoopFs.open(dataFilePath);
      chunkOffsets.addAll(
          getChunkOffsetsFromSortedIndex(conf, location, startMapIndex, endMapIndex));
    } else {
      dataFilePath = new Path(location.getStorageInfo().getFilePath());
      dfsInputStream = hadoopFs.open(dataFilePath);
      chunkOffsets.addAll(getChunkOffsetsFromUnsortedIndex(location));
    }
    this.startChunkIndex = startChunkIndex == -1 ? 0 : startChunkIndex;
    this.endChunkIndex =
        endChunkIndex == -1
            ? chunkOffsets.size() - 2
            : Math.min(chunkOffsets.size() - 2, endChunkIndex);
    this.numChunks = this.endChunkIndex - this.startChunkIndex + 1;

    if (checkpointMetadata.isPresent()) {
      this.partitionReaderCheckpointMetadata = checkpointMetadata;
      this.returnedChunks = checkpointMetadata.get().getReturnedChunks().size();
    } else {
      this.partitionReaderCheckpointMetadata =
          conf.isPartitionReaderCheckpointEnabled()
              ? Optional.of(new PartitionReaderCheckpointMetadata())
              : Optional.empty();
    }
    logger.debug(
        "DFS {} total offset count:{} chunk count: {} "
            + "start chunk index:{} end chunk index:{} offsets:{}",
        location.getStorageInfo().getFilePath(),
        chunkOffsets.size(),
        this.numChunks,
        this.startChunkIndex,
        this.endChunkIndex,
        chunkOffsets);
    if (this.numChunks > 0) {
      this.threadNum = Math.min(numChunks, conf.clientDfsFetchExecutorThreads());
      this.fetchExecutor =
          ThreadUtils.newDaemonFixedThreadPool(
              threadNum,
              "celeborn-client-dfs-partition-fetcher" + location.getStorageInfo().getFilePath());
      logger.debug("Start dfs read on location {}", location);
      ShuffleClient.incrementTotalReadCounter();
    }
  }

  private void fetchChunks() {
    try {
      for (int i = 0; i < threadNum; i++) {
        final int startChunkIndex = (chunkOffsets.size() / threadNum) * i;
        final int endChunkIndex =
            (i == threadNum - 1)
                ? chunkOffsets.size() - 1
                : chunkOffsets.size() / threadNum * (i + 1);
        final int threadIndex = i;
        Thread fetchThread =
            new Thread(
                () -> {
                  try {
                    FSDataInputStream hdfsInputStream = hadoopFs.open(dataFilePath);
                    for (int j = startChunkIndex; j < endChunkIndex; j++) {
                      if (partitionReaderCheckpointMetadata.isPresent()
                          && partitionReaderCheckpointMetadata.get().isCheckpointed(j)) {
                        logger.info(
                            "Skipping chunk {} as it has already been returned,"
                                + " likely by a previous reader for the same partition.",
                            j);
                        continue;
                      }
                      while (results.size() >= fetchMaxReqsInFlight) {
                        Thread.sleep(50);
                      }
                      long offset = chunkOffsets.get(j);
                      long length = chunkOffsets.get(j + 1) - offset;
                      logger.debug(
                          "shuffleKey:{}, uniqueId:{}, startChunkIndex:{}, endChunkIndex:{}, threadIndex:{} read{}, offset:{}, length:{}",
                          shuffleKey,
                          location.getUniqueId(),
                          startChunkIndex,
                          endChunkIndex,
                          threadIndex,
                          j,
                          offset,
                          length);
                      byte[] buffer = new byte[(int) length];
                      try {
                        hdfsInputStream.readFully(offset, buffer);
                      } catch (IOException e) {
                        logger.warn(
                            "read HDFS {} failed will retry, error detail {}",
                            location.getStorageInfo().getFilePath(),
                            e);
                        try {
                          if (hdfsInputStream != null) {
                            hdfsInputStream.close();
                          }
                          hdfsInputStream = hadoopFs.open(dataFilePath);
                          hdfsInputStream.readFully(offset, buffer);
                        } catch (IOException ex) {
                          logger.warn(
                              "retry read HDFS {} failed, error detail {} ",
                              location.getStorageInfo().getFilePath(),
                              e);
                          exception.set(ex);
                          return;
                        }
                      } finally {
                        if (hdfsInputStream != null) {
                          try {
                            hdfsInputStream.close();
                          } catch (IOException e) {
                            logger.error(
                                "Retry failed for reading DFS {} at offset {}",
                                location.getStorageInfo().getFilePath(),
                                offset,
                                e);
                            exception.set(e);
                          }
                        }
                      }
                      results.put(Pair.of(j, Unpooled.wrappedBuffer(buffer)));
                      logger.debug("add index {} to results", j);
                    }
                  } catch (Exception t) {
                    logger.warn("Fetch thread is cancelled.", t);
                    // cancel a task for speculative, ignore this exception
                    exception.set(t);
                  }
                  logger.debug(
                      "startChunkIndex {} endChunkIndex {} fetch {} is done.",
                      startChunkIndex,
                      endChunkIndex,
                      location.getStorageInfo().getFilePath());
                },
                "Dfs-fetch-thread-" + threadIndex + "-" + location.getStorageInfo().getFilePath());
        fetchExecutor.submit(fetchThread);
      }
    } catch (Exception t) {
      logger.warn("Fetch thread is cancelled.", t);
      // cancel a task for speculative, ignore this exception
      exception.set(t);
    }
  }

  private List<Long> getChunkOffsetsFromUnsortedIndex(PartitionLocation location)
      throws IOException {
    List<Long> offsets;
    String indexPath = Utils.getIndexFilePath(location.getStorageInfo().getFilePath());
    try (FSDataInputStream indexInputStream = hadoopFs.open(new Path(indexPath))) {
      long indexSize = hadoopFs.getFileStatus(new Path(indexPath)).getLen();
      byte[] indexBuffer = new byte[(int) indexSize];
      indexInputStream.readFully(0L, indexBuffer);
      ByteBuffer buffer = ByteBuffer.wrap(indexBuffer);
      int offsetSize = buffer.getInt();
      offsets = new ArrayList<>(offsetSize);
      for (int i = 0; i < offsetSize; i++) {
        offsets.add(buffer.getLong());
      }
    }
    return offsets;
  }

  private List<Long> getChunkOffsetsFromSortedIndex(
      CelebornConf conf, PartitionLocation location, int startMapIndex, int endMapIndex)
      throws IOException {
    String indexPath = Utils.getIndexFilePath(location.getStorageInfo().getFilePath());
    List<Long> offsets;
    try (FSDataInputStream indexInputStream = hadoopFs.open(new Path(indexPath))) {
      logger.debug("read sorted index {}", indexPath);
      long indexSize = hadoopFs.getFileStatus(new Path(indexPath)).getLen();
      // Index size won't be large, so it's safe to do the conversion.
      byte[] indexBuffer = new byte[(int) indexSize];
      indexInputStream.readFully(0L, indexBuffer);
      offsets =
          new ArrayList<>(
              ShuffleBlockInfoUtils.getChunkOffsetsFromShuffleBlockInfos(
                  startMapIndex,
                  endMapIndex,
                  shuffleChunkSize,
                  ShuffleBlockInfoUtils.parseShuffleBlockInfosFromByteBuffer(indexBuffer),
                  false));
    }
    return offsets;
  }

  @Override
  public boolean hasNext() {
    logger.debug("check has next current index: {} chunks {}", returnedChunks, numChunks);
    return returnedChunks < numChunks;
  }

  private void checkpoint() {
    if (lastReturnedChunkId != -1) {
      partitionReaderCheckpointMetadata.ifPresent(
          readerCheckpointMetadata -> readerCheckpointMetadata.checkpoint(lastReturnedChunkId));
    }
  }

  @Override
  public ByteBuf next() throws Exception {
    Pair<Integer, ByteBuf> chunk = null;
    checkpoint();
    if (!fetchThreadStarted) {
      fetchThreadStarted = true;
      fetchChunks();
    }
    try {
      long totalWaitTimeMs = 0;
      long lastLogTimeMs = 0;

      while (chunk == null) {
        checkException();
        Long startFetchWait = System.nanoTime();
        chunk = results.poll(500, TimeUnit.MILLISECONDS);
        long waitTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait);
        metricsCallback.incReadTime(waitTimeMs);
        totalWaitTimeMs += waitTimeMs;
        // Log when wait time exceeds another threshold since last log
        if (chunk == null && totalWaitTimeMs >= lastLogTimeMs + partitionReaderWaitLogThreshold) {
          lastLogTimeMs = totalWaitTimeMs;
          logger.info(
              "Waiting for data from partition {}/{} for {}ms",
              location.getFileName(),
              location.hostAndPorts(),
              totalWaitTimeMs);
        }

        logger.debug("poll result with result size: {}", results.size());
      }

      if (totalWaitTimeMs >= partitionReaderWaitLogThreshold) {
        logger.info(
            "Finished waiting for data from partition {}/{} after {}ms",
            location.getFileName(),
            location.hostAndPorts(),
            totalWaitTimeMs);
      }
    } catch (Exception e) {
      logger.error("PartitionReader thread interrupted while fetching data.");
      throw e;
    }
    returnedChunks++;
    lastReturnedChunkId = chunk.getLeft();
    return chunk.getRight();
  }

  private void checkException() throws Exception {
    Exception e = exception.get();
    if (e != null) {
      throw e;
    }
  }

  @Override
  public void close() {
    closed = true;
    if (fetchExecutor != null) {
      fetchExecutor.shutdownNow();
    }
    try {
      dfsInputStream.close();
    } catch (IOException e) {
      logger.warn("close DFS input stream failed.", e);
    }
    if (results.size() > 0) {
      results.forEach(chunk -> chunk.getRight().release());
    }
    results.clear();
    closeStream();
  }

  private void closeStream() {
    if (client != null && client.isActive()) {
      TransportMessage bufferStreamEnd =
          new TransportMessage(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamType(StreamType.ChunkStream)
                  .setStreamId(streamHandler.getStreamId())
                  .build()
                  .toByteArray());
      client.sendRpc(bufferStreamEnd.toByteBuffer());
    }
  }

  @Override
  public PartitionLocation getLocation() {
    return location;
  }

  @Override
  public Optional<PartitionReaderCheckpointMetadata> getPartitionReaderCheckpointMetadata() {
    return partitionReaderCheckpointMetadata;
  }
}
