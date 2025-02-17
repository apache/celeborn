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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
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
  private final LinkedBlockingQueue<ByteBuf> results;
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private volatile boolean closed = false;
  private ExecutorService fetchThread;
  private boolean fetchThreadStarted;
  private FSDataInputStream dfsInputStream;
  private int numChunks = 0;
  private int returnedChunks = 0;
  private int currentChunkIndex = 0;
  private final List<Long> chunkOffsets = new ArrayList<>();
  private TransportClient client;
  private PbStreamHandler streamHandler;
  private MetricsCallback metricsCallback;
  private FileSystem hadoopFs;

  public DfsPartitionReader(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      PbStreamHandler pbStreamHandler,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex,
      MetricsCallback metricsCallback)
      throws IOException {
    this.conf = conf;
    shuffleChunkSize = conf.dfsReadChunkSize();
    fetchMaxReqsInFlight = conf.clientFetchMaxReqsInFlight();
    results = new LinkedBlockingQueue<>();

    this.metricsCallback = metricsCallback;
    this.location = location;
    if (location.getStorageInfo() != null
        && location.getStorageInfo().getType() == StorageInfo.Type.S3) {
      this.hadoopFs = ShuffleClient.getHadoopFs(conf).get(StorageInfo.Type.S3);
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

    if (endMapIndex != Integer.MAX_VALUE) {
      dfsInputStream =
          hadoopFs.open(new Path(Utils.getSortedFilePath(location.getStorageInfo().getFilePath())));
      chunkOffsets.addAll(
          getChunkOffsetsFromSortedIndex(conf, location, startMapIndex, endMapIndex));
    } else {
      dfsInputStream = hadoopFs.open(new Path(location.getStorageInfo().getFilePath()));
      chunkOffsets.addAll(getChunkOffsetsFromUnsortedIndex(conf, location));
    }
    logger.debug(
        "DFS {} index count:{} offsets:{}",
        location.getStorageInfo().getFilePath(),
        chunkOffsets.size(),
        chunkOffsets);
    if (chunkOffsets.size() > 1) {
      numChunks = chunkOffsets.size() - 1;
      fetchThread =
          ThreadUtils.newDaemonSingleThreadExecutor(
              "celeborn-client-dfs-partition-fetcher" + location.getStorageInfo().getFilePath());
      logger.debug("Start dfs read on location {}", location);
      ShuffleClient.incrementTotalReadCounter();
    }
  }

  private List<Long> getChunkOffsetsFromUnsortedIndex(CelebornConf conf, PartitionLocation location)
      throws IOException {
    List<Long> offsets;
    try (FSDataInputStream indexInputStream =
        hadoopFs.open(new Path(Utils.getIndexFilePath(location.getStorageInfo().getFilePath())))) {
      offsets = new ArrayList<>();
      int offsetCount = indexInputStream.readInt();
      for (int i = 0; i < offsetCount; i++) {
        offsets.add(indexInputStream.readLong());
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

  @Override
  public ByteBuf next() throws IOException, InterruptedException {
    ByteBuf chunk = null;
    if (!fetchThreadStarted) {
      fetchThreadStarted = true;
      fetchThread.submit(
          () -> {
            try {
              while (!closed && currentChunkIndex < numChunks) {
                while (results.size() >= fetchMaxReqsInFlight) {
                  Thread.sleep(50);
                }
                long offset = chunkOffsets.get(currentChunkIndex);
                long length = chunkOffsets.get(currentChunkIndex + 1) - offset;
                logger.debug("read {} offset {} length {}", currentChunkIndex, offset, length);
                byte[] buffer = new byte[(int) length];
                try {
                  dfsInputStream.readFully(offset, buffer);
                } catch (IOException e) {
                  logger.warn(
                      "read DFS {} failed will retry, error detail {}",
                      location.getStorageInfo().getFilePath(),
                      e);
                  try {
                    dfsInputStream.close();
                    dfsInputStream =
                        hadoopFs.open(
                            new Path(
                                Utils.getSortedFilePath(location.getStorageInfo().getFilePath())));
                    dfsInputStream.readFully(offset, buffer);
                  } catch (IOException ex) {
                    logger.warn(
                        "retry read DFS {} failed, error detail {} ",
                        location.getStorageInfo().getFilePath(),
                        e);
                    exception.set(ex);
                    break;
                  }
                }
                results.put(Unpooled.wrappedBuffer(buffer));
                logger.debug("add index {} to results", currentChunkIndex++);
              }
            } catch (Exception e) {
              logger.warn("Fetch thread is cancelled.", e);
              // cancel a task for speculative, ignore this exception
            }
            logger.debug("fetch {} is done.", location.getStorageInfo().getFilePath());
          });
    }
    try {
      while (chunk == null) {
        checkException();
        Long startFetchWait = System.nanoTime();
        chunk = results.poll(500, TimeUnit.MILLISECONDS);
        metricsCallback.incReadTime(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait));
        logger.debug("poll result with result size: {}", results.size());
      }
    } catch (InterruptedException e) {
      logger.error("PartitionReader thread interrupted while fetching data.");
      throw e;
    }
    returnedChunks++;
    return chunk;
  }

  private void checkException() throws IOException {
    IOException e = exception.get();
    if (e != null) {
      throw e;
    }
  }

  @Override
  public void close() {
    closed = true;
    if (fetchThread != null) {
      fetchThread.shutdownNow();
    }
    try {
      dfsInputStream.close();
    } catch (IOException e) {
      logger.warn("close DFS input stream failed.", e);
    }
    if (results.size() > 0) {
      results.forEach(ReferenceCounted::release);
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
}
