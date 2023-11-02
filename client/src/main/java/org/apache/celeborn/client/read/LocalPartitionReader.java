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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.common.util.ThreadUtils;

public class LocalPartitionReader implements PartitionReader {

  private static final Logger logger = LoggerFactory.getLogger(LocalPartitionReader.class);
  private static volatile ThreadPoolExecutor readLocalShufflePool;
  private final LinkedBlockingQueue<ByteBuf> results;
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private final int fetchMaxReqsInFlight;
  private final PartitionLocation location;
  private volatile boolean closed = false;
  private final int numChunks;
  private int returnedChunks = 0;
  private int chunkIndex = 0;
  private String fullPath;
  private boolean mapRangeRead = false;
  private FileChannel shuffleChannel;
  private List<Long> chunkOffsets;
  private AtomicBoolean pendingFetchTask = new AtomicBoolean(false);
  private MetricsCallback metricsCallback;

  public LocalPartitionReader(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex,
      MetricsCallback metricsCallback)
      throws IOException {
    if (readLocalShufflePool == null) {
      synchronized (LocalPartitionReader.class) {
        if (readLocalShufflePool == null) {
          readLocalShufflePool =
              ThreadUtils.newDaemonCachedThreadPool(
                  "local-shuffle-reader-thread", conf.readLocalShuffleThreads(), 60);
        }
      }
    }
    fetchMaxReqsInFlight = conf.clientFetchMaxReqsInFlight();
    results = new LinkedBlockingQueue<>();
    this.location = location;
    this.metricsCallback = metricsCallback;
    PbStreamHandler streamHandle;
    long fetchTimeoutMs = conf.clientFetchTimeoutMs();
    try {
      TransportClient client =
          clientFactory.createClient(location.getHost(), location.getFetchPort(), 0);
      TransportMessage openStreamMsg =
          new TransportMessage(
              MessageType.OPEN_STREAM,
              PbOpenStream.newBuilder()
                  .setShuffleKey(shuffleKey)
                  .setFileName(location.getFileName())
                  .setStartIndex(startMapIndex)
                  .setEndIndex(endMapIndex)
                  .setReadLocalShuffle(true)
                  .build()
                  .toByteArray());
      ByteBuffer response = client.sendRpcSync(openStreamMsg.toByteBuffer(), fetchTimeoutMs);
      streamHandle = TransportMessage.fromByteBuffer(response).getParsedPayload();
    } catch (IOException | InterruptedException e) {
      throw new IOException(
          "Read shuffle file from local file failed, partition location: "
              + location
              + " filePath: "
              + location.getStorageInfo().getFilePath(),
          e);
    }

    chunkOffsets = new ArrayList<>(streamHandle.getChunkOffsetsList());
    numChunks = streamHandle.getNumChunks();
    fullPath = streamHandle.getFullPath();
    mapRangeRead = endMapIndex != Integer.MAX_VALUE;

    logger.debug(
        "Local partition reader {} offsets:{}",
        location.getStorageInfo().getFilePath(),
        StringUtils.join(chunkOffsets, ","));

    ShuffleClient.incrementLocalReadCounter();
  }

  private void doFetchChunks(int chunkIndex, int toFetch) {
    try {
      if (shuffleChannel == null) {
        shuffleChannel = FileChannelUtils.openReadableFileChannel(fullPath);
        if (mapRangeRead) {
          shuffleChannel.position(chunkOffsets.get(0));
        }
      }
      for (int i = 0; i < toFetch; i++) {
        long offset = chunkOffsets.get(chunkIndex + i);
        long length = chunkOffsets.get(chunkIndex + i + 1) - offset;
        logger.debug("Read {} offset {} length {}", chunkIndex, offset, length);
        // A chunk must be smaller than INT.MAX_VALUE
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        while (buffer.hasRemaining()) {
          if (-1 == shuffleChannel.read(buffer)) {
            throw new CelebornIOException(
                "Read local file " + location.getStorageInfo().getFilePath() + " failed");
          }
        }
        buffer.flip();
        // Avoid resource leak
        synchronized (this) {
          if (!closed) {
            results.put(Unpooled.wrappedBuffer(buffer));
            logger.debug("Add index {} to results", chunkIndex + i);
          }
        }
      }
    } catch (InterruptedException e) {
      // cancel a task for speculative, ignore this exception
      logger.warn("Read thread is interrupted.", e);
    } catch (Exception ioe) {
      logger.error("Read thread encountered error.", ioe);
      if (ioe instanceof CelebornIOException) {
        exception.set((IOException) ioe);
      } else {
        exception.set(new CelebornIOException("Read thread encountered error", ioe));
      }
    }
    pendingFetchTask.compareAndSet(true, false);
  }

  private void fetchChunks() {
    int inFlight = chunkIndex - returnedChunks;
    if (inFlight < fetchMaxReqsInFlight) {
      int toFetch = Math.min(fetchMaxReqsInFlight - inFlight + 1, numChunks - chunkIndex);
      if (pendingFetchTask.compareAndSet(false, true)) {
        logger.debug(
            "Trigger local reader fetch chunk with {} and fetch {} chunks", chunkIndex, toFetch);
        int currentIndex = chunkIndex;
        readLocalShufflePool.submit(() -> doFetchChunks(currentIndex, toFetch));
        chunkIndex += toFetch;
      }
    }
  }

  @Override
  public boolean hasNext() {
    logger.debug("Check has next current index: {} chunks {}", returnedChunks, numChunks);
    return returnedChunks < numChunks;
  }

  @Override
  public ByteBuf next() throws IOException, InterruptedException {
    checkException();
    if (chunkIndex < numChunks) {
      fetchChunks();
    }
    ByteBuf chunk = null;
    try {
      while (chunk == null) {
        checkException();
        Long startFetchWait = System.nanoTime();
        chunk = results.poll(100, TimeUnit.MILLISECONDS);
        metricsCallback.incReadTime(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait));
        logger.debug("Poll result with result size: {}", results.size());
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
    synchronized (this) {
      closed = true;
      if (!results.isEmpty()) {
        results.forEach(ReferenceCounted::release);
      }
      results.clear();
    }
    try {
      if (shuffleChannel != null) {
        shuffleChannel.close();
      }
    } catch (IOException e) {
      logger.warn("Close local shuffle file failed.", e);
    }
  }

  @Override
  public PartitionLocation getLocation() {
    return location;
  }
}
