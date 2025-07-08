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
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.util.ExceptionUtils;

public class WorkerPartitionReader implements PartitionReader {
  private final Logger logger = LoggerFactory.getLogger(WorkerPartitionReader.class);
  private PartitionLocation location;
  private final TransportClientFactory clientFactory;
  private PbStreamHandler streamHandler;
  private TransportClient client;
  private MetricsCallback metricsCallback;

  private int lastReturnedChunkId = -1;
  private int returnedChunks;
  private int chunkIndex;
  private int startChunkIndex;
  private int endChunkIndex;

  private int inflightRequestCount;
  private final LinkedBlockingQueue<Pair<Integer, ByteBuf>> results;
  private final ChunkReceivedCallback callback;

  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private final String shuffleKey;
  private final int fetchMaxReqsInFlight;
  private final long fetchTimeoutMs;
  private boolean closed = false;

  // for test
  private int fetchChunkRetryCnt;
  private int fetchChunkMaxRetry;
  private final boolean testFetch;
  private Long readNonPartitionWaitTime;

  private Optional<PartitionReaderCheckpointMetadata> partitionReaderCheckpointMetadata;

  WorkerPartitionReader(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      PbStreamHandler pbStreamHandler,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex,
      int fetchChunkRetryCnt,
      int fetchChunkMaxRetry,
      MetricsCallback metricsCallback,
      int startChunkIndex,
      int endChunkIndex,
      Optional<PartitionReaderCheckpointMetadata> checkpointMetadata)
      throws IOException, InterruptedException {
    this.shuffleKey = shuffleKey;
    fetchMaxReqsInFlight = conf.clientFetchMaxReqsInFlight();
    results = new LinkedBlockingQueue<>();
    fetchTimeoutMs = conf.clientFetchTimeoutMs();
    readNonPartitionWaitTime = conf.clientReadNonPartitionWaitTime();
    inflightRequestCount = 0;
    this.metricsCallback = metricsCallback;
    // only add the buffer to results queue if this reader is not closed.
    callback =
        new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            // only add the buffer to results queue if this reader is not closed.
            synchronized (this) {
              ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
              if (!closed) {
                buf.retain();
                results.add(Pair.of(chunkIndex, buf));
              }
            }
          }

          @Override
          public void onFailure(int chunkIndex, Throwable e) {
            String errorMsg =
                String.format("Fetch chunk %d of shuffle key %s failed.", chunkIndex, shuffleKey);
            logger.error(errorMsg, e);
            exception.set(new CelebornIOException(errorMsg, e));
          }
        };
    try {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());
    } catch (InterruptedException ie) {
      logger.error("PartitionReader thread interrupted while creating client.");
      throw ie;
    }

    if (pbStreamHandler == null) {
      TransportMessage openStreamMsg =
          new TransportMessage(
              MessageType.OPEN_STREAM,
              PbOpenStream.newBuilder()
                  .setShuffleKey(shuffleKey)
                  .setFileName(location.getFileName())
                  .setStartIndex(startMapIndex)
                  .setEndIndex(endMapIndex)
                  .build()
                  .toByteArray());
      ByteBuffer response = client.sendRpcSync(openStreamMsg.toByteBuffer(), fetchTimeoutMs);
      this.streamHandler = TransportMessage.fromByteBuffer(response).getParsedPayload();
    } else {
      this.streamHandler = pbStreamHandler;
    }
    this.startChunkIndex = startChunkIndex == -1 ? 0 : startChunkIndex;
    this.endChunkIndex =
        endChunkIndex == -1
            ? streamHandler.getNumChunks() - 1
            : Math.min(streamHandler.getNumChunks() - 1, endChunkIndex);
    this.chunkIndex = this.startChunkIndex;
    this.location = location;
    this.clientFactory = clientFactory;
    this.fetchChunkRetryCnt = fetchChunkRetryCnt;
    this.fetchChunkMaxRetry = fetchChunkMaxRetry;
    if (checkpointMetadata.isPresent()) {
      this.partitionReaderCheckpointMetadata = checkpointMetadata;
      this.returnedChunks = checkpointMetadata.get().getReturnedChunks().size();
    } else {
      this.partitionReaderCheckpointMetadata =
          conf.isPartitionReaderCheckpointEnabled()
              ? Optional.of(new PartitionReaderCheckpointMetadata())
              : Optional.empty();
    }
    testFetch = conf.testFetchFailure();
    ShuffleClient.incrementTotalReadCounter();
  }

  @Override
  public boolean hasNext() {
    return returnedChunks < endChunkIndex - startChunkIndex + 1;
  }

  private void checkpoint() {
    if (lastReturnedChunkId != -1) {
      partitionReaderCheckpointMetadata.ifPresent(
          readerCheckpointMetadata -> readerCheckpointMetadata.checkpoint(lastReturnedChunkId));
    }
  }

  @Override
  public ByteBuf next() throws IOException, InterruptedException {
    checkpoint();
    checkException();
    if (chunkIndex <= endChunkIndex) {
      fetchChunks();
    }
    Pair<Integer, ByteBuf> chunk = null;
    try {
      while (chunk == null) {
        checkException();
        Long startFetchWait = System.nanoTime();
        chunk = results.poll(readNonPartitionWaitTime, TimeUnit.MILLISECONDS);
        metricsCallback.incReadTime(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait));
      }
    } catch (InterruptedException e) {
      logger.error("PartitionReader thread interrupted while polling data.");
      throw e;
    }
    returnedChunks++;
    inflightRequestCount--;
    lastReturnedChunkId = chunk.getLeft();
    return chunk.getRight();
  }

  @Override
  public void close() {
    synchronized (this) {
      closed = true;
    }
    if (results.size() > 0) {
      results.forEach(
          chunk -> {
            chunk.getRight().release();
          });
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

  private void fetchChunks() throws IOException, InterruptedException {
    final int inFlight = inflightRequestCount;
    if (inFlight < fetchMaxReqsInFlight) {
      int toFetch = Math.min(fetchMaxReqsInFlight - inFlight + 1, endChunkIndex + 1 - chunkIndex);

      while (toFetch > 0 && chunkIndex <= endChunkIndex) {
        if (partitionReaderCheckpointMetadata.isPresent()
            && partitionReaderCheckpointMetadata.get().isCheckpointed(chunkIndex)) {
          logger.info(
              "Skipping chunk {} as it has already been returned,"
                  + " likely by a previous reader for the same partition.",
              chunkIndex);
          chunkIndex++;
          // IMP Since we're skipping fetching this chunk, we must not decrement toFetch here
          // Eg: If chunkIndex=1, toFetch=2, endChunkIndex = 4 and chunkIdsAlreadyReturned = {1,2}
          // if we skip chunk 1 and 2, decrementing toFetch here would wrongly exit the loop
          // without ever fetching chunk {3, 4}, and next() would end up waiting for chunk {3,4}
          // infinitely.
        } else if (testFetch && fetchChunkRetryCnt < fetchChunkMaxRetry - 1 && chunkIndex == 3) {
          callback.onFailure(chunkIndex, new CelebornIOException("Test fetch chunk failure"));
          toFetch--;
        } else {
          if (!client.isActive()) {
            try {
              client = clientFactory.createClient(location.getHost(), location.getFetchPort());
            } catch (IOException e) {
              logger.error(
                  "FetchChunk for shuffleKey: {}, streamId: {}, chunkIndex: {} failed.",
                  shuffleKey,
                  streamHandler.getStreamId(),
                  chunkIndex,
                  e);
              ExceptionUtils.wrapAndThrowIOException(e);
            } catch (InterruptedException e) {
              logger.error("PartitionReader thread interrupted while fetching chunks.");
              throw e;
            }
          }
          client.fetchChunk(streamHandler.getStreamId(), chunkIndex, fetchTimeoutMs, callback);
          inflightRequestCount++;
          chunkIndex++;
          toFetch--;
        }
      }
    }
  }

  private void checkException() throws IOException {
    IOException e = exception.get();
    if (e != null) {
      throw e;
    }
  }
}
