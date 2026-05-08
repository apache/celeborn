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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;

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
import org.apache.celeborn.common.protocol.PbCoalescedChunkBoundary;
import org.apache.celeborn.common.protocol.PbCoalescedStreamHandler;
import org.apache.celeborn.common.protocol.PbOpenCoalescedStream;
import org.apache.celeborn.common.protocol.StreamType;

public class SharedCoalescedStream {
  private final String shuffleKey;
  private final PartitionLocation location;
  // Keep the exact worker request so a failed shared stream can be reopened without switching
  // chunk coordinate systems underneath the per-reducer readers.
  private final PbOpenCoalescedStream reopenRequest;
  private final long fetchTimeoutMs;
  private final TransportClientFactory clientFactory;
  private final LinkedBlockingQueue<Pair<Integer, ByteBuf>> results = new LinkedBlockingQueue<>();
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private final AtomicLong generation = new AtomicLong();
  private PbCoalescedStreamHandler handler;
  private TransportClient client;
  private ByteBuf currentChunk;
  private int currentChunkIndex = -1;
  private volatile boolean closed;

  public SharedCoalescedStream(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      PbOpenCoalescedStream reopenRequest,
      PbCoalescedStreamHandler handler,
      TransportClientFactory clientFactory)
      throws IOException, InterruptedException {
    this.shuffleKey = shuffleKey;
    this.location = location;
    this.reopenRequest = reopenRequest;
    this.handler = handler;
    this.fetchTimeoutMs = conf.clientFetchTimeoutMs();
    this.clientFactory = clientFactory;
    this.client = clientFactory.createClient(location.getHost(), location.getFetchPort());
  }

  public synchronized ByteBuf getChunk(int chunkIndex) throws IOException, InterruptedException {
    if (closed) {
      throw new CelebornIOException("Coalesced stream is already closed");
    }
    // One shared worker stream backs several reducer readers. Spark consumes those reducers in the
    // same order used to build the coalesced request, so callers must only advance this stream.
    if (chunkIndex < currentChunkIndex) {
      throw new CelebornIOException(
          "Coalesced stream chunks must be consumed in order for " + shuffleKey);
    }
    while (currentChunkIndex < chunkIndex) {
      if (currentChunk != null) {
        currentChunk.release();
        currentChunk = null;
      }
      fetchChunk(currentChunkIndex + 1);
    }
    return currentChunk;
  }

  private void fetchChunk(int chunkIndex) throws IOException, InterruptedException {
    try {
      fetchChunkOnce(chunkIndex);
    } catch (IOException e) {
      reopenStream();
      fetchChunkOnce(chunkIndex);
    }
  }

  private void fetchChunkOnce(int chunkIndex) throws IOException, InterruptedException {
    exception.set(null);
    if (!client.isActive()) {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());
    }
    long fetchGeneration = generation.get();
    ChunkReceivedCallback callback =
        new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int returnedChunkIndex, ManagedBuffer buffer) {
            ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
            synchronized (results) {
              // A late response from the old worker stream is no longer valid after reopen.
              if (!closed && fetchGeneration == generation.get()) {
                buf.retain();
                results.add(Pair.of(returnedChunkIndex, buf));
              }
            }
          }

          @Override
          public void onFailure(int returnedChunkIndex, Throwable e) {
            exception.compareAndSet(
                null,
                new CelebornIOException(
                    "Fetch coalesced chunk " + returnedChunkIndex + " failed.", e));
          }
        };
    client.fetchChunk(handler.getStreamId(), chunkIndex, fetchTimeoutMs, callback);
    while (currentChunk == null) {
      IOException fetchException = exception.get();
      if (fetchException != null) {
        throw fetchException;
      }
      Pair<Integer, ByteBuf> chunk = results.poll(500, TimeUnit.MILLISECONDS);
      if (chunk != null) {
        currentChunkIndex = chunk.getLeft();
        currentChunk = chunk.getRight();
      }
    }
  }

  private void reopenStream() throws IOException, InterruptedException {
    long oldStreamId = handler.getStreamId();
    PbCoalescedStreamHandler reopenedHandler;
    try {
      if (!client.isActive()) {
        client = clientFactory.createClient(location.getHost(), location.getFetchPort());
      }
      TransportMessage reopenMessage =
          new TransportMessage(MessageType.OPEN_COALESCED_STREAM, reopenRequest.toByteArray());
      reopenedHandler =
          TransportMessage.fromByteBuffer(
                  client.sendRpcSync(reopenMessage.toByteBuffer(), fetchTimeoutMs))
              .getParsedPayload();
    } catch (Exception e) {
      throw new CelebornIOException("Failed to reopen coalesced stream for " + shuffleKey, e);
    }

    // Existing reducer readers and checkpoints are expressed in the original composite layout.
    // Reopen is only safe if the worker rebuilt that exact layout for the same immutable files.
    // Otherwise there is no safe mid-stream path switch; the task must fail and restart.
    if (!hasSameLayout(handler, reopenedHandler)) {
      closeStream(reopenedHandler.getStreamId());
      throw new CelebornIOException(
          "Reopened coalesced stream layout changed for shuffle " + shuffleKey);
    }

    generation.incrementAndGet();
    clearQueuedChunks();
    handler = reopenedHandler;
    closeStream(oldStreamId);
  }

  private boolean hasSameLayout(
      PbCoalescedStreamHandler expected, PbCoalescedStreamHandler actual) {
    if (expected.getNumChunks() != actual.getNumChunks()
        || expected.getBoundariesCount() != actual.getBoundariesCount()) {
      return false;
    }
    for (int idx = 0; idx < expected.getBoundariesCount(); idx++) {
      PbCoalescedChunkBoundary expectedBoundary = expected.getBoundaries(idx);
      PbCoalescedChunkBoundary actualBoundary = actual.getBoundaries(idx);
      if (!expectedBoundary.equals(actualBoundary)) {
        return false;
      }
    }
    return true;
  }

  private void clearQueuedChunks() {
    synchronized (results) {
      results.forEach(chunk -> chunk.getRight().release());
      results.clear();
    }
  }

  private void closeStream(long streamId) {
    if (client != null && client.isActive()) {
      TransportMessage bufferStreamEnd =
          new TransportMessage(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamType(StreamType.ChunkStream)
                  .setStreamId(streamId)
                  .build()
                  .toByteArray());
      client.sendRpc(bufferStreamEnd.toByteBuffer());
    }
  }

  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (currentChunk != null) {
      currentChunk.release();
      currentChunk = null;
    }
    synchronized (results) {
      results.forEach(chunk -> chunk.getRight().release());
      results.clear();
    }
    closeStream(handler.getStreamId());
  }
}
