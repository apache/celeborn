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
import org.apache.celeborn.common.protocol.PbCoalescedStreamHandler;
import org.apache.celeborn.common.protocol.StreamType;

public class SharedCoalescedStream {
  private final String shuffleKey;
  private final PartitionLocation location;
  private final PbCoalescedStreamHandler handler;
  private final long fetchTimeoutMs;
  private final TransportClientFactory clientFactory;
  private final LinkedBlockingQueue<Pair<Integer, ByteBuf>> results = new LinkedBlockingQueue<>();
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private TransportClient client;
  private ByteBuf currentChunk;
  private int currentChunkIndex = -1;
  private boolean closed;

  public SharedCoalescedStream(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      PbCoalescedStreamHandler handler,
      TransportClientFactory clientFactory)
      throws IOException, InterruptedException {
    this.shuffleKey = shuffleKey;
    this.location = location;
    this.handler = handler;
    this.fetchTimeoutMs = conf.clientFetchTimeoutMs();
    this.clientFactory = clientFactory;
    this.client = clientFactory.createClient(location.getHost(), location.getFetchPort());
  }

  public synchronized ByteBuf getChunk(int chunkIndex) throws IOException, InterruptedException {
    if (closed) {
      throw new CelebornIOException("Coalesced stream is already closed");
    }
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
    exception.set(null);
    if (!client.isActive()) {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());
    }
    ChunkReceivedCallback callback =
        new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int returnedChunkIndex, ManagedBuffer buffer) {
            ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
            synchronized (SharedCoalescedStream.this) {
              if (!closed) {
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
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(fetchTimeoutMs);
    while (currentChunk == null) {
      IOException fetchException = exception.get();
      if (fetchException != null) {
        throw fetchException;
      }
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        throw new CelebornIOException(
            "Timed out fetching coalesced chunk " + chunkIndex + " for " + shuffleKey);
      }
      Pair<Integer, ByteBuf> chunk =
          results.poll(
              Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNanos), 500), TimeUnit.MILLISECONDS);
      if (chunk != null) {
        currentChunkIndex = chunk.getLeft();
        currentChunk = chunk.getRight();
      }
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
    results.forEach(chunk -> chunk.getRight().release());
    results.clear();
    if (client != null && client.isActive()) {
      TransportMessage bufferStreamEnd =
          new TransportMessage(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamType(StreamType.ChunkStream)
                  .setStreamId(handler.getStreamId())
                  .build()
                  .toByteArray());
      client.sendRpc(bufferStreamEnd.toByteBuffer());
    }
  }
}
