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

package org.apache.celeborn.common.network.server;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileManagedBuffers;
import org.apache.celeborn.common.meta.TimeWindow;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 */
public class ChunkStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(ChunkStreamManager.class);

  private final AtomicLong nextStreamId;
  protected final ConcurrentHashMap<Long, StreamState> streams;

  /** State of a single stream. */
  protected static class StreamState {
    final FileManagedBuffers buffers;

    // The channel associated to the stream
    final Channel associatedChannel;

    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    int curChunk = 0;
    final TimeWindow fetchTimeMetric;

    // Used to keep track of the number of chunks being transferred and not finished yet.
    volatile long chunksBeingTransferred = 0L;

    StreamState(FileManagedBuffers buffers, Channel channel, TimeWindow fetchTimeMetric) {
      this.buffers = Preconditions.checkNotNull(buffers);
      this.associatedChannel = channel;
      this.fetchTimeMetric = fetchTimeMetric;
    }
  }

  public ChunkStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }

  public ManagedBuffer getChunk(long streamId, int chunkIndex, int offset, int len) {
    StreamState state = streams.get(streamId);
    if (state == null) {
      throw new IllegalStateException(
          String.format(
              "Stream %s for chunk %s is not registered(Maybe removed).", streamId, chunkIndex));
    } else if (chunkIndex >= state.buffers.numChunks()) {
      throw new IllegalStateException(
          String.format("Requested chunk index beyond end %s", chunkIndex));
    }

    FileManagedBuffers buffers = state.buffers;
    if (buffers.hasAlreadyRead(chunkIndex)) {
      throw new IllegalStateException(
          String.format("Chunk %s for stream %s has already been read.", chunkIndex, streamId));
    }
    ManagedBuffer nextChunk = buffers.chunk(chunkIndex, offset, len);

    if (state.buffers.isFullyRead()) {
      // Normally, when all chunks are returned to the client, the stream should be removed here.
      // But if there is a switch on the client side, it will not go here at this time, so we need
      // to remove the stream when the connection is terminated, and release the unused buffer.
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }

    return nextChunk;
  }

  public TimeWindow getFetchTimeMetric(long streamId) {
    StreamState state = streams.get(streamId);
    if (state != null) {
      return state.fetchTimeMetric;
    } else {
      return null;
    }
  }

  public static String genStreamChunkId(long streamId, int chunkId) {
    return String.format("%d_%d", streamId, chunkId);
  }

  // Parse streamChunkId to be stream id and chunk id. This is used when fetch remote chunk as a
  // stream.
  public static Pair<Long, Integer> parseStreamChunkId(String streamChunkId) {
    String[] array = streamChunkId.split("_");
    assert array.length == 2 : "Stream id and chunk index should be specified.";
    long streamId = Long.parseLong(array[0]);
    int chunkIndex = Integer.parseInt(array[1]);
    return ImmutablePair.of(streamId, chunkIndex);
  }

  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry : streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streams.remove(entry.getKey());
      }
    }
  }

  public void chunkBeingSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred++;
    }
  }

  public void chunkSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred--;
    }
  }

  public long chunksBeingTransferred() {
    long sum = 0L;
    for (StreamState streamState : streams.values()) {
      sum += streamState.chunksBeingTransferred;
    }
    return sum;
  }

  /**
   * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
   * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
   * client connection is closed before the iterator is fully drained, then the remaining buffers
   * will all be release()'d.
   *
   * <p>If an app ID is provided, only callers who've authenticated with the given app ID will be
   * allowed to fetch from this stream.
   *
   * <p>This method also associates the stream with a single client connection, which is guaranteed
   * to be the only reader of the stream. Once the connection is closed, the stream will never be
   * used again, enabling cleanup by `connectionTerminated`.
   */
  public long registerStream(
      FileManagedBuffers buffers, Channel channel, TimeWindow fetchTimeMetric) {
    long myStreamId = nextStreamId.getAndIncrement();
    streams.put(myStreamId, new StreamState(buffers, channel, fetchTimeMetric));
    return myStreamId;
  }

  @VisibleForTesting
  public int numStreamStates() {
    return streams.size();
  }
}
