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

package org.apache.celeborn.service.deploy.worker.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.network.buffer.FileChunkBuffers;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.service.deploy.worker.shuffledb.DB;
import org.apache.celeborn.service.deploy.worker.shuffledb.DBBackend;
import org.apache.celeborn.service.deploy.worker.shuffledb.DBProvider;
import org.apache.celeborn.service.deploy.worker.shuffledb.StoreVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.TimeWindow;
import org.apache.celeborn.common.network.buffer.ChunkBuffers;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 */
public class ChunkStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(ChunkStreamManager.class);
  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);
  private static final String RECOVERY_REGISTERED_STREAMS = "registeredStreams";

  private final AtomicLong nextStreamId;
  // StreamId -> StreamState
  protected final ConcurrentHashMap<Long, StreamState> streams;
  // ShuffleKey -> StreamId
  protected final ConcurrentHashMap<String, Set<Long>> shuffleStreamIds;
  private final CelebornConf conf;
  private File recoverFile;
  private DB registeredStreamsDb;

  /** State of a single stream. */
  public static class StreamState {
    public final ChunkBuffers buffers;
    public final String shuffleKey;
    public final String fileName;
    public final TimeWindow fetchTimeMetric;
    // Used to keep track of the number of chunks being transferred and not finished yet.
    volatile long chunksBeingTransferred = 0L;

    StreamState(
        String shuffleKey, ChunkBuffers buffers, String fileName, TimeWindow fetchTimeMetric) {
      this.buffers = buffers;
      this.shuffleKey = shuffleKey;
      this.fileName = fileName;
      this.fetchTimeMetric = fetchTimeMetric;
    }
  }

  public ChunkStreamManager(CelebornConf conf) {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = JavaUtils.newConcurrentHashMap();
    shuffleStreamIds = JavaUtils.newConcurrentHashMap();
    this.conf = conf;
    boolean gracefulShutdown = conf.workerGracefulShutdown();
    if (gracefulShutdown) {
      try {
        String recoverPath = conf.workerGracefulShutdownRecoverPath();
        DBBackend dbBackend = DBBackend.byName(conf.workerGracefulShutdownRecoverDbBackend());
        String recoveryRegisteredStreams = dbBackend.fileName(RECOVERY_REGISTERED_STREAMS);
        this.recoverFile = new File(recoverPath, recoveryRegisteredStreams);
        this.registeredStreamsDb = DBProvider.initDB(dbBackend, recoverFile, CURRENT_VERSION);
      } catch (Exception e) {
        throw new IllegalStateException(
                "Failed to reload DB for sorted shuffle files from: " + recoverFile, e);
      }
    }
  }

  public void init(StorageManager storageManager, TransportConf transportConf) {
    reloadRegisteredStreams(storageManager, transportConf);
  }

  private void reloadRegisteredStreams(StorageManager storageManager, TransportConf transportConf) {
    registeredStreamsDb.iterator().forEachRemaining(
        entry -> {
          try {
            long streamId = ByteBuffer.wrap(entry.getKey()).getLong();
            PbRegisteredStream pbRegisteredStream =
                    PbSerDeUtils.fromPbRegisteredStream(entry.getValue());
            String shuffleKey = pbRegisteredStream.getShuffleKey();
            String fileName = pbRegisteredStream.getFileName();
            Boolean isBufferBacked = pbRegisteredStream.getIsBufferBacked();

            ChunkBuffers buffers = null;
            TimeWindow fetchTimeMetric = null;
            Boolean isValidRestore = true;
            if (isBufferBacked) {
              DiskFileInfo diskFileInfo = (DiskFileInfo) storageManager.getFileInfo(shuffleKey, fileName);
              if (diskFileInfo != null) {
                buffers = new FileChunkBuffers(diskFileInfo, transportConf);
                fetchTimeMetric = storageManager.getFetchTimeMetric(diskFileInfo.getFile());
              } else {
                isValidRestore = false;
              }
            }

            if (isValidRestore) {
              registerStream(streamId, shuffleKey, buffers, fileName, fetchTimeMetric);
            }
          } catch (Exception e) {
            logger.error("Failed to reload registered stream from DB entry: " + entry, e);
          }
        });
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

    ChunkBuffers buffers = state.buffers;
    return buffers.chunk(chunkIndex, offset, len);
  }

  public TimeWindow getFetchTimeMetric(long streamId) {
    StreamState state = streams.get(streamId);
    if (state != null) {
      return state.fetchTimeMetric;
    } else {
      return null;
    }
  }

  public void chunkBeingSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      synchronized (streamState) {
        streamState.chunksBeingTransferred++;
      }
    }
  }

  public void chunkSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      synchronized (streamState) {
        streamState.chunksBeingTransferred--;
      }
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
   * Registers a stream with shuffle key and disk file when reading local or dfs shuffle, which is
   * served to obtain disk file via registered stream id to close stream.
   *
   * <p>This stream could be reused again when other channel of the client is reconnected. If a
   * stream is not properly closed, it will eventually be cleaned up by `cleanupExpiredShuffleKey`.
   */
  public long registerStream(long streamId, String shuffleKey, String fileName) {
    return registerStream(streamId, shuffleKey, null, fileName, null);
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
   * <p>This stream could be reused again when other channel of the client is reconnected. If a
   * stream is not properly closed, it will eventually be cleaned up by `cleanupExpiredShuffleKey`.
   */
  public long registerStream(
      String shuffleKey, ChunkBuffers buffers, String fileName, TimeWindow fetchTimeMetric) {
    long myStreamId = nextStreamId.getAndIncrement();
    return registerStream(myStreamId, shuffleKey, buffers, fileName, fetchTimeMetric);
  }

  public long registerStream(
      long streamId,
      String shuffleKey,
      ChunkBuffers buffers,
      String fileName,
      TimeWindow fetchTimeMetric) {
    StreamState streamState = new StreamState(shuffleKey, buffers, fileName, fetchTimeMetric);
    streams.put(streamId, streamState);
    shuffleStreamIds.compute(
        shuffleKey,
        (key, value) -> {
          if (value == null) {
            value = ConcurrentHashMap.newKeySet();
          }
          value.add(streamId);
          return value;
        });

    return streamId;
  }

  public long nextStreamId() {
    long currentId = nextStreamId.getAndIncrement();
    while (streams.containsKey(currentId)) {
      currentId = nextStreamId.getAndIncrement();;
    }

    return currentId;
  }

  public void cleanupExpiredShuffleKey(Set<String> expiredShuffleKeys) {
    logger.info(
        "Clean up expired shuffle keys {}",
        String.join(",", expiredShuffleKeys.toArray(new String[0])));
    for (String expiredShuffleKey : expiredShuffleKeys) {
      Set<Long> expiredStreamIds = shuffleStreamIds.remove(expiredShuffleKey);

      // normally expiredStreamIds set will be empty as streamId will be removed when be fully read
      if (expiredStreamIds != null && !expiredStreamIds.isEmpty()) {
        expiredStreamIds.forEach(streams::remove);
      }
    }
    logger.info(
        "Cleaned up expired shuffle keys. The count of shuffle keys and streams: {}, {}",
        shuffleStreamIds.size(),
        streams.size());
  }

  public StreamState getStreamState(long streamId) {
    return streams.get(streamId);
  }

  public StreamState removeStreamState(long streamId) {
    return streams.remove(streamId);
  }

  public int getStreamsCount() {
    return streams.size();
  }

  @VisibleForTesting
  public long numShuffleSteams() {
    return shuffleStreamIds.values().stream().mapToLong(Set::size).sum();
  }

  private void persisteRegisteredStreams() {
    streams.forEach(
        (streamId, streamState) -> {
          // Only need to persist FileChunkBuffers since MemoryChunkBuffers aren't restored anyways
          // Skipping sorted file info since these are not restored
          if (streamState.buffers == null || (streamState.buffers instanceof FileChunkBuffers &&
                  !((FileChunkBuffers) streamState.buffers).isSortedFileInfo())) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
            keyBuffer.putLong(0, streamId);
            keyBuffer.flip();
            try {
              registeredStreamsDb.put(keyBuffer.array(),
                      PbSerDeUtils.toPbRegisteredStream(streamState.shuffleKey, streamState.fileName,
                              streamState.buffers != null));
            } catch (Exception e) {
              logger.error("Failed to persist stream state for streamId: " + streamId, e);
            }
          }
        });
  }

  public void close(int exitKind) {
    logger.info("Closing {}", this.getClass().getSimpleName());
    if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN() && registeredStreamsDb != null) {
      try {
        persisteRegisteredStreams();
      } catch (Exception e) {
        logger.error("Failed to persist registered streams to DB: " + recoverFile, e);
      } finally {
        try {
          registeredStreamsDb.close();
        } catch (Exception e) {
          logger.error("Failed to close DB for registered streams: " + recoverFile, e);
        }
      }
    }
  }
}
