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

package com.aliyun.emr.rss.client.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.util.NettyUtils;
import com.aliyun.emr.rss.common.network.util.TransportConf;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.protocol.TransportModuleConstants;
import com.aliyun.emr.rss.common.util.Utils;

/**
 * Encapsulate the Partition Location information, so that for the file corresponding to this
 * Partition Location, you can ignore whether there is a retry and whether the Master/Slave
 * switch is performed.
 *
 * Specifically, for a file, we can try maxTries times, and each attempt actually includes attempts
 * to all available copies in a Partition Location. In this way, we can simply take advantage of
 * the ability of multiple copies, and can also ensure that the number of retries for a file will
 * not be too many. Each retry is actually a switch between Master and Slave. Therefore, each retry
 * needs to create a new connection and reopen the file to generate the stream id.
 */
public class RetryingChunkClient {
  private static final Logger logger = LoggerFactory.getLogger(RetryingChunkClient.class);
  private static final ExecutorService executorService = Executors.newCachedThreadPool(
      NettyUtils.createThreadFactory("Chunk Fetch Retry"));

  private final ChunkReceivedCallback callback;
  private final List<Replica> replicas;
  private final long retryWaitMs;
  private final int maxTries;

  private volatile int numTries = 0;

  public RetryingChunkClient(
    RssConf conf,
    String shuffleKey,
    PartitionLocation location,
    ChunkReceivedCallback callback,
    TransportClientFactory clientFactory) {
    this(conf, shuffleKey, location, callback, clientFactory, 0, Integer.MAX_VALUE);
  }

  public RetryingChunkClient(
      RssConf conf,
      String shuffleKey,
      PartitionLocation location,
      ChunkReceivedCallback callback,
      TransportClientFactory clientFactory,
      int startMapId,
      int endMapId) {
    TransportConf transportConf = Utils.fromRssConf(conf, TransportModuleConstants.DATA_MODULE, 0);

    this.replicas = new ArrayList<>(2);
    this.callback = callback;
    this.retryWaitMs = transportConf.ioRetryWaitTimeMs();

    long timeoutMs = RssConf.fetchChunkTimeoutMs(conf);
    if (location != null) {
      this.replicas.add(new Replica(timeoutMs, shuffleKey, location,
        clientFactory, startMapId, endMapId));
      if (location.getPeer() != null) {
        this.replicas.add(new Replica(timeoutMs, shuffleKey, location.getPeer(),
          clientFactory, startMapId, endMapId));
      }
    }

    if (this.replicas.size() <= 0) {
      throw new IllegalArgumentException("Must contain at least one available PartitionLocation.");
    }

    this.maxTries = (transportConf.maxIORetries() + 1) * replicas.size();
  }

  /**
   * This method should only be called once after RetryingChunkReader is initialized, so it is
   * assumed that there is no concurrency problem when it is called.
   *
   * @return numChunks.
   */
  public int openChunks() throws IOException {
    int numChunks = -1;
    while (numChunks == -1 && hasRemainingRetries()) {
      Replica replica = getCurrentReplica();
      try {
        replica.getOrOpenStream();
        numChunks = replica.getNumChunks();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }

        logger.warn("Exception raised while sending open chunks message to {}.", replica, e);

        numChunks = -1;
        if (shouldRetry(e)) {
          numTries += 1; // openChunks will not be concurrently called.
        } else {
          break;
        }
      }
    }
    if (numChunks == -1) {
      throw new IOException(String.format("Could not open chunks after %d tries.", numTries));
    }
    return numChunks;
  }

  /**
   * Fetch for a chunk. It can be retried multiple times, so there is no guarantee that the order
   * will arrive on the server side, nor can it guarantee an orderly return. Therefore, the chunks
   * should be as orderly as possible when calling.
   *
   * @param chunkIndex the index of the chunk to be fetched.
   */
  public void fetchChunk(int chunkIndex) {
    Replica replica;
    RetryingChunkReceiveCallback callback;
    synchronized (this) {
      replica = getCurrentReplica();
      callback = new RetryingChunkReceiveCallback(numTries);
    }
    try {
      TransportClient client = replica.getOrOpenStream();
      client.fetchChunk(replica.getStreamId(), chunkIndex, callback);
    } catch (Exception e) {
      logger.error("Exception raised while beginning fetch chunk {} {}.",
          chunkIndex, numTries > 0 ? "(after " + numTries + " retries)" : "", e);

      if (shouldRetry(e)) {
        initiateRetry(chunkIndex, callback.currentNumTries);
      } else {
        callback.onFailure(chunkIndex, e);
      }
    }
  }

  @VisibleForTesting
  Replica getCurrentReplica() {
    int currentReplicaIndex = numTries % replicas.size();
    return replicas.get(currentReplicaIndex);
  }

  @VisibleForTesting
  int getNumTries() {
    return numTries;
  }

  private boolean hasRemainingRetries() {
    return numTries < maxTries;
  }

  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException = e instanceof IOException
        || e instanceof TimeoutException
        || (e.getCause() != null && e.getCause() instanceof TimeoutException)
        || (e.getCause() != null && e.getCause() instanceof IOException);
    return isIOException && hasRemainingRetries();
  }

  @SuppressWarnings("UnstableApiUsage")
  private synchronized void initiateRetry(final int chunkIndex, int currentNumTries) {
    numTries = Math.max(numTries, currentNumTries + 1);

    logger.info("Retrying fetch ({}/{}) for chunk {} from {} after {} ms.",
        currentNumTries, maxTries, chunkIndex, getCurrentReplica(), retryWaitMs);

    executorService.submit(() -> {
      Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
      fetchChunk(chunkIndex);
    });
  }

  private class RetryingChunkReceiveCallback implements ChunkReceivedCallback {
    final int currentNumTries;

    RetryingChunkReceiveCallback(int currentNumTries) {
      this.currentNumTries = currentNumTries;
    }

    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      callback.onSuccess(chunkIndex, buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      if (shouldRetry(e)) {
        initiateRetry(chunkIndex, this.currentNumTries);
      } else {
        logger.error("Failed to fetch chunk {}, and will not retry({} tries).",
          chunkIndex, this.currentNumTries);
        callback.onFailure(chunkIndex, e);
      }
    }
  }
}

class Replica {
  private static final Logger logger = LoggerFactory.getLogger(Replica.class);
  private final long timeoutMs;
  private final String shuffleKey;
  private final PartitionLocation location;
  private final TransportClientFactory clientFactory;

  private long streamId;
  private int numChunks;
  private TransportClient client;
  private int startMapId;
  private int endMapId;

  Replica(
    long timeoutMs,
    String shuffleKey,
    PartitionLocation location,
    TransportClientFactory clientFactory,
    int startMapId,
    int endMapId) {
    this.timeoutMs = timeoutMs;
    this.shuffleKey = shuffleKey;
    this.location = location;
    this.clientFactory = clientFactory;
    this.startMapId = startMapId;
    this.endMapId = endMapId;
  }

  Replica(
      long timeoutMs,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory) {
    this(timeoutMs, shuffleKey, location, clientFactory, 0, Integer.MAX_VALUE);
  }

  public synchronized TransportClient getOrOpenStream()
      throws IOException, InterruptedException {
    if (client == null || !client.isActive()) {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());

      ByteBuffer openMessage = createOpenMessage();
      ByteBuffer response = client.sendRpcSync(openMessage, timeoutMs);
      streamId = response.getLong();
      numChunks = response.getInt();
    }
    return client;
  }

  public long getStreamId() {
    return streamId;
  }

  public int getNumChunks() {
    return numChunks;
  }

  @Override
  public String toString() {
    return location.getHost() + ":" + location.getFetchPort();
  }

  private ByteBuffer createOpenMessage() {
    byte[] shuffleKeyBytes = shuffleKey.getBytes(StandardCharsets.UTF_8);
    byte[] fileNameBytes = location.getFileName().getBytes(StandardCharsets.UTF_8);
    ByteBuffer openMessage = ByteBuffer.allocate(
      4 + shuffleKeyBytes.length + 4 + fileNameBytes.length + 4 + 4);
    openMessage.putInt(shuffleKeyBytes.length);
    openMessage.put(shuffleKeyBytes);
    openMessage.putInt(fileNameBytes.length);
    openMessage.put(fileNameBytes);
    openMessage.putInt(startMapId);
    openMessage.putInt(endMapId);
    openMessage.flip();
    return openMessage;
  }

  @VisibleForTesting
  PartitionLocation getLocation() {
    return location;
  }
}
