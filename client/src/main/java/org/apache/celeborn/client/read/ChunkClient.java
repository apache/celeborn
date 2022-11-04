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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.Utils;

/**
 * Encapsulate the Partition Location information, so that for the file corresponding to this
 * Partition Location, you can ignore whether there is a retry and whether the Master/Slave switch
 * is performed.
 *
 * <p>Specifically, for a file, we can try maxTries times, and each attempt actually includes
 * attempts to all available copies in a Partition Location. In this way, we can simply take
 * advantage of the ability of multiple copies, and can also ensure that the number of retries for a
 * file will not be too many. Each retry is actually a switch between Master and Slave. Therefore,
 * each retry needs to create a new connection and reopen the file to generate the stream id.
 */
public class ChunkClient {
  private static final Logger logger = LoggerFactory.getLogger(ChunkClient.class);
  private static final ExecutorService executorService =
      Executors.newCachedThreadPool(NettyUtils.createThreadFactory("fetch-chuck"));

  private final ChunkReceivedCallback callback;
  private final Replica replica;
  private final long retryWaitMs;
  private final int maxTries;

  private volatile int numTries = 0;
  private PartitionLocation location;
  private int fetchFailTrigger;

  public PartitionLocation getLocation() {
    return location;
  }

  public ChunkClient(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      ChunkReceivedCallback callback,
      TransportClientFactory clientFactory) {
    this(conf, shuffleKey, location, callback, clientFactory, 0, Integer.MAX_VALUE);
  }

  public ChunkClient(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      ChunkReceivedCallback callback,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex) {
    TransportConf transportConf =
        Utils.fromCelebornConf(conf, TransportModuleConstants.DATA_MODULE, 0);
    this.fetchFailTrigger = conf.fetchFailTrigger();
    this.callback = callback;
    this.retryWaitMs = transportConf.ioRetryWaitTimeMs();

    long fetchTimeoutMs = conf.fetchTimeoutMs();
    this.location = location;

    if (location == null) {
      throw new IllegalArgumentException("Must contain at least one available PartitionLocation.");
    } else {
      replica =
          new Replica(
              fetchTimeoutMs, shuffleKey, location, clientFactory, startMapIndex, endMapIndex);
    }

    this.maxTries = (transportConf.maxIORetries() + 1);
  }

  /**
   * This method should only be called once after RetryingChunkReader is initialized, so it is
   * assumed that there is no concurrency problem when it is called.
   *
   * @return numChunks.
   */
  public synchronized int openChunks() throws IOException {
    int numChunks = -1;
    Exception currentException = null;
    while (numChunks == -1 && hasRemainingRetries()) {
      // Only not wait for first request to each replicate.
      if (numTries >= maxTries) {
        logger.info(
            "Retrying openChunk ({}/{}) for chunk from {} after {} ms.",
            numTries,
            maxTries,
            replica,
            retryWaitMs);
        Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
      }
      try {
        replica.getOrOpenStream();
        numChunks = replica.getNumChunks();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (Exception e) {
        logger.error("Exception raised while sending open chunks message to " + replica + ".", e);
        currentException = e;
        if (shouldRetry(e)) {
          numTries += 1;
        } else {
          break;
        }
      }
    }
    if (numChunks == -1) {
      if (currentException != null) {
        throw new IOException(
            String.format("Could not open chunks from %s after %d tries.", replica, numTries),
            currentException);
      } else {
        throw new IOException(
            String.format("Could not open chunks from %s after %d tries.", replica, numTries));
      }
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
    RetryingChunkReceiveCallback callback;
    synchronized (this) {
      callback = new RetryingChunkReceiveCallback(numTries);
      if (location.getPeer() != null && fetchFailTrigger != 0 && chunkIndex == fetchFailTrigger) {
        IOException manualTriggeredFailure = new IOException("Manual triggered fetch failure");
        callback.onFailure(
            chunkIndex,
            new ChunkClientException(
                manualTriggeredFailure.getMessage(), manualTriggeredFailure, location));
      }
    }
    try {
      TransportClient client = replica.getOrOpenStream();
      client.fetchChunk(replica.getStreamId(), chunkIndex, callback);
    } catch (Exception e) {
      logger.error(
          "Exception raised while beginning fetch chunk {}{}.",
          chunkIndex,
          numTries > 0 ? " (after " + numTries + " retries)" : "",
          e);

      if (shouldRetry(e)) {
        initiateRetry(chunkIndex, callback.currentNumTries);
      } else {
        callback.onFailure(chunkIndex, e);
      }
    }
  }

  @VisibleForTesting
  int getNumTries() {
    return numTries;
  }

  private boolean hasRemainingRetries() {
    return numTries < maxTries;
  }

  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException =
        e instanceof IOException
            || e instanceof TimeoutException
            || (e.getCause() != null && e.getCause() instanceof TimeoutException)
            || (e.getCause() != null && e.getCause() instanceof IOException)
            || (e instanceof RuntimeException
                && e.getMessage().startsWith(IOException.class.getName()));
    return isIOException && hasRemainingRetries();
  }

  @SuppressWarnings("UnstableApiUsage")
  private synchronized void initiateRetry(final int chunkIndex, int currentNumTries) {
    numTries = Math.max(numTries, currentNumTries + 1);

    logger.info(
        "Retrying fetch ({}/{}) for chunk {} from {} after {} ms.",
        currentNumTries,
        maxTries,
        chunkIndex,
        replica,
        retryWaitMs);

    executorService.submit(
        () -> {
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
        logger.error("Abandon to fetch chunk {} after {} tries.", chunkIndex, this.currentNumTries);
        callback.onFailure(chunkIndex, new ChunkClientException(e.getMessage(), e, location));
      }
    }
  }
}
