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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

public class WorkerPartitionReader implements PartitionReader {
  private final Logger logger = LoggerFactory.getLogger(WorkerPartitionReader.class);
  private ChunkClient client;
  private int numChunks;

  private int returnedChunks;
  private int currentChunkIndex;

  private final LinkedBlockingQueue<ChunkData> results;

  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private final int fetchMaxReqsInFlight;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private Set<PartitionLocation> readableLocations = ConcurrentHashMap.newKeySet();
  private Set<PartitionLocation> failedLocations = ConcurrentHashMap.newKeySet();

  WorkerPartitionReader(
      RssConf conf,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    fetchMaxReqsInFlight = conf.fetchChunkMaxReqsInFlight(conf);
    results = new LinkedBlockingQueue<>();
    readableLocations.add(location);
    if (location.getPeer() != null) {
      readableLocations.add(location.getPeer());
    }
    // only add the buffer to results queue if this reader is not closed.
    ChunkReceivedCallback callback =
      new ChunkReceivedCallback() {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer, PartitionLocation location) {
          // only add the buffer to results queue if this reader is not closed.
          ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
          if (!closed.get() && !failedLocations.contains(location)) {
            buf.retain();
            results.add(new ChunkData(buf, location));
          }
        }

        @Override
        public void onFailure(int chunkIndex, PartitionLocation location, Throwable e) {
          readableLocations.remove(location);
          if (readableLocations.isEmpty()) {
            String errorMsg = "Fetch chunk " + chunkIndex + " failed.";
            logger.error(errorMsg, e);
            exception.set(new IOException(errorMsg, e));
          } else {
            try {
              synchronized (WorkerPartitionReader.this) {
                if (!failedLocations.contains(location)) {
                  failedLocations.add(location);
                  client = new ChunkClient(conf, shuffleKey, location.getPeer(),
                    this, clientFactory, startMapIndex, endMapIndex);
                  currentChunkIndex = 0;
                  returnedChunks = 0;
                  numChunks = client.openChunks();
                }
              }
            } catch (IOException e1) {
              logger.error(e1.getMessage(), e1);
              exception.set(new IOException(e1.getMessage(), e1));
            }
          }
        }
    };
    client = new ChunkClient(conf, shuffleKey, location, callback, clientFactory,
            startMapIndex, endMapIndex);
    numChunks = client.openChunks();
  }

  public synchronized boolean hasNext() {
    return returnedChunks < numChunks;
  }

  public ByteBuf next() throws IOException {
    checkException();
    synchronized (this) {
      if (currentChunkIndex < numChunks) {
        fetchChunks();
      }
    }
    ByteBuf chunk = null;
    try {
      while (chunk == null) {
        checkException();
        ChunkData chunkData = results.poll(500, TimeUnit.MILLISECONDS);
        if (chunkData != null) {
          synchronized (this) {
            if (failedLocations.contains(chunkData.location)) {
              chunkData.release();
            } else {
              chunk = chunkData.buf;
              returnedChunks++;
            }
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
    return chunk;
  }

  public void close() {
    closed.set(true);
    if (results.size() > 0) {
      results.forEach(ChunkData::release);
    }
    results.clear();
  }

  private void fetchChunks() {
    final int inFlight = currentChunkIndex - returnedChunks;
    if (inFlight < fetchMaxReqsInFlight) {
      final int toFetch =
          Math.min(fetchMaxReqsInFlight - inFlight + 1, numChunks - currentChunkIndex);
      for (int i = 0; i < toFetch; i++) {
        client.fetchChunk(currentChunkIndex++);
      }
    }
  }

  private void checkException() throws IOException {
    IOException e = exception.get();
    if (e != null) {
      throw e;
    }
  }

  private static class ChunkData {
    ByteBuf buf;
    PartitionLocation location;

    ChunkData(ByteBuf buf, PartitionLocation location) {
      this.buf = buf;
      this.location = location;
    }

    public void release() {
      buf.release();
    }
  }
}
