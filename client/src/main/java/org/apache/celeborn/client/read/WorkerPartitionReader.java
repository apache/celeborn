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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;

public class WorkerPartitionReader implements PartitionReader {
  private final Logger logger = LoggerFactory.getLogger(WorkerPartitionReader.class);
  private final AtomicReference<ChunkClient> client = new AtomicReference<>();
  private final AtomicInteger numChunks = new AtomicInteger(0);

  private AtomicInteger returnedChunks = new AtomicInteger(0);
  private AtomicInteger currentChunkIndex = new AtomicInteger(0);

  private final LinkedBlockingQueue<ByteBuf> results;

  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private final int fetchMaxReqsInFlight;
  private boolean closed = false;
  private Set<PartitionLocation> readableLocations = ConcurrentHashMap.newKeySet();

  WorkerPartitionReader(
      CelebornConf conf,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    fetchMaxReqsInFlight = conf.fetchMaxReqsInFlight();
    results = new LinkedBlockingQueue<>();
    readableLocations.add(location);
    if (location.getPeer() != null) {
      readableLocations.add(location.getPeer());
    }
    // only add the buffer to results queue if this reader is not closed.
    ChunkReceivedCallback callback =
        new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            // only add the buffer to results queue if this reader is not closed.
            synchronized (this) {
              ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
              if (!closed) {
                buf.retain();
                results.add(buf);
              }
            }
          }

          @Override
          public void onFailure(int chunkIndex, Throwable e, PartitionLocation location) {
            readableLocations.remove(location);
            if (readableLocations.isEmpty()) {
              String errorMsg = "Fetch chunk " + chunkIndex + " failed.";
              logger.error(errorMsg, e);
              exception.set(new IOException(errorMsg, e));
            } else {
              try {
                synchronized (this) {
                  client.set(
                      new ChunkClient(
                          conf,
                          shuffleKey,
                          location.getPeer(),
                          this,
                          clientFactory,
                          startMapIndex,
                          endMapIndex));
                  currentChunkIndex.set(0);
                  returnedChunks.set(0);
                  numChunks.set(client.get().openChunks());
                }
              } catch (IOException e1) {
                logger.error(e1.getMessage(), e1);
                exception.set(new IOException(e1.getMessage(), e1));
              }
            }
          }
        };
    client.set(
        new ChunkClient(
            conf, shuffleKey, location, callback, clientFactory, startMapIndex, endMapIndex));
    numChunks.set(client.get().openChunks());
  }

  public synchronized boolean hasNext() {
    return returnedChunks.get() < numChunks.get();
  }

  public synchronized ByteBuf next() throws IOException {
    checkException();
    if (currentChunkIndex.get() < numChunks.get()) {
      fetchChunks();
    }
    ByteBuf chunk = null;
    try {
      while (chunk == null) {
        checkException();
        chunk = results.poll(500, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
    returnedChunks.incrementAndGet();
    return chunk;
  }

  public void close() {
    synchronized (this) {
      closed = true;
    }
    if (results.size() > 0) {
      results.forEach(ReferenceCounted::release);
    }
    results.clear();
  }

  private void fetchChunks() {
    final int inFlight =
        currentChunkIndex.get() - returnedChunks.get() < 0
            ? 0
            : currentChunkIndex.get() - returnedChunks.get();
    if (inFlight < fetchMaxReqsInFlight) {
      final int toFetch =
          Math.min(fetchMaxReqsInFlight - inFlight + 1, numChunks.get() - currentChunkIndex.get());
      for (int i = 0; i < toFetch; i++) {
        client.get().fetchChunk(currentChunkIndex.getAndIncrement());
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
