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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.CelebornTezBypassWriter;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.CelebornTezReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.CelebornIOException;

public class CelebornFetcher extends CallableWithNdc<FetchResult> {
  private static final Logger LOG = LoggerFactory.getLogger(CelebornFetcher.class);
  private final FetcherCallback fetcherCallback;

  private final FetchedInputAllocator inputManager;

  private long copyBlockCount = 0;

  private volatile boolean stopped = false;

  private final CelebornTezReader celebornTezReader;
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long waitTime = 0;
  private long copyTime = 0; // the sum of readTime + decompressTime + serializeTime + waitTime
  private static int uniqueMapId = 0;

  private boolean hasPendingData = false;
  private long startWait;
  private int waitCount = 0;
  private byte[] shuffleData = null;

  CelebornFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputManager,
      CelebornTezReader celebornTezReader) {
    this.fetcherCallback = fetcherCallback;
    this.inputManager = inputManager;
    this.celebornTezReader = celebornTezReader;
  }

  public void fetchAllRssBlocks() throws IOException {
    while (!stopped) {
      try {
        copyFromRssServer();
      } catch (Exception e) {
        LOG.error("Failed to fetchAllRssBlocks.", e);
        throw e;
      }
    }
  }

  @VisibleForTesting
  public void copyFromRssServer() throws IOException {
    long blockStartFetch = 0;
    // fetch a block
    if (!hasPendingData) {
      final long startFetch = System.currentTimeMillis();
      blockStartFetch = System.currentTimeMillis();
      shuffleData = celebornTezReader.getShuffleBlock();
      long fetchDuration = System.currentTimeMillis() - startFetch;
      readTime += fetchDuration;
    }

    if (shuffleData != null) {
      // start to merge
      final long startSerialization = System.currentTimeMillis();
      if (issueMapOutputMerge(blockStartFetch)) {
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        serializeTime += serializationDuration;
        // if reserve successes, reset status for next fetch
        if (hasPendingData) {
          waitTime += System.currentTimeMillis() - startWait;
        }
        hasPendingData = false;
        shuffleData = null;
      } else {
        LOG.info("UncompressedData is null");
        // if reserve fail, return and wait
        waitCount++;
        startWait = System.currentTimeMillis();
        return;
      }

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime + waitTime;
    } else {
      LOG.info("UncompressedData is null");
      // finish reading data, close related reader and check data consistent
      celebornTezReader.close();
      LOG.info(
          "Reduce task partition:"
              + celebornTezReader.getPartitionId()
              + " read block cnt: "
              + copyBlockCount
              + " cost "
              + readTime
              + " ms to fetch and "
              + serializeTime
              + " ms to serialize and "
              + waitTime
              + " ms to wait resource, total copy time: "
              + copyTime);
      LOG.info("Stop fetcher");
      stopFetch();
    }
  }

  private boolean issueMapOutputMerge(long blockStartFetch) throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    FetchedInput fetchedInput = null;

    try {
      fetchedInput =
          inputManager.allocate(
              shuffleData.length, shuffleData.length, uniqueInputAttemptIdentifier);
    } catch (IOException ioe) {
      // kill this reduce attempt
      throw ioe;
    }

    // Allocated space and then write data to mapOutput
    try {
      CelebornTezBypassWriter.write(fetchedInput, shuffleData);
      // let the merger knows this block is ready for merging
      fetcherCallback.fetchSucceeded(
          null,
          uniqueInputAttemptIdentifier,
          fetchedInput,
          shuffleData.length,
          shuffleData.length,
          System.currentTimeMillis() - blockStartFetch);
    } catch (Throwable t) {
      LOG.error("Failed to write fetchedInput.", t);
      throw new CelebornIOException(
          "Partition: "
              + celebornTezReader.getPartitionId()
              + " cannot write block to "
              + fetchedInput.getClass().getSimpleName()
              + " due to: "
              + t.getClass().getName());
    }
    return true;
  }

  private InputAttemptIdentifier getNextUniqueInputAttemptIdentifier() {
    return new InputAttemptIdentifier(uniqueMapId++, 0);
  }

  private void stopFetch() {
    stopped = true;
  }

  @VisibleForTesting
  public int getRetryCount() {
    return waitCount;
  }

  @Override
  protected FetchResult callInternal() throws Exception {
    celebornTezReader.init();
    fetchAllRssBlocks();
    return null;
  }

  public void shutdown() {}

  public Integer getPartitionId() {
    return celebornTezReader.getPartitionId();
  }
}
