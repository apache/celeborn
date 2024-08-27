package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.celeborn.common.exception.CelebornIOException;
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

public class CelebornFetcher extends CallableWithNdc<FetchResult> {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornFetcher.class);

  private final FetcherCallback fetcherCallback;
  private final FetchedInputAllocator inputManager;
  private static int uniqueMapId = 0;
  CelebornTezReader reader;
  private final AtomicBoolean isShutDown = new AtomicBoolean(false);
  final int partition;

  public CelebornFetcher(FetchedInputAllocator inputManager,
      FetcherCallback fetcherCallback,
      CelebornTezReader reader,
      int partition) {
    this.fetcherCallback = fetcherCallback;
    this.inputManager = inputManager;
    this.partition = partition;
    this.reader = reader;
  }

  public void fetchAllBlocksInOnPartition() throws IOException {
    try {
      byte[] data = reader.fetchData();
      long blockStartFetchTime = System.currentTimeMillis();
      issueMapOutputMerge(data.length, blockStartFetchTime, data);
    } catch (Exception e) {
      LOG.error("Failed to fetchAllCelebornBlocks.", e);
      throw e;
    }
  }

  private boolean issueMapOutputMerge(int compressedLength, long blockStartFetch, byte[] data)
      throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    FetchedInput fetchedInput = null;

    try {
      fetchedInput =
          inputManager.allocate(compressedLength, compressedLength, uniqueInputAttemptIdentifier);
    } catch (IOException ioe) {
      // kill this reduce attempt
      throw ioe;
    }

    // Allocated space and then write data to mapOutput
    try {
      CelebornTezBypassWriter.write(fetchedInput, data);
      // let the merger knows this block is ready for merging
      fetcherCallback.fetchSucceeded(
          null,
          uniqueInputAttemptIdentifier,
          fetchedInput,
          compressedLength,
          compressedLength,
          System.currentTimeMillis() - blockStartFetch);
    } catch (Throwable t) {
      LOG.error("Failed to write fetchedInput.", t);
      throw new CelebornIOException(
          "Partition: "
              + partition
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

  @Override
  protected FetchResult callInternal() throws Exception {
    if (!isShutDown.get()) {
      fetchAllBlocksInOnPartition();
      isShutDown.getAndSet(true);
    }
    return null;
  }

  public void close() throws IOException {
    isShutDown.getAndSet(true);
  }

  public int getPartition() {
    return partition;
  }
}
