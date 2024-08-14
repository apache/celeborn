package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.SHUFFLE_ERR_GRP_NAME;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.unsafe.Platform;

public class CelebornTezReader {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CelebornTezReader.class);

  private String appUniqueId;
  private int shuffleId;
  private int partitionId;
  private int attemptNumber;
  private ShuffleClient shuffleClient;
  private MergeManager mergeManager;
  private volatile boolean stopped = false;
  private boolean hasPendingData = false;
  private long inputShuffleSize;
  private byte[] shuffleData;
  private CelebornInputStream celebornInputStream;
  private static int uniqueMapId = 0;
  private final AtomicInteger issuedCnt = new AtomicInteger(0);
  private final TezCounter ioErrs;

  public CelebornTezReader(
      String appUniqueId,
      String lifecycleManagerHost,
      int lifecycleManagerPort,
      int shuffleId,
      int partitionId,
      int attemptNumber,
      TezCounters tezCounters,
      UserIdentifier userIdentifier,
      MergeManager mergeManager,
      CelebornConf conf) {
    shuffleClient =
        ShuffleClient.get(
            appUniqueId, lifecycleManagerHost, lifecycleManagerPort, conf, userIdentifier, null);
    this.appUniqueId = appUniqueId;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    this.shuffleId = shuffleId;
    this.mergeManager = mergeManager;
    ioErrs = tezCounters.findCounter(SHUFFLE_ERR_GRP_NAME, "IO_ERROR");
  }

  public void fetchAndMerge() throws IOException {
    MetricsCallback metricsCallback =
        new MetricsCallback() {
          @Override
          public void incBytesRead(long bytesRead) {}

          @Override
          public void incReadTime(long time) {}
        };
    celebornInputStream =
        shuffleClient.readPartition(
            shuffleId, partitionId, attemptNumber, 0, Integer.MAX_VALUE, metricsCallback);

    while (!stopped) {
      try {
        // If merge is on, block
        mergeManager.waitForInMemoryMerge();
        // read blocks
        fetchToLocalAndMerge();
      } catch (Exception e) {
        logger.error("Celeborn shuffle fetcher fetch data failed.", e);
      } finally {
      }
    }
  }

  private void fetchToLocalAndMerge() throws IOException {
    if (!hasPendingData) {
      shuffleData = getShuffleBlock();
    }

    if (shuffleData != null) {
      // start to merge
      if (wrapMapOutput(shuffleData)) {
        hasPendingData = false;
      } else {
        return;
      }
    } else {
      celebornInputStream.close();
      logger.info("reduce task {} read {} bytes", partitionId, inputShuffleSize);
      stopped = true;
    }
  }

  private InputAttemptIdentifier getNextUniqueInputAttemptIdentifier() {
    return new InputAttemptIdentifier(uniqueMapId++, 0);
  }

  private boolean wrapMapOutput(byte[] shuffleData) throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    MapOutput mapOutput = null;
    try {
      issuedCnt.incrementAndGet();
      mapOutput = mergeManager.reserve(uniqueInputAttemptIdentifier, shuffleData.length, 0, 1);
    } catch (IOException ioe) {
      // kill this reduce attempt
      ioErrs.increment(1);
      throw ioe;
    }
    // Check if we can shuffle *now* ...
    if (mapOutput == null || mapOutput.getType() == MapOutput.Type.WAIT) {
      logger.info("RssMRFetcher - MergeManager returned status WAIT ...");
      // Not an error but wait to process data.
      // Use a retry flag to avoid re-fetch and re-uncompress.
      hasPendingData = true;
      return false;
    }

    // write data to mapOutput
    try {
      RssTezBypassWriter.write(mapOutput, shuffleData);
      mapOutput.getType();
      // let the merger knows this block is ready for merging
      mapOutput.commit();
    } catch (Throwable t) {
      ioErrs.increment(1);
      mapOutput.abort();
      throw new CelebornIOException(t);
    }
    return true;
  }

  private byte[] getShuffleBlock() throws IOException {
    // get len
    byte[] header = new byte[4];
    int count = celebornInputStream.read(header);
    if (count == -1) {
      stopped = true;
      return null;
    }
    while (count != header.length) {
      count += celebornInputStream.read(header, count, 4 - count);
    }

    // get data
    int blockLen = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
    inputShuffleSize += blockLen;
    byte[] shuffleData = new byte[blockLen];
    count = celebornInputStream.read(shuffleData);
    while (count != shuffleData.length) {
      count += celebornInputStream.read(shuffleData, count, blockLen - count);
      if (count == -1) {
        // read shuffle is done.
        stopped = true;
        throw new CelebornIOException("Read mr shuffle failed.");
      }
    }
    return shuffleData;
  }
}
