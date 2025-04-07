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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.celeborn.client.read.CelebornIntegrityCheckTracker.registerValidation;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.celeborn.common.CommitMetadata;
import scala.Tuple2;

import com.github.luben.zstd.ZstdException;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Exception;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ClientUtils;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.compress.Decompressor;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.ExceptionMaker;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushFailedBatch;

public abstract class CelebornInputStream extends InputStream {
  private static final Logger logger = LoggerFactory.getLogger(CelebornInputStream.class);

  public static CelebornInputStream create(
      CelebornConf conf,
      TransportClientFactory clientFactory,
      String shuffleKey,
      ArrayList<PartitionLocation> locations,
      ArrayList<PbStreamHandler> streamHandlers,
      int[] attempts,
      Map<String, Set<PushFailedBatch>> failedBatchSetMap,
      Map<String, Pair<Integer, Integer>> chunksRange,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      ConcurrentHashMap<String, Long> fetchExcludedWorkers,
      ShuffleClient shuffleClient,
      int appShuffleId,
      int shuffleId,
      int partitionId,
      ExceptionMaker exceptionMaker,
      MetricsCallback metricsCallback,
      int expectedMapperCount)
      throws IOException {

      // if startMapIndex > endMapIndex, means partition is skew partition and read by Celeborn
      // implementation.
      // locations will split to sub-partitions with startMapIndex size.
      boolean readSkewPartitionWithoutMapRange =
          ClientUtils.readSkewPartitionWithoutMapRange(conf, startMapIndex, endMapIndex);
      if (readSkewPartitionWithoutMapRange) {
        return new CelebornInputStreamImpl(
            conf,
            clientFactory,
            shuffleKey,
            locations,
            streamHandlers,
            attempts,
            failedBatchSetMap,
            attemptNumber,
            taskId,
            chunksRange,
            fetchExcludedWorkers,
            shuffleClient,
            appShuffleId,
            shuffleId,
            partitionId,
            exceptionMaker,
            true,
            metricsCallback,
            expectedMapperCount);
      } else {
        return new CelebornInputStreamImpl(
            conf,
            clientFactory,
            shuffleKey,
            locations,
            streamHandlers,
            attempts,
            failedBatchSetMap,
            attemptNumber,
            taskId,
            startMapIndex,
            endMapIndex,
            /*partitionLocationToChunkRange = */ null,
            fetchExcludedWorkers,
            shuffleClient,
            appShuffleId,
            shuffleId,
            partitionId,
            exceptionMaker,
            false,
            metricsCallback,
            expectedMapperCount);
      }
    }
  }

  public static CelebornInputStream empty() {
    return emptyInputStream;
  }

  private static final CelebornInputStream emptyInputStream =
      new CelebornInputStream() {
        @Override
        public int read() throws IOException {
          return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return -1;
        }

        @Override
        public int totalPartitionsToRead() {
          return 0;
        }

        @Override
        public int partitionsRead() {
          return 0;
        }

        @Override
        public Map<String, CommitMetadata> getExpectedCommitMetadata() {
          return Map.of();
        }
      };

  public abstract int totalPartitionsToRead();

  public abstract int partitionsRead();

  public abstract Map<String, CommitMetadata> getExpectedCommitMetadata();

  private static final class CelebornInputStreamImpl extends CelebornInputStream {
    private static final Random RAND = new Random();

    private final CelebornConf conf;
    private final TransportClientFactory clientFactory;
    private final String shuffleKey;
    private ArrayList<PartitionLocation> locations;
    private ArrayList<PbStreamHandler> streamHandlers;
    private int[] attempts;
    private final int attemptNumber;
    private final long taskId;
    private final int startMapIndex;
    private final int endMapIndex;
    private final Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange;

    private Map<Integer, Set<Integer>> batchesRead = new HashMap<>();

    private final Map<String, Set<PushFailedBatch>> failedBatches;

    private byte[] compressedBuf;
    private byte[] rawDataBuf;
    private Decompressor decompressor;

    private ByteBuf currentChunk;
    private boolean firstChunk = true;
    private PartitionReader currentReader;
    private final int fetchChunkMaxRetry;
    private int fetchChunkRetryCnt = 0;
    int retryWaitMs;
    private int fileIndex;
    private int position;
    private int limit;

    private MetricsCallback callback;

    // mapId, attemptId, batchId, size
    private final int BATCH_HEADER_SIZE = 4 * 4;
    private final byte[] sizeBuf = new byte[BATCH_HEADER_SIZE];
    private LongAdder skipCount = new LongAdder();
    private final boolean rangeReadFilter;
    private final boolean enabledReadLocalShuffle;
    private final String localHostAddress;
    private final Map<String, CommitMetadata> expectedCommitMetadataMap = new HashMap<>();

    private boolean shuffleCompressionEnabled;
    private boolean shuffleIntegrityCheckEnabled;
    private boolean shuffleIntegrityCheckEmptyPartitionEnabled;
    private long fetchExcludedWorkerExpireTimeout;
    private ConcurrentHashMap<String, Long> fetchExcludedWorkers;

    private boolean containLocalRead = false;
    private ShuffleClient shuffleClient;
    private int appShuffleId;
    private int shuffleId;
    private int partitionId;
    private ExceptionMaker exceptionMaker;
    private final int expectedMapperCount;
    private boolean closed = false;
    private boolean integrityChecked = false;
    private final CommitMetadata aggregatedExpectedCommitMetadata = new CommitMetadata();
    private final CommitMetadata aggregatedActualCommitMetadata = new CommitMetadata();
    private final RoaringBitmap actualMappersRead = new RoaringBitmap();
    private final Lock rangeReadMapperBitmapLock = new ReentrantLock();
    private RoaringBitmap rangeReadMappersBitmap = new RoaringBitmap();

    private final boolean readSkewPartitionWithoutMapRange;

    CelebornInputStreamImpl(
        CelebornConf conf,
        TransportClientFactory clientFactory,
        String shuffleKey,
        ArrayList<PartitionLocation> locations,
        ArrayList<PbStreamHandler> streamHandlers,
        int[] attempts,
        Map<String, Set<PushFailedBatch>> failedBatchSet,
        int attemptNumber,
        long taskId,
        Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange,
        ConcurrentHashMap<String, Long> fetchExcludedWorkers,
        ShuffleClient shuffleClient,
        int appShuffleId,
        int shuffleId,
        int partitionId,
        ExceptionMaker exceptionMaker,
        boolean splitSkewPartitionWithoutMapRange,
        MetricsCallback metricsCallback,
        int expectedMapperCount)
        throws IOException {
      this(
          conf,
          clientFactory,
          shuffleKey,
          locations,
          streamHandlers,
          attempts,
          failedBatchSet,
          attemptNumber,
          taskId,
          /*startMapIndex = */ -1,
          /*endMapIndex = */ -1,
          partitionLocationToChunkRange,
          fetchExcludedWorkers,
          shuffleClient,
          appShuffleId,
          shuffleId,
          partitionId,
          exceptionMaker,
          splitSkewPartitionWithoutMapRange,
          metricsCallback,
          expectedMapperCount);
    }

    CelebornInputStreamImpl(
        CelebornConf conf,
        TransportClientFactory clientFactory,
        String shuffleKey,
        ArrayList<PartitionLocation> locations,
        ArrayList<PbStreamHandler> streamHandlers,
        int[] attempts,
        Map<String, Set<PushFailedBatch>> failedBatchSet,
        int attemptNumber,
        long taskId,
        int startMapIndex,
        int endMapIndex,
        Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange,
        ConcurrentHashMap<String, Long> fetchExcludedWorkers,
        ShuffleClient shuffleClient,
        int appShuffleId,
        int shuffleId,
        int partitionId,
        ExceptionMaker exceptionMaker,
        boolean readSkewPartitionWithoutMapRange,
        MetricsCallback metricsCallback,
        int expectedMapperCount)
        throws IOException {
      this.conf = conf;
      this.clientFactory = clientFactory;
      this.shuffleKey = shuffleKey;
      this.locations = locations;
      if (streamHandlers != null && streamHandlers.size() == locations.size()) {
        this.streamHandlers = streamHandlers;
      }
      this.attempts = attempts;
      this.attemptNumber = attemptNumber;
      this.taskId = taskId;
      this.startMapIndex = startMapIndex;
      this.endMapIndex = endMapIndex;
      this.partitionLocationToChunkRange = partitionLocationToChunkRange;
      this.rangeReadFilter = conf.shuffleRangeReadFilterEnabled();
      this.enabledReadLocalShuffle = conf.enableReadLocalShuffleFile();
      this.localHostAddress = Utils.localHostName(conf);
      this.shuffleCompressionEnabled =
          !conf.shuffleCompressionCodec().equals(CompressionCodec.NONE);
      this.shuffleIntegrityCheckEnabled = conf.clientShuffleIntegrityCheckEnabled();
      if (this.shuffleIntegrityCheckEnabled) {
        checkArgument(
                this.shuffleCompressionEnabled, "Shuffle integrity check requires shuffle compression");
      }
      //TODO(gaurav): remove this flag
      this.shuffleIntegrityCheckEmptyPartitionEnabled =
              conf.clientShuffleIntegrityCheckEmptyPartitionEnabled();
      this.fetchExcludedWorkerExpireTimeout = conf.clientFetchExcludedWorkerExpireTimeout();
      this.failedBatches = failedBatchSet;
      this.readSkewPartitionWithoutMapRange = readSkewPartitionWithoutMapRange;
      this.fetchExcludedWorkers = fetchExcludedWorkers;

      if (conf.clientPushReplicateEnabled()) {
        fetchChunkMaxRetry = conf.clientFetchMaxRetriesForEachReplica() * 2;
      } else {
        fetchChunkMaxRetry = conf.clientFetchMaxRetriesForEachReplica();
      }
      this.retryWaitMs = conf.networkIoRetryWaitMs(TransportModuleConstants.DATA_MODULE);
      this.callback = metricsCallback;
      this.exceptionMaker = exceptionMaker;
      this.expectedMapperCount = expectedMapperCount;
      this.partitionId = partitionId;
      this.expectedMapperCount = expectedMapperCount;
      this.appShuffleId = appShuffleId;
      this.shuffleId = shuffleId;
      this.shuffleClient = shuffleClient;

      boolean chunkPrefetchEnabled = conf.clientChunkPrefetchEnabled();
      moveToNextReader(chunkPrefetchEnabled);
      if (chunkPrefetchEnabled) {
        init();
        firstChunk = false;
      }
    }

    private boolean skipLocation(int startMapIndex, int endMapIndex, PartitionLocation location) {
      if (!rangeReadFilter) {
        return false;
      }
      if (endMapIndex == Integer.MAX_VALUE) {
        return false;
      }
      RoaringBitmap bitmap = location.getMapIdBitMap();
      if (bitmap == null && location.hasPeer()) {
        bitmap = location.getPeer().getMapIdBitMap();
      }
      if (bitmap == null) {
        throw new RuntimeException(
            "Bitmap for PartitionLocation is null even when range Filter Feature is enabled: "
                + location);
      }
      synchronized (rangeReadMapperBitmapLock) {
        rangeReadMappersBitmap = RoaringBitmap.or(bitmap, rangeReadMappersBitmap);
      }
      for (int i = startMapIndex; i < endMapIndex; i++) {
        if (bitmap.contains(i)) {
          return false;
        }
      }
      return true;
    }

    private Tuple2<PartitionLocation, PbStreamHandler> nextReadableLocation() {
      int locationCount = locations.size();
      if (fileIndex >= locationCount) {
        return null;
      }
      PartitionLocation currentLocation = locations.get(fileIndex);
      // if pushShuffleFailureTrackingEnabled is true, should not skip location
      while ((readSkewPartitionWithoutMapRange
              && !partitionLocationToChunkRange.containsKey(currentLocation.getUniqueId()))
          || (!readSkewPartitionWithoutMapRange
              && skipLocation(startMapIndex, endMapIndex, currentLocation))) {
        skipCount.increment();
        fileIndex++;
        if (fileIndex == locationCount) {
          return null;
        }
        currentLocation = locations.get(fileIndex);
      }

      fetchChunkRetryCnt = 0;

      return new Tuple2(
          currentLocation, streamHandlers == null ? null : streamHandlers.get(fileIndex));
    }

    private void moveToNextReader(boolean fetchChunk) throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      Tuple2<PartitionLocation, PbStreamHandler> currentLocation = nextReadableLocation();
      if (currentLocation == null) {
        return;
      }
      currentReader = createReaderWithRetry(currentLocation._1, currentLocation._2);
      fileIndex++;
      while (!currentReader.hasNext()) {
        currentReader.close();
        currentReader = null;
        currentLocation = nextReadableLocation();
        if (currentLocation == null) {
          return;
        }
        currentReader = createReaderWithRetry(currentLocation._1, currentLocation._2);
        fileIndex++;
      }
      if (fetchChunk) {
        currentChunk = getNextChunk();
      }
    }

    private boolean isExcluded(PartitionLocation location) {
      Long timestamp = fetchExcludedWorkers.get(location.hostAndFetchPort());
      if (timestamp == null) {
        return false;
      } else if (System.currentTimeMillis() - timestamp > fetchExcludedWorkerExpireTimeout) {
        fetchExcludedWorkers.remove(location.hostAndFetchPort());
        return false;
      } else if (location.getPeer() != null) {
        Long peerTimestamp = fetchExcludedWorkers.get(location.getPeer().hostAndFetchPort());
        // To avoid both replicate locations is excluded, if peer add to excluded list earlier,
        // change to try peer.
        if (peerTimestamp == null || peerTimestamp < timestamp) {
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }

    private PartitionReader createReaderWithRetry(
        PartitionLocation location, PbStreamHandler pbStreamHandler) throws IOException {
      return createReaderWithRetry(location, pbStreamHandler, Optional.empty());
    }

    private PartitionReader createReaderWithRetry(
        PartitionLocation location,
        PbStreamHandler pbStreamHandler,
        Optional<PartitionReaderCheckpointMetadata> checkpointMetadata)
        throws IOException {
      Exception lastException = null;
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          logger.debug("Create reader for location {}", location);
          if (isExcluded(location)) {
            throw new CelebornIOException("Fetch data from excluded worker! " + location);
          }
          PartitionReader reader =
              createReader(
                  location,
                  pbStreamHandler,
                  fetchChunkRetryCnt,
                  fetchChunkMaxRetry,
                  checkpointMetadata);
          return reader;
        } catch (Exception e) {
          lastException = e;
          shuffleClient.excludeFailedFetchLocation(location.hostAndFetchPort(), e);
          fetchChunkRetryCnt++;
          if (location.hasPeer() && !readSkewPartitionWithoutMapRange) {
            // fetchChunkRetryCnt % 2 == 0 means both replicas have been tried,
            // so sleep before next try.
            if (fetchChunkRetryCnt % 2 == 0) {
              Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
            }
            logger.warn(
                "CreatePartitionReader failed {}/{} times for location {}, change to peer",
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                location,
                e);
            if (pbStreamHandler != null) {
              try {
                TransportClient client =
                    clientFactory.createClient(location.getHost(), location.getFetchPort());
                TransportMessage bufferStreamEnd =
                    new TransportMessage(
                        MessageType.BUFFER_STREAM_END,
                        PbBufferStreamEnd.newBuilder()
                            .setStreamType(StreamType.ChunkStream)
                            .setStreamId(pbStreamHandler.getStreamId())
                            .build()
                            .toByteArray());
                client.sendRpc(bufferStreamEnd.toByteBuffer());
              } catch (InterruptedException | IOException ex) {
                logger.warn(
                    "Close {} stream {} failed",
                    location.hostAndFetchPort(),
                    pbStreamHandler.getStreamId(),
                    ex);
              }
              pbStreamHandler = null;
            }
            location = location.getPeer();
          } else {
            logger.warn(
                "CreatePartitionReader failed {}/{} times for location {}, retry the same location",
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                location,
                e);
            Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
          }
        }
      }
      throw new CelebornIOException("createPartitionReader failed! " + location, lastException);
    }

    private ByteBuf getNextChunk() throws IOException {
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          if (isExcluded(currentReader.getLocation())) {
            throw new CelebornIOException(
                "Fetch data from excluded worker! " + currentReader.getLocation());
          }
          return currentReader.next();
        } catch (Exception e) {
          shuffleClient.excludeFailedFetchLocation(
              currentReader.getLocation().hostAndFetchPort(), e);
          fetchChunkRetryCnt++;
          currentReader.close();
          if (fetchChunkRetryCnt == fetchChunkMaxRetry) {
            logger.warn("Fetch chunk fail exceeds max retry {}", fetchChunkRetryCnt, e);
            throw new CelebornIOException(
                "Fetch chunk failed for "
                    + fetchChunkRetryCnt
                    + " times for location "
                    + currentReader.getLocation(),
                e);
          } else {
            if (currentReader.getLocation().hasPeer() && !readSkewPartitionWithoutMapRange) {
              logger.warn(
                  "Fetch chunk failed {}/{} times for location {}, change to peer",
                  fetchChunkRetryCnt,
                  fetchChunkMaxRetry,
                  currentReader.getLocation(),
                  e);
              // fetchChunkRetryCnt % 2 == 0 means both replicas have been tried,
              // so sleep before next try.
              if (fetchChunkRetryCnt % 2 == 0) {
                Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
              }
              // We must not use checkpoint for peer location since chunkIds don't always match
              // across peers
              currentReader = createReaderWithRetry(currentReader.getLocation().getPeer(), null);
            } else {
              logger.warn(
                  "Fetch chunk failed {}/{} times for location {}",
                  fetchChunkRetryCnt,
                  fetchChunkMaxRetry,
                  currentReader.getLocation(),
                  e);
              Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
              // When reading from the same host again, it is possible to skip already read data
              // chunks,
              // improving read performance during retries.
              currentReader =
                  createReaderWithRetry(
                      currentReader.getLocation(),
                      null,
                      currentReader.getPartitionReaderCheckpointMetadata());
            }
          }
        }
      }
      throw new CelebornIOException("Fetch chunk failed! " + currentReader.getLocation());
    }

    private PartitionReader createReader(
        PartitionLocation location,
        PbStreamHandler pbStreamHandler,
        int fetchChunkRetryCnt,
        int fetchChunkMaxRetry,
        Optional<PartitionReaderCheckpointMetadata> checkpointMetadata)
        throws IOException, InterruptedException {

      StorageInfo storageInfo = location.getStorageInfo();

      int startChunkIndex = -1;
      int endChunkIndex = -1;
      if (partitionLocationToChunkRange != null) {
        Pair<Integer, Integer> chunkRange =
            partitionLocationToChunkRange.get(location.getUniqueId());
        startChunkIndex = chunkRange.getLeft();
        endChunkIndex = chunkRange.getRight();
      }
      switch (storageInfo.getType()) {
        case HDD:
        case SSD:
        case MEMORY:
          if (enabledReadLocalShuffle
              && location.getHost().equals(localHostAddress)
              && storageInfo.getType() != StorageInfo.Type.MEMORY) {
            logger.debug("Read local shuffle file {}", localHostAddress);
            containLocalRead = true;
            return new LocalPartitionReader(
                conf,
                shuffleKey,
                location,
                pbStreamHandler,
                clientFactory,
                startMapIndex,
                endMapIndex,
                callback,
                startChunkIndex,
                endChunkIndex);
          } else {
            return new WorkerPartitionReader(
                conf,
                shuffleKey,
                location,
                pbStreamHandler,
                clientFactory,
                startMapIndex,
                endMapIndex,
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                callback,
                startChunkIndex,
                endChunkIndex,
                checkpointMetadata);
          }
        case S3:
        case HDFS:
          return new DfsPartitionReader(
              conf,
              shuffleKey,
              location,
              pbStreamHandler,
              clientFactory,
              startMapIndex,
              endMapIndex,
              callback,
              startChunkIndex,
              endChunkIndex,
              checkpointMetadata);
        default:
          throw new CelebornIOException(
              String.format("Unknown storage info %s to read location %s", storageInfo, location));
      }
    }

    @Override
    public int read() throws IOException {
      if (position < limit) {
        int b = rawDataBuf[position];
        position++;
        return b & 0xFF;
      }

      if (!fillBuffer()) {
        return -1;
      }

      if (position >= limit) {
        return read();
      } else {
        int b = rawDataBuf[position];
        position++;
        return b & 0xFF;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }

      int readBytes = 0;
      while (readBytes < len) {
        while (position >= limit) {
          if (!fillBuffer()) {
            return readBytes > 0 ? readBytes : -1;
          }
        }

        int bytesToRead = Math.min(limit - position, len - readBytes);
        System.arraycopy(rawDataBuf, position, b, off + readBytes, bytesToRead);
        position += bytesToRead;
        readBytes += bytesToRead;
      }

      return readBytes;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        int locationsCount = locations.size();
        logger.debug(
            "AppShuffleId {}, shuffleId {}, partitionId {}, total location count {}, read {}, skip {}",
            appShuffleId,
            shuffleId,
            partitionId,
            locationsCount,
            locationsCount - skipCount.sum(),
            skipCount.sum());
        if (currentChunk != null) {
          logger.debug("Release chunk {}", currentChunk);
          currentChunk.release();
          currentChunk = null;
        }
        if (currentReader != null) {
          logger.debug("Closing reader");
          currentReader.close();
          currentReader = null;
        }
        if (containLocalRead) {
          ShuffleClient.printReadStats(logger);
        }

        compressedBuf = null;
        rawDataBuf = null;
        batchesRead = null;
        locations = null;
        attempts = null;
        decompressor = null;
        fetchExcludedWorkers = null;

        closed = true;
      }
    }

    void validateIntegrity() {
      if (integrityChecked) {
        logger.info("Skipping integrity checks since checks have already been performed");
        return;
      }
      if (!shuffleIntegrityCheckEnabled) {
        logger.info("Skipping integrity checks since shuffleIntegrityCheckEnabled is disabled");
        return;
      }

      String key = Utils.makeReducerKey(shuffleId, partitionId);

      boolean isCommitMetadataEqual =
          CommitMetadata.checkCommitMetadata(
              aggregatedExpectedCommitMetadata, aggregatedActualCommitMetadata);
      if (!isCommitMetadataEqual) {
        String errorMessage =
            String.format(
                "Mismatched CommitMetadata in AppShuffleId %s, shuffleId %s, partitionId %s. Expected: %s, actual: %s",
                appShuffleId,
                shuffleId,
                partitionId,
                aggregatedExpectedCommitMetadata,
                aggregatedActualCommitMetadata);
        RuntimeException exception = new RuntimeException(errorMessage);
        logger.error("Integrity check failed", exception);
        throw exception;
      } else {
        logger.info(
            "Matched CommitMetadata for {}. Expected CommitMetadata: {}, actual CommitMetadata: {}",
            key,
            aggregatedExpectedCommitMetadata,
            aggregatedActualCommitMetadata);
      }

      if (!shuffleIntegrityCheckEmptyPartitionEnabled) {
        logger.info(
            "Skipping empty partition check since shuffleIntegrityCheckEmptyPartitionEnabled is disabled");
        registerValidation(appShuffleId, startMapIndex, endMapIndex, partitionId);
        integrityChecked = true;
        return;
      }

      int actualMappersCount;
      if (endMapIndex == Integer.MAX_VALUE) {
        actualMappersCount = actualMappersRead.getCardinality();
      } else {
        if (rangeReadFilter) {
          synchronized (rangeReadMapperBitmapLock) {
            actualMappersCount = rangeReadMappersBitmap.getCardinality();
          }
        } else {
          logger.info(
              "Potential AQE case. Completeness check will be made in LM: partitionId: {}, startMapIndex: {}, endMapIndex: {},",
              partitionId,
              startMapIndex,
              endMapIndex);
          try {
            int actualMapperCount = actualMappersRead.getCardinality();
            shuffleClient.reducerPartitionEnd(
                shuffleId, partitionId, startMapIndex, endMapIndex, actualMapperCount);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          registerValidation(appShuffleId, startMapIndex, endMapIndex, partitionId);
          integrityChecked = true;
          return;
        }
      }
      boolean isMapperCountEqual = actualMappersCount == expectedMapperCount;
      if (!isMapperCountEqual) {
        String errorMessage =
            String.format(
                "Mismatched mapper count in AppShuffleId %s, shuffleId %s, partitionId %s for rangeReadFilter %b, startMapIndex %d, endMapIndex %d. Expected count: %s, actual count: %s (actual mappers %s)",
                appShuffleId,
                shuffleId,
                partitionId,
                rangeReadFilter,
                startMapIndex,
                endMapIndex,
                expectedMapperCount,
                actualMappersCount,
                actualMappersRead);
        RuntimeException exception = new RuntimeException(errorMessage);
        logger.error("Integrity check failed", exception);
        throw exception;
      }
      registerValidation(appShuffleId, startMapIndex, endMapIndex, partitionId);
      integrityChecked = true;
      logger.info(
          "Matched mapper count for {}. Expected mapper count: {}, actual mapper count: {}",
          key,
          expectedMapperCount,
          actualMappersCount);
    }

    private boolean moveToNextChunk() throws IOException {
      if (currentChunk != null) {
        currentChunk.release();
      }
      currentChunk = null;
      if (currentReader.hasNext()) {
        currentChunk = getNextChunk();
        return true;
      } else if (fileIndex < locations.size()) {
        moveToNextReader(true);
        return currentReader != null;
      }
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      return false;
    }

    private void init() {
      int bufferSize = conf.clientFetchBufferSize();

      if (shuffleCompressionEnabled) {
        int headerLen = Decompressor.getCompressionHeaderLength(conf);
        bufferSize += headerLen;
        compressedBuf = new byte[bufferSize];
        decompressor = Decompressor.getDecompressor(conf);
      }
      rawDataBuf = new byte[bufferSize];
    }

    private boolean fillBuffer() throws IOException {
      try {
        if (firstChunk && currentReader != null) {
          init();
          currentChunk = getNextChunk();
          firstChunk = false;
        }
        if (currentChunk == null) {
          validateIntegrity();
          close();
          return false;
        }

        PushFailedBatch failedBatch = new PushFailedBatch(-1, -1, -1);
        boolean hasData = false;
        while (currentChunk.isReadable() || moveToNextChunk()) {
          currentChunk.readBytes(sizeBuf);
          int mapId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
          int attemptId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4);
          int batchId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8);
          int size = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12);

          if (shuffleCompressionEnabled) {
            if (size > compressedBuf.length) {
              compressedBuf = new byte[size];
            }

            currentChunk.readBytes(compressedBuf, 0, size);
          } else {
            if (size > rawDataBuf.length) {
              rawDataBuf = new byte[size];
            }

            currentChunk.readBytes(rawDataBuf, 0, size);
          }

          // de-duplicate
          if (attemptId == attempts[mapId]) {
            actualMappersRead.add(mapId);
            if (readSkewPartitionWithoutMapRange) {
              Set<PushFailedBatch> failedBatchSet =
                  this.failedBatches.get(currentReader.getLocation().getUniqueId());
              if (null != failedBatchSet) {
                failedBatch.setMapId(mapId);
                failedBatch.setAttemptId(attemptId);
                failedBatch.setBatchId(batchId);
                if (failedBatchSet.contains(failedBatch)) {
                  logger.warn("Skip duplicated batch: {}.", failedBatch);
                  continue;
                }
              }
            }
            if (!batchesRead.containsKey(mapId)) {
              Set<Integer> batchSet = new HashSet<>();
              batchesRead.put(mapId, batchSet);
            }
            Set<Integer> batchSet = batchesRead.get(mapId);
            if (!batchSet.contains(batchId)) {
              batchSet.add(batchId);
              callback.incBytesRead(BATCH_HEADER_SIZE + size);

              // Handling when integrity checks are disabled.
              if (!shuffleIntegrityCheckEnabled) {
                if (shuffleCompressionEnabled) {
                  // decompress data
                  int originalLength = decompressor.getOriginalLen(compressedBuf);
                  if (rawDataBuf.length < originalLength) {
                    rawDataBuf = new byte[originalLength];
                  }
                  limit = decompressor.decompress(compressedBuf, rawDataBuf, 0);
                } else {
                  limit = size;
                }
                position = 0;
                hasData = true;
                break;
              }

              // Handling when integrity checks are enabled.
              String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
              if (batchId == ShuffleClient.METADATA_BATCH_ID) {
                if (!shuffleCompressionEnabled) {
                  throw new RuntimeException(
                          "Unexpected commit metadata when shuffleCompression is disabled");
                }
                int originalLength = decompressor.getOriginalLen(compressedBuf);
                var rawMetadataBuf = new byte[originalLength];
                decompressor.decompress(compressedBuf, rawMetadataBuf, 0);

                CommitMetadata commitMetadata =
                        convertRawMetadataToMapAttemptCommitMetadata(rawMetadataBuf);
                logger.debug(
                        "partition {} converted CommitMetadata{} for map id {} attempt Id {} input stream {} ",
                        partitionId,
                        commitMetadata,
                        mapId,
                        attemptId,
                        this.hashCode());

                expectedCommitMetadataMap.put(mapKey, commitMetadata);
                aggregatedExpectedCommitMetadata.addCommitData(commitMetadata);
              } else {
                limit = size;

                if (shuffleCompressionEnabled) {
                  // decompress data
                  int originalLength = decompressor.getOriginalLen(compressedBuf);
                  if (rawDataBuf.length < originalLength) {
                    rawDataBuf = new byte[originalLength];
                  }
                  limit = decompressor.decompress(compressedBuf, rawDataBuf, 0);
                  aggregatedActualCommitMetadata.addDataWithOffsetAndLength(rawDataBuf, 0, limit);
                } else {
                  limit = size;
                }
                position = 0;
                hasData = true;
                break;
              }
            } else {
              callback.incDuplicateBytesRead(BATCH_HEADER_SIZE + size);
              logger.debug(
                  "Skip duplicated batch: mapId {}, attemptId {}, batchId {}.",
                  mapId,
                  attemptId,
                  batchId);
            }
          }
        }

        if (!hasData) {
          validateIntegrity();
          // TODO(borovsky) consider closing the stream
        }

        return hasData;
      } catch (LZ4Exception | ZstdException | IOException e) {
        logger.error(
            "Failed to fill buffer from chunk. AppShuffleId {}, shuffleId {}, partitionId {}, location {}",
            appShuffleId,
            shuffleId,
            partitionId,
            Optional.ofNullable(currentReader).map(PartitionReader::getLocation).orElse(null),
            e);
        IOException ioe;
        if (e instanceof IOException) {
          ioe = (IOException) e;
        } else {
          ioe = new IOException(e);
        }
        if (exceptionMaker != null) {
          if (shuffleClient.reportShuffleFetchFailure(appShuffleId, shuffleId, taskId)) {
            /*
             * [[ExceptionMaker.makeException]], for spark applications with celeborn.client.spark.fetch.throwsFetchFailure enabled will result in creating
             * a FetchFailedException; and that will make the TaskContext as failed with shuffle fetch issues - see SPARK-19276 for more.
             * Given this, Celeborn can wrap the FetchFailedException with our CelebornIOException
             */
            ioe =
                new CelebornIOException(
                    exceptionMaker.makeFetchFailureException(
                        appShuffleId, shuffleId, partitionId, e));
          }
        }
        throw ioe;
      } catch (Exception e) {
        logger.error(
            "Failed to fill buffer from chunk. AppShuffleId {}, shuffleId {}, partitionId {}, location {}",
            appShuffleId,
            shuffleId,
            partitionId,
            Optional.ofNullable(currentReader).map(PartitionReader::getLocation).orElse(null),
            e);
        throw e;
      }
    }

    private CommitMetadata convertRawMetadataToMapAttemptCommitMetadata(byte[] rawMetadataBuf) {
      return CommitMetadata.decode(Unpooled.wrappedBuffer(rawMetadataBuf));
    }

    @Override
    public int totalPartitionsToRead() {
      return locations.size();
    }

    @Override
    public int partitionsRead() {
      return fileIndex;
    }

    @Override
    public Map<String, CommitMetadata> getExpectedCommitMetadata() {
      return expectedCommitMetadataMap;
    }
  }
}
