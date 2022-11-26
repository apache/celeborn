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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStream;
import com.aliyun.emr.rss.common.network.protocol.StreamHandle;
import io.netty.buffer.ByteBuf;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.client.compress.Decompressor;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.unsafe.Platform;

public abstract class RssInputStream extends InputStream {
  private static final Logger logger = LoggerFactory.getLogger(RssInputStream.class);

  public static RssInputStream create(
      RssConf conf,
      TransportClientFactory clientFactory,
      String shuffleKey,
      PartitionLocation[] locations,
      int[] attempts,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) throws IOException {
    if (locations == null || locations.length == 0) {
      return emptyInputStream;
    } else {
      return new RssInputStreamImpl(conf, clientFactory, shuffleKey, locations, attempts,
        attemptNumber, startMapIndex, endMapIndex);
    }
  }

  public static RssInputStream empty() {
    return emptyInputStream;
  }

  public abstract void setCallback(MetricsCallback callback);

  private static final RssInputStream emptyInputStream = new RssInputStream() {
    @Override
    public int read() throws IOException {
      return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return -1;
    }

    @Override
    public void setCallback(MetricsCallback callback) {
    }
  };

  private static final class RssInputStreamImpl extends RssInputStream {
    private final RssConf conf;
    private final TransportClientFactory clientFactory;
    private final String shuffleKey;
    private final PartitionLocation[] locations;
    private final int[] attempts;
    private final int attemptNumber;
    private final int startMapIndex;
    private final int endMapIndex;

    private final int maxInFlight;

    private final Map<Integer, Set<Integer>> batchesRead = new HashMap<>();

    private byte[] compressedBuf;
    private byte[] decompressedBuf;
    private final Decompressor decompressor;

    private ByteBuf currentChunk;
    private PartitionReader currentReader;
    private final int fetchChunkMaxRetry;
    private int fetchChunkRetryCnt = 0;
    private int fileIndex;
    private int position;
    private int limit;

    private MetricsCallback callback;

    // mapId, attempId, batchId, size
    private final int BATCH_HEADER_SIZE = 4 * 4;
    private final byte[] sizeBuf = new byte[BATCH_HEADER_SIZE];
    private LongAdder skipCount = new LongAdder();
    private final boolean rangeReadFilter;

    RssInputStreamImpl(
        RssConf conf,
        TransportClientFactory clientFactory,
        String shuffleKey,
        PartitionLocation[] locations,
        int[] attempts,
        int attemptNumber,
        int startMapIndex,
        int endMapIndex) throws IOException {
      this.conf = conf;
      this.clientFactory = clientFactory;
      this.shuffleKey = shuffleKey;

      List<PartitionLocation> shuffledLocations =
        new ArrayList() {{
          addAll(Arrays.asList(locations));
        }};
      Collections.shuffle(shuffledLocations);
      this.locations = shuffledLocations.toArray(new PartitionLocation[locations.length]);

      this.attempts = attempts;
      this.attemptNumber = attemptNumber;
      this.startMapIndex = startMapIndex;
      this.endMapIndex = endMapIndex;
      this.rangeReadFilter = RssConf.rangeReadFilterEnabled(conf);

      maxInFlight = RssConf.fetchChunkMaxReqsInFlight(conf);

      int headerLen = Decompressor.getCompressionHeaderLength(conf);
      int blockSize = RssConf.pushDataBufferSize(conf) + headerLen;
      compressedBuf = new byte[blockSize];
      decompressedBuf = new byte[blockSize];

      decompressor = Decompressor.getDecompressor(conf);

      fetchChunkMaxRetry = RssConf.fetchChunkMaxRetries(conf);

      moveToNextReader();
    }

    private boolean skipLocation(int startMapIndex, int endMapIndex, PartitionLocation location) {
      if (!rangeReadFilter) {
        return false;
      }
      if (endMapIndex == Integer.MAX_VALUE) {
        return false;
      }
      RoaringBitmap bitmap = location.getMapIdBitMap();
      if (bitmap == null && location.getPeer() != null) {
        bitmap = location.getPeer().getMapIdBitMap();
      }
      for (int i = startMapIndex; i < endMapIndex; i++) {
        if (bitmap.contains(i)) {
          return false;
        }
      }
      return true;
    }

    private PartitionLocation nextReadableLocation() {
      int locationCount = locations.length;
      if (fileIndex >= locationCount) {
        return null;
      }
      PartitionLocation currentLocation = locations[fileIndex];
      while (skipLocation(startMapIndex, endMapIndex, currentLocation)) {
        skipCount.increment();
        fileIndex++;
        if (fileIndex == locationCount) {
          return null;
        }
        currentLocation = locations[fileIndex];
      }

      fetchChunkRetryCnt = 0;

      return currentLocation;
    }

    private void moveToNextReader() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      PartitionLocation currentLocation = nextReadableLocation();
      if (currentLocation == null) {
        return;
      }
      currentReader = createReaderWithRetry(currentLocation);
      fileIndex++;
      while (!currentReader.hasNext()) {
        currentReader.close();
        currentReader = null;
        currentLocation = nextReadableLocation();
        if (currentLocation == null) {
          return;
        }
        currentReader = createReaderWithRetry(currentLocation);
        fileIndex++;
      }
      currentChunk = getNextChunk();
    }

    private PartitionReader createReaderWithRetry(PartitionLocation location) throws IOException {
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          return createReader(location, fetchChunkRetryCnt, fetchChunkMaxRetry);
        } catch (Exception e) {
          fetchChunkRetryCnt++;
          if (location.getPeer() != null) {
            location = location.getPeer();
            logger.warn("CreatePartitionReader failed {}/{} times, change to peer",
              fetchChunkRetryCnt, fetchChunkMaxRetry);
          } else {
            logger.warn("CreatePartitionReader failed {}/{} times, retry the same location",
              fetchChunkRetryCnt, fetchChunkMaxRetry);
          }
        }
      }
      throw new IOException("createPartitionReader failed!");
    }

    private ByteBuf getNextChunk() throws IOException {
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          return currentReader.next();
        } catch (Exception e) {
          fetchChunkRetryCnt++;
          currentReader.close();
          if (fetchChunkRetryCnt == fetchChunkMaxRetry) {
            logger.warn("Fetch chunk fail exceeds max retry {}", fetchChunkRetryCnt);
            throw new IOException("Fetch chunk failed for " + fetchChunkRetryCnt + " times");
          } else {
            if (currentReader.getLocation().getPeer() != null) {
              logger.warn("Fetch chunk failed {}/{} times, change to peer",
                fetchChunkRetryCnt, fetchChunkMaxRetry);
              currentReader = createReaderWithRetry(currentReader.location.getPeer());
            } else {
              logger.warn("Fetch chunk failed {}/{} times", fetchChunkRetryCnt, fetchChunkMaxRetry);
              currentReader = createReaderWithRetry(currentReader.location);
            }
          }
        }
      }
      throw new IOException("Fetch chunk failed!");
    }

    private PartitionReader createReader(
      PartitionLocation location,
      int fetchChunkRetryCnt,
      int fetchChunkMaxRetry) throws IOException {
      if (location.getPeer() == null) {
        logger.debug("Partition {} has only one partition replica.", location);
      }
      if (location.getPeer() != null && attemptNumber % 2 == 1) {
        location = location.getPeer();
        logger.debug("Read peer {} for attempt {}.", location, attemptNumber);
      }

      return new PartitionReader(location, fetchChunkRetryCnt, fetchChunkMaxRetry);
    }

    public void setCallback(MetricsCallback callback) {
      // callback must set before read()
      this.callback = callback;
    }

    @Override
    public int read() throws IOException {
      if (position < limit) {
        int b = decompressedBuf[position];
        position++;
        return b & 0xFF;
      }

      if (!fillBuffer()) {
        return -1;
      }

      if (position >= limit) {
        return read();
      } else {
        int b = decompressedBuf[position];
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
        System.arraycopy(decompressedBuf, position, b, off + readBytes, bytesToRead);
        position += bytesToRead;
        readBytes += bytesToRead;
      }

      return readBytes;
    }

    @Override
    public void close() {
      int locationsCount = locations.length;
      logger.debug(
          "total location count {} read {} skip {}",
          locationsCount,
          locationsCount - skipCount.sum(),
          skipCount.sum());
      if (currentChunk != null) {
        logger.debug("Release chunk {}!", currentChunk);
        currentChunk.release();
        currentChunk = null;
      }
      if (currentReader != null) {
        logger.debug("Closing reader");
        currentReader.close();
        currentReader = null;
      }
    }

    private boolean moveToNextChunk() throws IOException {
      if (currentChunk != null) {
        currentChunk.release();
      }
      currentChunk = null;
      if (currentReader.hasNext()) {
        currentChunk = getNextChunk();
        return true;
      } else if (fileIndex < locations.length) {
        moveToNextReader();
        return currentReader != null;
      }
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      return false;
    }

    private boolean fillBuffer() throws IOException {
      if (currentChunk == null) {
        return false;
      }

      long startTime = System.currentTimeMillis();

      boolean hasData = false;
      while (currentChunk.isReadable() || moveToNextChunk()) {
        currentChunk.readBytes(sizeBuf);
        int mapId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
        int attemptId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4);
        int batchId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8);
        int size = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12);
        if (size > compressedBuf.length) {
          compressedBuf = new byte[size];
        }

        currentChunk.readBytes(compressedBuf, 0, size);

        // de-duplicate
        if (attemptId == attempts[mapId]) {
          if (!batchesRead.containsKey(mapId)) {
            Set<Integer> batchSet = new HashSet<>();
            batchesRead.put(mapId, batchSet);
          }
          Set<Integer> batchSet = batchesRead.get(mapId);
          if (!batchSet.contains(batchId)) {
            batchSet.add(batchId);
            if (callback != null) {
              callback.incBytesRead(BATCH_HEADER_SIZE + size);
            }
            // decompress data
            int originalLength = decompressor.getOriginalLen(compressedBuf);
            if (decompressedBuf.length < originalLength) {
              decompressedBuf = new byte[originalLength];
            }
            limit = decompressor.decompress(compressedBuf, decompressedBuf, 0);
            position = 0;
            hasData = true;
            break;
          } else {
            logger.debug("Skip duplicated batch: mapId {}, attemptId {}," +
                " batchId {}.", mapId, attemptId, batchId);
          }
        }
      }

      if (callback != null) {
        callback.incReadTime(System.currentTimeMillis() - startTime);
      }
      return hasData;
    }

    private final class PartitionReader {
      private PartitionLocation location;
      private TransportClient client;
      private StreamHandle streamHandle;

      private int returnedChunks;
      private int chunkIndex;

      private final LinkedBlockingQueue<ByteBuf> results;
      private final ChunkReceivedCallback callback;

      private final AtomicReference<IOException> exception = new AtomicReference<>();

      private boolean closed = false;

      // for test
      private int fetchChunkRetryCnt;
      private int fetchChunkMaxRetry;
      private final boolean testFetch;

      PartitionReader(
        PartitionLocation location,
        int fetchChunkRetryCnt,
        int fetchChunkMaxRetry) throws IOException {
        this.location = location;

        results = new LinkedBlockingQueue<>();
        callback = new ChunkReceivedCallback() {
          @Override
          public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            // only add the buffer to results queue if this reader is not closed.
            synchronized(PartitionReader.this) {
              ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
              if (!closed) {
                buf.retain();
                results.add(buf);
              }
            }
          }

          @Override
          public void onFailure(int chunkIndex, Throwable e) {
            String errorMsg = "Fetch chunk " + chunkIndex + " failed.";
            logger.error(errorMsg, e);
            exception.set(new IOException(errorMsg, e));
          }
        };
        try {
          client = clientFactory.createClient(location.getHost(), location.getFetchPort());
        } catch (InterruptedException ie) {
          throw new IOException("Interrupted when createClient", ie);
        }
        OpenStream openBlocks = new OpenStream(shuffleKey, location.getFileName(),
          startMapIndex, endMapIndex);
        long timeoutMs = RssConf.fetchChunkTimeoutMs(conf);
        ByteBuffer response = client.sendRpcSync(openBlocks.toByteBuffer(), timeoutMs);
        streamHandle = (StreamHandle) Message.decode(response);

        this.fetchChunkRetryCnt = fetchChunkRetryCnt;
        this.fetchChunkMaxRetry = fetchChunkMaxRetry;
        testFetch = RssConf.testFetchFailure(conf);
      }

      boolean hasNext() {
        return returnedChunks < streamHandle.numChunks;
      }

      ByteBuf next() throws IOException {
        checkException();
        if (chunkIndex < streamHandle.numChunks) {
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
        returnedChunks++;
        return chunk;
      }

      void close() {
        synchronized(this) {
          closed = true;
        }
        if (results.size() > 0) {
          results.forEach(res -> res.release());
        }
        results.clear();
      }

      private void fetchChunks() {
        final int inFlight = chunkIndex - returnedChunks;
        if (inFlight < maxInFlight) {
          final int toFetch = Math.min(maxInFlight - inFlight + 1,
            streamHandle.numChunks - chunkIndex);
          for (int i = 0; i < toFetch; i++) {
            if (testFetch && fetchChunkRetryCnt < fetchChunkMaxRetry - 1 && chunkIndex == 3) {
              callback.onFailure(chunkIndex, new IOException("Test fetch chunk failure"));
            } else {
              client.fetchChunk(streamHandle.streamId, chunkIndex, callback);
              chunkIndex++;
            }
          }
        }
      }

      private void checkException() throws IOException {
        IOException e = exception.get();
        if (e != null) {
          throw e;
        }
      }

      public PartitionLocation getLocation() {
        return location;
      }
    }
  }
}
