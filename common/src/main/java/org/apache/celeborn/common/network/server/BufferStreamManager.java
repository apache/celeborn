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

package org.apache.celeborn.common.network.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.protocol.BacklogAnnouncement;
import org.apache.celeborn.common.network.protocol.ReadData;
import org.apache.celeborn.common.network.server.memory.MemoryManager;
import org.apache.celeborn.common.util.Utils;

public class BufferStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(BufferStreamManager.class);
  private final AtomicLong nextStreamId;
  protected final ConcurrentHashMap<Long, StreamState> streams;
  protected final ConcurrentHashMap<Long, AtomicInteger> streamCredits;
  protected final ConcurrentHashMap<Long, MapDataPartition> servingStreams;
  protected final ConcurrentHashMap<FileInfo, MapDataPartition> activeMapPartitions;
  protected final MemoryManager memoryManager = MemoryManager.instance();
  protected final StorageFetcherPool storageFetcherPool = new StorageFetcherPool();
  protected int minReadBuffers;
  protected int maxReadBuffers;

  private Thread readScheduler =
      new Thread(
          new Runnable() {
            @Override
            public void run() {
              while (true) {
                if (streamCredits.isEmpty()) {
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                } else {
                  synchronized (this) {
                    streamCredits.forEach(
                        (streamId, credit) -> {
                          // logger.info("streamId: {}, loop credit: {}", streamId, credit.get());
                          if (credit.get() >= 1) {
                            StreamState streamState = streams.get(streamId);
                            if (streamState != null) {
                              memoryManager.requestReadBuffers(
                                  1,
                                  credit.get(),
                                  streamState.bufferSize,
                                  (allocatedBuffers, throwable) -> {
                                    if (throwable != null) {
                                      allocatedBuffers.forEach(memoryManager::recycleReadBuffer);
                                      throw new RuntimeException(throwable);
                                    }
                                    if (servingStreams.containsKey(streamId)) {
                                      servingStreams
                                          .get(streamId)
                                          .onBuffer(new ArrayDeque<>(allocatedBuffers));
                                    } else {
                                      logger.warn("Serving stream is null, streamId: {}", streamId);
                                    }
                                  });
                            }
                          }
                        });
                  }
                }
              }
            }
          },
          "map-partition-readScheduler");

  protected class StreamState {
    private Channel associatedChannel;
    private int bufferSize;

    public StreamState(Channel associatedChannel, int bufferSize) {
      this.associatedChannel = associatedChannel;
      this.bufferSize = bufferSize;
    }

    public Channel getAssociatedChannel() {
      return associatedChannel;
    }

    public int getBufferSize() {
      return bufferSize;
    }
  }

  public BufferStreamManager() {
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
    streamCredits = new ConcurrentHashMap<>();
    servingStreams = new ConcurrentHashMap<>();
    activeMapPartitions = new ConcurrentHashMap<>();
    // readScheduler.start();
  }

  public void setInitialReadBuffers(int minReadBuffers, int maxReadBuffers) {
    this.minReadBuffers = minReadBuffers;
    this.maxReadBuffers = maxReadBuffers;
  }

  public long registerStream(
      Channel channel,
      int bufferSize,
      int initialCredit,
      int startSubIndex,
      int endSubIndex,
      FileInfo fileInfo)
      throws IOException {
    long streamId = nextStreamId.getAndIncrement();
    streams.put(streamId, new StreamState(channel, bufferSize));

    MapDataPartition mapDataPartition =
        activeMapPartitions.computeIfAbsent(fileInfo, (f) -> new MapDataPartition(fileInfo));
    activeMapPartitions.put(fileInfo, mapDataPartition);

    mapDataPartition.addStream(streamId);
    mapDataPartition.setupDataPartitionReader(startSubIndex, endSubIndex, streamId);
    servingStreams.put(streamId, mapDataPartition);
    addCredit(initialCredit, streamId);

    logger.info("streamId: {}, fileInfo: {}", streamId, fileInfo);

    return streamId;
  }

  public void addCredit(int numCredit, long streamId) {
    logger.debug("streamId: {}, add credit: {}", streamId, numCredit);
    if (streamCredits.containsKey(streamId)) {
      streamCredits.get(streamId).getAndAdd(numCredit);
      MapDataPartition mapDataPartition = servingStreams.get(streamId);
      if (mapDataPartition != null) {
        DataPartitionReader streamReader = mapDataPartition.getStreamReader(streamId);
        if (streamReader != null) {
          streamReader.sendData();
        }
      }
    } else {
      // register stream
      streamCredits.put(streamId, new AtomicInteger(numCredit));
    }
  }

  public void connectionTerminated(Channel channel) {
    for (Map.Entry<Long, StreamState> entry : streams.entrySet()) {
      if (entry.getValue().getAssociatedChannel() == channel) {
        logger.info("connection closed, streamId: {}", entry.getKey());
        cleanResource(entry.getKey());
      }
    }
  }

  public synchronized void cleanResource(long streamId) {
    logger.info("clean stream:" + streamId);
    streams.remove(streamId);
    streamCredits.remove(streamId);
    FileInfo fileInfo = servingStreams.remove(streamId).fileInfo;
    MapDataPartition mapDataPartition = activeMapPartitions.get(fileInfo);
    mapDataPartition.removeStream(streamId);
    if (mapDataPartition.activeStreamIds.isEmpty()) {
      for (ByteBuf buffer : mapDataPartition.buffers) {
        memoryManager.recycleReadBuffer(buffer);
      }
      activeMapPartitions.remove(fileInfo);
    }
  }

  // this means active data partition
  protected class MapDataPartition {
    private final List<Long> activeStreamIds = new ArrayList<>();
    private final FileInfo fileInfo;
    private final Set<DataPartitionReader> readers = new HashSet<>();
    private final ExecutorService readExecutor;
    private final ConcurrentHashMap<Long, DataPartitionReader> streamReaders =
        new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    /** All available buffers can be used by the partition readers for reading. */
    protected Queue<ByteBuf> buffers;

    public MapDataPartition(FileInfo fileInfo) {
      this.fileInfo = fileInfo;
      readExecutor = storageFetcherPool.getExecutorPool(fileInfo.getFilePath());
    }

    public synchronized void setupDataPartitionReader(
        int startSubIndex, int endSubIndex, long streamId) throws IOException {
      DataPartitionReader dataPartitionReader =
          new DataPartitionReader(startSubIndex, endSubIndex, fileInfo, streamId);
      dataPartitionReader.open();
      // allocate resources when the first reader is registered
      boolean allocateResources = readers.isEmpty();
      readers.add(dataPartitionReader);
      streamReaders.put(streamId, dataPartitionReader);

      // create initial buffers for read
      if (allocateResources) {
        memoryManager.requestReadBuffers(
            minReadBuffers,
            maxReadBuffers,
            fileInfo.getBufferSize(),
            (allocatedBuffers, throwable) ->
                MapDataPartition.this.onBuffer(new LinkedBlockingDeque<>(allocatedBuffers)));
      }
    }

    public DataPartitionReader getStreamReader(long streamId) {
      return streamReaders.get(streamId);
    }

    // Read logic is executed on another thread.
    public void onBuffer(Queue<ByteBuf> buffers) {
      this.buffers = buffers;
      process();
    }

    public void recycle(ByteBuf buffer, Queue<ByteBuf> bufferQueue) {
      buffer.clear();
      bufferQueue.add(buffer);
      process();
    }

    public void process() {
      readExecutor.submit(
          () -> {
            // Key for IO schedule.
            PriorityQueue<DataPartitionReader> sortedReaders = new PriorityQueue<>(readers);
            while (buffers.size() > 0 && !sortedReaders.isEmpty()) {
              DataPartitionReader reader = sortedReaders.poll();
              try {
                if (!reader.readAndSend(buffers, (buffer) -> this.recycle(buffer, buffers))) {
                  readers.remove(reader);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }

    public synchronized void addStream(long streamId) {
      activeStreamIds.add(streamId);
    }

    public synchronized void removeStream(long streamId) {
      activeStreamIds.remove(streamId);
    }
  }

  private class Buffer {
    private ByteBuf byteBuf;
    private Consumer<ByteBuf> byteBufferConsumer;

    public Buffer(ByteBuf byteBuf, Consumer<ByteBuf> byteBufferConsumer) {
      this.byteBuf = byteBuf;
      this.byteBufferConsumer = byteBufferConsumer;
    }
  }

  // this is a specific partition reader
  protected class DataPartitionReader implements Comparable<DataPartitionReader> {
    /** Buffer for reading an index entry to memory. */
    private final ByteBuffer indexBuffer;

    /** Buffer for reading buffer header to memory. */
    private final ByteBuffer headerBuffer;
    /** First index (inclusive) of the target reduce partitions to be read. */
    private final int startPartitionIndex;

    /** Last index (inclusive) of the target reduce partitions to be read. */
    private final int endPartitionIndex;

    /** Number of data regions in the target file. */
    private int numRegions;

    /** Opened data file channel to read data from. */
    private FileChannel dataFileChannel;

    /** Opened index file channel to read index from. */
    private FileChannel indexFileChannel;

    /** Number of remaining reduce partitions to read in the current data region. */
    private int numRemainingPartitions;

    /** Current data region index to read data from. */
    private int currentDataRegion = -1;

    /** File offset in data file to read data from. */
    private long dataConsumingOffset;

    /** Number of remaining bytes to read from the current reduce partition. */
    private volatile long currentPartitionRemainingBytes;

    /** Whether this file reader is closed or not. A closed file reader can not read any data. */
    private boolean isClosed;

    private FileInfo fileInfo;
    private int INDEX_ENTRY_SIZE = 16;
    private long streamId;
    protected final Object lock = new Object();

    @GuardedBy("lock")
    protected final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /** Whether all the data has been successfully read or not. */
    @GuardedBy("lock")
    protected boolean isFinished;

    /** Whether this partition reader has been released or not. */
    @GuardedBy("lock")
    protected boolean isReleased;

    /** Exception causing the release of this partition reader. */
    @GuardedBy("lock")
    protected Throwable errorCause;

    /** Whether there is any error at the consumer side or not. */
    @GuardedBy("lock")
    protected boolean isError;

    public DataPartitionReader(
        int startPartitionIndex, int endPartitionIndex, FileInfo fileInfo, long streamId) {
      this.startPartitionIndex = startPartitionIndex;
      this.endPartitionIndex = endPartitionIndex;

      int indexBufferSize = 16 * (endPartitionIndex - startPartitionIndex + 1);
      this.indexBuffer = ByteBuffer.allocateDirect(indexBufferSize);

      this.headerBuffer = ByteBuffer.allocateDirect(16);
      this.streamId = streamId;

      this.fileInfo = fileInfo;
      this.isClosed = false;
    }

    public void open() throws IOException {
      this.dataFileChannel = new FileInputStream(fileInfo.getFile()).getChannel();
      this.indexFileChannel = new FileInputStream(fileInfo.getIndexPath()).getChannel();

      long indexFileSize = indexFileChannel.size();
      // index is (offset,length)
      long indexRegionSize = fileInfo.getNumReducerPartitions() * (long) INDEX_ENTRY_SIZE;
      this.numRegions = Utils.checkedDownCast(indexFileSize / indexRegionSize);

      updateConsumingOffset();
    }

    public long getIndexRegionSize() {
      return fileInfo.getNumReducerPartitions() * (long) INDEX_ENTRY_SIZE;
    }

    private void updateConsumingOffset() throws IOException {
      while (currentPartitionRemainingBytes == 0
          && (currentDataRegion < numRegions - 1 || numRemainingPartitions > 0)) {
        if (numRemainingPartitions <= 0) {
          ++currentDataRegion;
          numRemainingPartitions = endPartitionIndex - startPartitionIndex + 1;

          // read the target index entry to the target index buffer
          indexFileChannel.position(
              currentDataRegion * getIndexRegionSize()
                  + (long) startPartitionIndex * INDEX_ENTRY_SIZE);
          Utils.readBuffer(indexFileChannel, indexBuffer, indexBuffer.capacity());
        }

        // get the data file offset and the data size
        dataConsumingOffset = indexBuffer.getLong();
        currentPartitionRemainingBytes = indexBuffer.getLong();
        --numRemainingPartitions;

        logger.info(
            "readBuffer updateConsumingOffset, {},  {}, {}, {}",
            streamId,
            dataFileChannel.size(),
            dataConsumingOffset,
            currentPartitionRemainingBytes);

        // if these checks fail, the partition file must be corrupted
        if (dataConsumingOffset < 0
            || dataConsumingOffset + currentPartitionRemainingBytes > dataFileChannel.size()
            || currentPartitionRemainingBytes < 0) {
          throw new RuntimeException("File " + fileInfo.getFilePath() + " is corrupted");
        }
      }
    }

    public boolean readBuffer(ByteBuf buffer) throws IOException {
      try {
        dataFileChannel.position(dataConsumingOffset);

        int readSize =
            Utils.readBuffer(dataFileChannel, headerBuffer, buffer, headerBuffer.capacity());
        currentPartitionRemainingBytes -= readSize;

        logger.info(
            "readBuffer data: {}, {}, {}, {}, {}, {}",
            streamId,
            currentPartitionRemainingBytes,
            readSize,
            dataConsumingOffset,
            Utils.getShortFormattedFileName(fileInfo),
            System.identityHashCode(buffer));

        // if this check fails, the partition file must be corrupted
        if (currentPartitionRemainingBytes < 0) {
          throw new RuntimeException("File is corrupted");
        } else if (currentPartitionRemainingBytes == 0) {
          logger.info(
              "readBuffer end, {},  {}, {}, {}",
              streamId,
              dataFileChannel.size(),
              dataConsumingOffset,
              currentPartitionRemainingBytes);
          int prevDataRegion = currentDataRegion;
          updateConsumingOffset();
          return prevDataRegion == currentDataRegion && currentPartitionRemainingBytes > 0;
        }

        dataConsumingOffset = dataFileChannel.position();

        logger.info(
            "readBuffer run: {}, {}, {}, {}",
            streamId,
            dataFileChannel.size(),
            dataConsumingOffset,
            currentPartitionRemainingBytes);
        return true;
      } catch (Throwable throwable) {
        logger.debug("Failed to read partition file.", throwable);
        isReleased = true;
        throw throwable;
      }
    }

    public boolean hasRemaining() {
      return currentPartitionRemainingBytes > 0;
    }

    public synchronized boolean readAndSend(Queue<ByteBuf> bufferQueue, Consumer<ByteBuf> consumer)
        throws IOException {
      boolean hasReaming = hasRemaining();
      boolean continueReading = hasReaming;
      int numDataBuffers = 0;
      while (continueReading) {

        ByteBuf buffer = bufferQueue.poll();
        if (buffer == null) {
          break;
        }

        try {
          continueReading = readBuffer(buffer);
        } catch (Throwable throwable) {
          memoryManager.recycleReadBuffer(buffer);
          throw throwable;
        }

        hasReaming = hasRemaining();
        addBuffer(buffer, hasReaming, consumer);
        ++numDataBuffers;
      }

      if (numDataBuffers > 0) {
        notifyBacklog(numDataBuffers);
      }

      if (!hasReaming) {
        closeReader();
      }

      return hasReaming;
    }

    protected void addBuffer(ByteBuf buffer, boolean hasRemaining, Consumer<ByteBuf> consumer) {
      if (buffer == null) {
        return;
      }
      final boolean recycleBuffer;
      boolean notifyDataAvailable = false;
      final Throwable throwable;

      synchronized (lock) {
        recycleBuffer = isReleased || isFinished || isError;
        throwable = errorCause;
        isFinished = !hasRemaining;

        if (!recycleBuffer) {
          notifyDataAvailable = buffersRead.isEmpty();
          buffersRead.add(new Buffer(buffer, consumer));
        }
      }

      if (recycleBuffer) {
        memoryManager.recycleReadBuffer(buffer);
        throw new RuntimeException("Partition reader has been failed or finished.", throwable);
      }

      if (notifyDataAvailable) {
        sendData();
      }
    }

    public synchronized void sendData() {
      while (!buffersRead.isEmpty()) {
        if (streamCredits.get(streamId).get() > 0) {
          Buffer readBuf = buffersRead.poll();
          logger.debug(
              "send "
                  + readBuf.byteBuf.readableBytes()
                  + " to stream "
                  + streamId
                  + ", fileInfo: "
                  + Utils.getShortFormattedFileName(fileInfo));
          ReadData readData = new ReadData(streamId, buffersRead.size(), 0, readBuf.byteBuf);
          streams
              .get(streamId)
              .associatedChannel
              .writeAndFlush(readData)
              .addListener(
                  (ChannelFutureListener)
                      future -> {
                        if (!future.isSuccess()) {
                          isError = true;
                          errorCause = future.cause();
                          throw new RuntimeException(future.cause());
                        } else {
                          // we have manually controlled the lifecycle of a buffer,
                          // so here is no need to release
                          // memoryManager.recycleReadBuffer(readBuf.byteBuf);
                        }
                        logger.debug(
                            "recycle "
                                + readBuf.byteBuf.readableBytes()
                                + ","
                                + System.identityHashCode(readBuf.byteBuf)
                                + " to stream "
                                + streamId
                                + ", fileInfo: "
                                + Utils.getShortFormattedFileName(fileInfo));
                        readBuf.byteBufferConsumer.accept(readBuf.byteBuf);
                      });
          logger.info(
              "streamId: {}, decrement credit: {}",
              streamId,
              streamCredits.get(streamId).decrementAndGet());
        } else {
          logger.info("streamId: {}, no credit: {}", streamId, streamCredits.get(streamId));
          // no credit
          break;
        }
      }

      logger.info(
          "streamId: {}, remaining: {}, bufferSize: {}",
          streamId,
          currentPartitionRemainingBytes,
          buffersRead.size());

      if (isClosed && buffersRead.isEmpty()) {
        cleanResource(streamId);
      }
    }

    public void closeReader() {
      isClosed = true;
      IOUtils.closeQuietly(indexFileChannel);
      IOUtils.closeQuietly(dataFileChannel);
      logger.info("Closed read for stream {}", this.streamId);
    }

    protected void notifyBacklog(int backlog) {
      StreamState streamState = streams.get(streamId);
      if (streamState == null) {
        throw new RuntimeException("StreamId " + streamId + " should not be null");
      }
      streamState.associatedChannel.writeAndFlush(new BacklogAnnouncement(streamId, backlog));
    }

    public long getPriority() {
      return dataConsumingOffset;
    }

    @Override
    public int compareTo(DataPartitionReader that) {
      return Long.compare(getPriority(), that.getPriority());
    }
  }

  class StorageFetcherPool {
    private final HashMap<String, ExecutorService> executorPools = new HashMap<>();

    public ExecutorService getExecutorPool(String mountPoint) {
      // simplified implementation, the thread number will be adjustable.
      // mount point might be UNKNOWN
      return executorPools.computeIfAbsent(
          mountPoint,
          k ->
              Executors.newFixedThreadPool(
                  1, new ThreadFactoryBuilder().setNameFormat("reader-thread-%d").build()));
    }
  }
}
