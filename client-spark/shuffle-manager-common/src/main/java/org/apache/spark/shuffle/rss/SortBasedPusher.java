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

package org.apache.spark.shuffle.rss;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.client.write.DataPusher;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.util.Utils;

public class SortBasedPusher extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(SortBasedPusher.class);

  private ShuffleInMemorySorter inMemSorter;
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();
  private MemoryBlock currentPage = null;
  private long pageCursor = -1;

  private final ShuffleClient rssShuffleClient;
  private final DataPusher dataPusher;
  private final int pushBufferSize;
  private final long PushThreshold;
  final int uaoSize = UnsafeAlignedOffset.getUaoSize();

  String appId;
  int shuffleId;
  int mapId;
  int attemptNumber;
  long taskAttemptId;
  int numMappers;
  int numPartitions;
  RssConf conf;
  Consumer<Integer> afterPush;
  LongAdder[] mapStatusLengths;

  public SortBasedPusher(
      TaskMemoryManager memoryManager,
      ShuffleClient rssShuffleClient,
      String appId,
      int shuffleId,
      int mapId,
      int attemptNumber,
      long taskAttemptId,
      int numMappers,
      int numPartitions,
      RssConf conf,
      Consumer<Integer> afterPush,
      LongAdder[] mapStatusLengths) throws IOException {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());

    this.rssShuffleClient = rssShuffleClient;

    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptNumber = attemptNumber;
    this.taskAttemptId = taskAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.conf = conf;
    this.afterPush = afterPush;
    this.mapStatusLengths = mapStatusLengths;

    dataPusher = new DataPusher(
      appId,
      shuffleId,
      mapId,
      attemptNumber,
      taskAttemptId,
      numMappers,
      numPartitions,
      conf,
      rssShuffleClient,
      this.afterPush,
      mapStatusLengths);

    pushBufferSize = RssConf.pushDataBufferSize(conf);
    PushThreshold = RssConf.sortPushThreshold(conf);

    inMemSorter = new ShuffleInMemorySorter(this, 4 * 1024 * 1024);
  }

  /**
   *
   * @return bytes of memory freed
   * @throws IOException
   */
  public long pushData() throws IOException {
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    byte[] dataBuf = new byte[pushBufferSize];
    int offSet = 0;
    int currentPartition = -1;
    while(sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert(partition >= currentPartition);
      if (partition != currentPartition) {
        if (currentPartition == -1) {
          currentPartition = partition;
        } else {
          int bytesWritten = rssShuffleClient.mergeData(
            appId,
            shuffleId,
            mapId,
            attemptNumber,
            currentPartition,
            dataBuf,
            0,
            offSet,
            numMappers,
            numPartitions
          );
          mapStatusLengths[currentPartition].add(bytesWritten);
          afterPush.accept(bytesWritten);
          currentPartition = partition;
          offSet = 0;
        }
      }
      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      int recordSize = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);

      if (offSet + recordSize > dataBuf.length) {
        dataPusher.addTask(partition, dataBuf, offSet);
        offSet = 0;
      }

      long recordReadPosition = recordOffsetInPage + uaoSize;
      Platform.copyMemory(
        recordPage, recordReadPosition, dataBuf, Platform.BYTE_ARRAY_OFFSET + offSet, recordSize);
      offSet += recordSize;
    }
    if (offSet > 0) {
      dataPusher.addTask(currentPartition, dataBuf, offSet);
    }

    long freedBytes = freeMemory();
    inMemSorter.reset();
    return freedBytes;
  }

  public void insertRecord(Object recordBase, long recordOffset, int recordSize,
                           int partitionId, boolean copySize)
    throws IOException {

    if (getUsed() > PushThreshold &&
        pageCursor + Utils.byteStringAsBytes("8k") >
          currentPage.getBaseOffset() + currentPage.size()) {
      logger.info("Memory Used across threshold, trigger push. Memory: " + getUsed() +
        ", currentPage size: " + currentPage.size());
      pushData();
    }

    growPointerArrayIfNecessary();
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    int required;
    // Need 4 or 8 bytes to store the record recordSize.
    if (copySize) {
      required = recordSize + 4 + uaoSize;
    } else {
      required = recordSize + uaoSize;
    }
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    if (copySize) {
      UnsafeAlignedOffset.putSize(base, pageCursor, recordSize + 4);
      pageCursor += uaoSize;
      Platform.putInt(base, pageCursor, Integer.reverseBytes(recordSize));
      pageCursor += 4;
      Platform.copyMemory(recordBase, recordOffset, base, pageCursor, recordSize);
      pageCursor += recordSize;
    } else {
      UnsafeAlignedOffset.putSize(base, pageCursor, recordSize);
      pageCursor += uaoSize;
      Platform.copyMemory(recordBase, recordOffset, base, pageCursor, recordSize);
      pageCursor += recordSize;
    }
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        logger.info("Pushdata in growPointerArrayIfNecessary, memory used " + getUsed());
        pushData();
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  @Override
  public long spill(long l, MemoryConsumer memoryConsumer) throws IOException {
    logger.info("Pushdata in spill, memory used " + getUsed());
    if (getUsed() != 0) {
      logger.info("Pushdata is not empty , do push.");
      return pushData();
    }
    return 0;
  }

  private long freeMemory() {
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
  }

  public void close() throws IOException {
    cleanupResources();
    dataPusher.waitOnTermination();
  }

  public long getUsed() {
    return super.getUsed();
  }
}
