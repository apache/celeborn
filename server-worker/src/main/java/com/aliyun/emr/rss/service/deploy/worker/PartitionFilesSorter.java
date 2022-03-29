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

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;
import sun.nio.ch.DirectBuffer;

import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.unsafe.Platform;
import com.aliyun.emr.rss.common.util.ThreadUtils;

public class PartitionFilesSorter {
  private static Logger logger = LoggerFactory.getLogger(PartitionFilesSorter.class);
  public static final String SORTED_SUFFIX = ".sorted";
  public static final String INDEX_SUFFIX = ".index";

  private final ConcurrentHashMap<String, Set<String>> sortedShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<String>> sortingShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Map<String, Map<Integer, List<ShuffleBlockInfo>>>>
    cachedIndexMaps = new ConcurrentHashMap<>();
  private final LinkedBlockingQueue<FileSorter> shuffleSortTaskDeque = new LinkedBlockingQueue<>();
  protected final long sortTimeout;
  protected final long fetchBlockSize;
  protected final double maxSingleSortFileRatio;
  protected final long maxSingleFileInMemSize;
  protected final long reserveMemoryForOffHeapSort;

  private final ExecutorService fileSorterExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "worker-file-sorter-execute", Math.max(Runtime.getRuntime().availableProcessors(), 8), 120);
  private final Thread fileSorterSchedulerThread;

  PartitionFilesSorter(MemoryTracker memoryTracker, long sortTimeOut, long fetchBlockSize,
    double maxSingleSortFileRatio, long reserveMemoryForOffHeapSort) {
    this.sortTimeout = sortTimeOut;
    this.fetchBlockSize = fetchBlockSize;
    this.maxSingleSortFileRatio = maxSingleSortFileRatio;
    this.maxSingleFileInMemSize = (long) (maxSingleSortFileRatio * VM.maxDirectMemory());
    this.reserveMemoryForOffHeapSort = reserveMemoryForOffHeapSort;

    fileSorterSchedulerThread = new Thread(() -> {
      try {
        while (true) {
          FileSorter task = shuffleSortTaskDeque.take();
          if (task.inMemSort()) {
            memoryTracker.reserveSortMemory(task.getOriginFileLen());
          } else {
            memoryTracker.reserveSortMemory(reserveMemoryForOffHeapSort);
          }
          while (!memoryTracker.sortMemoryReady()) {
            Thread.sleep(20);
          }
          fileSorterExecutors.submit(() -> {
            task.sort();
            if (task.inMemSort()) {
              memoryTracker.releaseSortMemory(task.getOriginFileLen());
            } else {
              memoryTracker.releaseSortMemory(reserveMemoryForOffHeapSort);
            }
            logger.info("sort {} complete", task.getOriginFile());
          });
        }
      } catch (InterruptedException e) {
        logger.warn("sort file failed, detail :", e);
      }
    });
    fileSorterSchedulerThread.start();
  }

  public FileInfo openStream(String shuffleKey, String fileName, FileWriter fileWriter,
    int startMaxIndex, int endMapIndex) {
    if (endMapIndex == Integer.MAX_VALUE) {
      return new FileInfo(fileWriter.getFile(), fileWriter.getChunkOffsets());
    } else {
      String shuffleSortKey = shuffleKey + "-" + fileName;

      Set<String> sorted =
        sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
      Set<String> sorting =
        sortingShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());

      boolean sortInMem = fileWriter.getFileLength() < maxSingleFileInMemSize;

      FileSorter fileSorter = new FileSorter(fileWriter.getFile(), fetchBlockSize,
        fileWriter.getFileLength(), sortInMem, shuffleSortKey, shuffleKey);

      synchronized (sorted) {
        if (sorted.contains(shuffleSortKey)) {
          return fileSorter.resolve(startMaxIndex, endMapIndex);
        }
      }

      synchronized (sorting) {
        if (!sorting.contains(shuffleSortKey)) {
          sorting.add(shuffleSortKey);
          try {
            shuffleSortTaskDeque.put(fileSorter);
          } catch (InterruptedException e) {
            logger.info("scheduler thread is interrupted means worker is shutting down.");
          }
        }
      }

      long sortStartTime = System.currentTimeMillis();
      while (!fileSorter.sortComplete) {
        try {
          Thread.sleep(10);
          if (System.currentTimeMillis() - sortStartTime > sortTimeout) {
            logger.error("waiting file {} to sort timeout", shuffleSortKey);
            return null;
          }
          if (fileSorter.exception != null) {
            logger.error("sort {} failed ", fileSorter.getOriginFile(), fileSorter.exception);
            return null;
          }
        } catch (InterruptedException e) {
          logger.info("scheduler thread is interrupted means worker is shutting down.");
          return null;
        }
      }
      return fileSorter.resolve(startMaxIndex, endMapIndex);
    }
  }

  public void cleanup(HashSet<String> expirtedShuffleKeys) {
    for (String expirtedShuffleKey : expirtedShuffleKeys) {
      sortingShuffleFiles.remove(expirtedShuffleKey);
      sortedShuffleFiles.remove(expirtedShuffleKey);
      cachedIndexMaps.remove(expirtedShuffleKey);
    }
  }

  private void cleanBuffer(ByteBuffer buf) {
    ((DirectBuffer) buf).cleaner().clean();
  }

  public void close() {
    fileSorterSchedulerThread.interrupt();
    fileSorterExecutors.shutdownNow();
    cachedIndexMaps.clear();
  }

  class ShuffleBlockInfo {
    protected int offset;
    protected int length;
  }

  class FileSorter {
    private final File originFile;
    private final String sortedFileName;
    private final String indexFileName;
    private final long chunkSize;
    private final long originFileLen;
    private final String shuffleFileId;
    private final String shuffleKey;
    private final boolean inMemSort;
    private boolean sortComplete;
    private Exception exception;

    FileSorter(File originFile, long chunkSize, long originFileLen, boolean inMemSort,
      String shuffleFileId, String shuffleKey) {
      this.originFile = originFile;
      this.sortedFileName = originFile.getAbsolutePath() + SORTED_SUFFIX;
      this.indexFileName = originFile.getAbsolutePath() + INDEX_SUFFIX;
      this.chunkSize = chunkSize;
      this.originFileLen = originFileLen;
      this.inMemSort = inMemSort;
      this.shuffleFileId = shuffleFileId;
      this.shuffleKey = shuffleKey;
    }

    public void sort() {
      try (FileChannel originFileChannel = new FileInputStream(originFile).getChannel();
           FileChannel sortedFileChannel = new FileOutputStream(sortedFileName).getChannel();) {
        int batchHeaderLen = 16;
        byte[] batchHeader = new byte[batchHeaderLen];

        Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();
        Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();

        ByteBuffer headerBuf = null;
        ByteBuffer originFileBuf = null;
        if (inMemSort) {
          originFileBuf = ByteBuffer.allocateDirect((int) originFileLen);
          readFully(originFileChannel, originFileBuf);
          originFileChannel.close();
          originFileBuf.flip();
        } else {
          headerBuf = ByteBuffer.allocate(batchHeaderLen);
        }

        int index = 0;
        while (index != originFileLen) {
          final int blockStartIndex = index;
          if (inMemSort) {
            originFileBuf.get(batchHeader);
          } else {
            readFully(originFileChannel, headerBuf);
            batchHeader = headerBuf.array();
            headerBuf.rewind();
          }

          // header is 4 integers: mapId, attemptId, nextBatchId, compressedBlockTotalSize
          int mapId = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET);
          final int compressedSize = Platform.getInt(batchHeader,
            Platform.BYTE_ARRAY_OFFSET + 12);

          List<ShuffleBlockInfo> singleMapIdShuffleBlockList =
            originShuffleBlockInfos.computeIfAbsent(mapId,
              v -> new ArrayList<>());
          ShuffleBlockInfo blockInfo = new ShuffleBlockInfo();
          blockInfo.offset = blockStartIndex;
          blockInfo.length = compressedSize + 16;
          singleMapIdShuffleBlockList.add(blockInfo);

          index += batchHeaderLen + compressedSize;
          if (inMemSort) {
            originFileBuf.position(index);
          } else {
            originFileChannel.position(index);
          }
        }

        int fileIndex = 0;
        for (Map.Entry<Integer, List<ShuffleBlockInfo>>
               originBlockInfoEntry : originShuffleBlockInfos.entrySet()) {
          int mapId = originBlockInfoEntry.getKey();
          List<ShuffleBlockInfo> originShuffleBlocks = originBlockInfoEntry.getValue();
          List<ShuffleBlockInfo> sortedShuffleBlocks = new ArrayList<>();
          for (ShuffleBlockInfo blockInfo : originShuffleBlocks) {
            int offset = blockInfo.offset;
            int length = blockInfo.length;
            ShuffleBlockInfo sortedBlock = new ShuffleBlockInfo();
            sortedBlock.offset = fileIndex;
            sortedBlock.length = length;
            sortedShuffleBlocks.add(sortedBlock);
            if (inMemSort) {
              originFileBuf.limit(offset + length);
              originFileBuf.position(offset);
              fileIndex += sortedFileChannel.write(originFileBuf.slice());
            } else {
              fileIndex += transferFully(originFileChannel, sortedFileChannel, offset, length);
            }
          }
          sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
        }
        if (inMemSort) {
          cleanBuffer(originFileBuf);
        }

        sortedFileChannel.close();
        if (!originFile.delete()) {
          logger.warn("clean origin file failed, origin file is : {}",
            originFile.getAbsolutePath());
        }
        writeIndex(sortedBlockInfoMap);

        sortedShuffleFiles.get(shuffleKey).add(shuffleFileId);
        sortingShuffleFiles.get(shuffleKey).remove(shuffleFileId);
        sortComplete = true;
      } catch (Exception e) {
        exception = e;
      }
    }

    public FileInfo resolve(int startMapIndex, int endMapIndex) {
      Map<Integer, List<ShuffleBlockInfo>> indexMap = null;
      if (cachedIndexMaps.containsKey(shuffleKey) &&
            cachedIndexMaps.get(shuffleKey).containsKey(shuffleFileId)) {
        indexMap = cachedIndexMaps.get(shuffleKey).get(shuffleFileId);
      } else {
        try (FileInputStream indexStream = new FileInputStream(indexFileName)) {
          File indexFile = new File(indexFileName);
          int indexSize = (int) indexFile.length();
          ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
          readFully(indexStream.getChannel(), indexBuf);
          indexBuf.rewind();
          indexMap = readIndex(indexBuf);
          Map<String, Map<Integer, List<ShuffleBlockInfo>>> cacheMap =
            cachedIndexMaps.computeIfAbsent(shuffleKey, v -> new ConcurrentHashMap<>());
          cacheMap.put(shuffleFileId, indexMap);
        } catch (Exception e) {
          logger.error("Read sorted shuffle file error , detail : ", e);
          return null;
        }
      }
      return new FileInfo(new File(sortedFileName),
        getChunkOffsets(startMapIndex, endMapIndex, indexMap));
    }

    private void writeIndex(Map<Integer, List<ShuffleBlockInfo>> indexMap)
      throws IOException {
      File indexFile = new File(indexFileName);
      FileChannel indexFileChannel = new FileOutputStream(indexFile).getChannel();

      int indexSize = 0;
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
        indexSize += 8;
        indexSize += entry.getValue().size() * 8;
      }

      ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
        int mapId = entry.getKey();
        List<ShuffleBlockInfo> list = entry.getValue();
        indexBuf.putInt(mapId);
        indexBuf.putInt(list.size());
        list.forEach(info -> {
          indexBuf.putInt(info.offset);
          indexBuf.putInt(info.length);
        });
      }

      indexBuf.flip();
      indexFileChannel.write(indexBuf);
      indexFileChannel.close();
    }

    private Map<Integer, List<ShuffleBlockInfo>> readIndex(ByteBuffer indexBuf) {
      Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();
      while (indexBuf.hasRemaining()) {
        int mapId = indexBuf.getInt();
        int count = indexBuf.getInt();
        List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          int offset = indexBuf.getInt();
          int length = indexBuf.getInt();
          ShuffleBlockInfo info = new ShuffleBlockInfo();
          info.offset = offset;
          info.length = length;
          blockInfos.add(info);
        }
        indexMap.put(mapId, blockInfos);
      }
      return indexMap;
    }

    private void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
      while (buffer.hasRemaining()) {
        if (-1 == channel.read(buffer)) {
          throw new IOException("Unexpected EOF, file name : " + originFile.getAbsolutePath());
        }
      }
    }

    private int transferFully(FileChannel originChannel, FileChannel targetChannel,
      int offset, int length) throws IOException {
      int transferedSize = 0;
      while (transferedSize != length) {
        transferedSize += originChannel.transferTo(offset + transferedSize,
          length - transferedSize, targetChannel);
      }
      return transferedSize;
    }

    private ArrayList<Long> getChunkOffsets(int startMapIndex, int endMapIndex,
      Map<Integer, List<ShuffleBlockInfo>> indexMap) {
      ArrayList<Long> sortedChunkOffset = new ArrayList<>();
      ShuffleBlockInfo lastBlock = null;
      logger.debug("Refresh offsets for file {} , startMapIndex {} endMapIndex {}",
        sortedFileName, startMapIndex, endMapIndex);
      for (int i = startMapIndex; i < endMapIndex; i++) {
        List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
        if (blockInfos != null) {
          for (ShuffleBlockInfo info : blockInfos) {
            if (sortedChunkOffset.size() == 0) {
              sortedChunkOffset.add((long) info.offset);
            }
            if (info.offset - sortedChunkOffset.get(sortedChunkOffset.size() - 1) > chunkSize) {
              sortedChunkOffset.add((long) info.offset);
            }
            lastBlock = info;
          }
        }
      }
      if (lastBlock != null) {
        long endChunkOffset = lastBlock.length + lastBlock.offset;
        if (!sortedChunkOffset.contains(endChunkOffset)) {
          sortedChunkOffset.add(endChunkOffset);
        }
      }
      return sortedChunkOffset;
    }

    public String getOriginFile() {
      return originFile.getAbsolutePath();
    }

    public long getOriginFileLen() {
      return originFileLen;
    }

    public boolean inMemSort() {
      return inMemSort;
    }

  }
}
