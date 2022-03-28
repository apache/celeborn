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
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final ConcurrentHashMap<String, Map<String, ByteBuffer>> cachedIndexes =
    new ConcurrentHashMap<>();
  private final LinkedBlockingQueue<FileSorter> shuffleSortTaskDeque = new LinkedBlockingQueue<>();
  private final MemoryTracker memoryTracker;
  protected final long sortTimeout;
  protected final long fetchBlockSize;
  protected final double maxSingleSortFileRatio;
  protected final long maxSingleFileInMemSize;
  protected final long reserveMemoryForOffHeapSort;

  private ExecutorService fileSorterExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "worker-file-sorter-execute", Math.max(Runtime.getRuntime().availableProcessors(), 8), 120);
  private Thread fileSorterSchedulerThread;

  public PartitionFilesSorter(MemoryTracker memoryTracker, long sortTimeOut, long fetchBlockSize,
    double maxSingleSortFileRatio, long reserveMemoryForOffHeapSort) {
    this.memoryTracker = memoryTracker;
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
            memoryTracker.reserveSortMemory(this.reserveMemoryForOffHeapSort);
          }
          while (!memoryTracker.sortMemoryReady()) {
            Thread.sleep(20);
          }
          fileSorterExecutors.submit(() -> {
            task.sort();
            if (task.inMemSort()) {
              memoryTracker.releaseSortMemory(task.getOriginFileLen());
            } else {
              memoryTracker.releaseSortMemory(this.reserveMemoryForOffHeapSort);
            }
            logger.info("sort {} complete", task.getOriginFile());
          });
        }
      } catch (InterruptedException e) {
        logger.warn("sort file failed, detail : {}", e);
      }
    });
    fileSorterSchedulerThread.start();
  }

  public FileInfo openStream(String shuffleKey, String fileName, FileWriter fileWriter,
    int startMaxIndex, int endMapIndex) {
    FileInfo fileInfo = null;
    if (endMapIndex == Integer.MAX_VALUE) {
      fileInfo = new FileInfo(fileWriter.getFile(), fileWriter.getChunkOffsets());
    } else {
      String shuffleSortKey = shuffleKey + "-" + fileName;

      Set<String> shuffleBoundSortedShuffleFiles =
        sortedShuffleFiles.compute(shuffleKey, (k, v) -> {
          if (v == null) {
            v = ConcurrentHashMap.newKeySet();
          }
          return v;
        });
      Set<String> shuffleBoundSortingShuffleFiles =
        sortingShuffleFiles.compute(shuffleKey, (k, v) -> {
          if (v == null) {
            v = ConcurrentHashMap.newKeySet();
          }
          return v;
        });

      boolean sortInMem = fileWriter.getFileLength() < this.maxSingleFileInMemSize;

      FileSorter fileSorter = new FileSorter(fileWriter.getFile(), fetchBlockSize,
        fileWriter.getFileLength(), null, sortInMem, shuffleSortKey, shuffleKey);

      synchronized (shuffleBoundSortedShuffleFiles) {
        if (shuffleBoundSortedShuffleFiles.contains(shuffleSortKey)) {
          fileInfo = fileSorter.resolve(startMaxIndex, endMapIndex);
        }
      }

      synchronized (shuffleBoundSortingShuffleFiles) {
        if (!shuffleBoundSortedShuffleFiles.contains(shuffleSortKey)) {
          shuffleBoundSortedShuffleFiles.add(shuffleSortKey);
          AtomicBoolean sortSuccess = new AtomicBoolean(false);
          SortCallback callback = new SortCallback() {
            @Override
            public void onSuccess() {
              shuffleBoundSortingShuffleFiles.remove(shuffleSortKey);
              sortSuccess.set(true);
              synchronized (shuffleBoundSortedShuffleFiles) {
                shuffleBoundSortedShuffleFiles.add(shuffleSortKey);
              }
            }

            @Override
            public void onFailure() {
              shuffleBoundSortingShuffleFiles.remove(shuffleSortKey);
            }
          };
          fileSorter.setSortCallBack(callback);

          try {
            shuffleSortTaskDeque.put(fileSorter);
          } catch (InterruptedException e) {
            logger.info("scheduler thread is interrupted means worker is shutting down.");
          }

          synchronized (fileSorter) {
            try {
              fileSorter.wait(this.sortTimeout);
            } catch (InterruptedException e) {
              logger.warn("sort {} timeout, detail:{}", fileSorter.getOriginFile(), e);
            }
          }
          if (sortSuccess.get()) {
            fileInfo = fileSorter.resolve(startMaxIndex, endMapIndex);
          }
        } else {
          long startWaitTime = System.currentTimeMillis();
          while (!shuffleBoundSortedShuffleFiles.contains(shuffleSortKey)) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              logger.warn("sort {} timeout, detail:{}", fileName, e);
            }
            if (System.currentTimeMillis() - startWaitTime > this.sortTimeout) {
              logger.error("waiting file {} to be sort timeout", shuffleSortKey);
            }
          }
          fileInfo = fileSorter.resolve(startMaxIndex, endMapIndex);
        }
      }
    }
    return fileInfo;
  }

  public void cleanup(HashSet<String> expirtedShuffleKeys) {
    for (String expirtedShuffleKey : expirtedShuffleKeys) {
      sortingShuffleFiles.remove(expirtedShuffleKey);
      sortedShuffleFiles.remove(expirtedShuffleKey);
      Map<String, ByteBuffer> cacheMaps = cachedIndexes.remove(expirtedShuffleKey);
      cacheMaps.forEach((k, v) -> cleanBuffer(v));
    }
  }

  private void cleanBuffer(ByteBuffer buf) {
    if (buf != null) {
      ((DirectBuffer) buf).cleaner().clean();
      buf = null;
    }
  }

  public void close() {
    fileSorterSchedulerThread.interrupt();
    fileSorterExecutors.shutdownNow();
    cachedIndexes.forEach((k, v) -> v.forEach((a, b) -> cleanBuffer(b)));
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

    private SortCallback sortCallBack;
    private final boolean inMemSort;

    FileSorter(File originFile, long chunkSize, long originFileLen,
      SortCallback sortCallBack, boolean inMemSort, String shuffleFileId, String shuffleKey) {
      this.originFile = originFile;
      this.sortedFileName = this.originFile.getAbsolutePath() + SORTED_SUFFIX;
      this.indexFileName = this.originFile.getAbsolutePath() + INDEX_SUFFIX;
      this.chunkSize = chunkSize;
      this.originFileLen = originFileLen;
      this.sortCallBack = sortCallBack;
      this.inMemSort = inMemSort;
      this.shuffleFileId = shuffleFileId;
      this.shuffleKey = shuffleKey;
    }

    public void setSortCallBack(SortCallback callback) {
      this.sortCallBack = callback;
    }

    public void sort() {
      try {
        int batchHeaderLen = 16;
        byte[] batchHeader = new byte[batchHeaderLen];

        FileChannel originFileChannel = new FileInputStream(originFile).getChannel();
        FileChannel sortedFileChannel = new FileOutputStream(sortedFileName).getChannel();

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

          originShuffleBlockInfos.compute(mapId, (k, v) -> {
            ShuffleBlockInfo blockInfo = new ShuffleBlockInfo();
            blockInfo.offset = blockStartIndex;
            blockInfo.length = compressedSize + 16;
            if (v == null) {
              v = new ArrayList<>();
            }
            v.add(blockInfo);
            return v;
          });
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
        sortCallBack.onSuccess();
      } catch (Exception e) {
        logger.error("Shuffle sort file failed , detail :", e);
        sortCallBack.onFailure();
      } finally {
        synchronized (this) {
          notify();
        }
      }
    }

    public FileInfo resolve(int startMapIndex, int endMapIndex) {
      ByteBuffer indexBuf = null;
      if (cachedIndexes.containsKey(shuffleKey) &&
            cachedIndexes.get(shuffleKey).containsKey(shuffleFileId)) {
        indexBuf = cachedIndexes.get(shuffleKey).get(shuffleFileId);
        indexBuf.flip();
      } else {
        try (FileInputStream indexStream = new FileInputStream(indexFileName)) {
          File indexFile = new File(indexFileName);
          int indexSize = (int) indexFile.length();
          indexBuf = ByteBuffer.allocateDirect(indexSize);
          readFully(indexStream.getChannel(), indexBuf);
          indexBuf.flip();
        } catch (Exception e) {
          logger.error("Read sorted shuffle file error , detail : {}", e);
          return null;
        }
      }
      Map<Integer, List<ShuffleBlockInfo>> indexMap = readIndex(indexBuf);
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

      Map<String, ByteBuffer> cacheMap = cachedIndexes.compute(shuffleKey, (k, v) -> {
        if (v == null) {
          v = new ConcurrentHashMap<>();
        }
        return v;
      });

      cacheMap.put(shuffleFileId, indexBuf);

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
      cleanBuffer(indexBuf);
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

  public interface SortCallback {
    void onSuccess();

    void onFailure();
  }
}
