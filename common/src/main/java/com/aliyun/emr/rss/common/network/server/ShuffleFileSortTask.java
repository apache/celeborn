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

package com.aliyun.emr.rss.common.network.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;
import sun.nio.ch.DirectBuffer;

import com.aliyun.emr.rss.common.unsafe.Platform;

public class ShuffleFileSortTask {
  private static Logger logger = LoggerFactory.getLogger(ShuffleFileSortTask.class);
  private File originFile;
  private String sortedFile;
  private String indexFile;
  private final long chunkSize;
  private final long maxInMemSortSize;
  private final long originFileLen;
  public static final String SORTED_SUFFIX = ".sorted";
  public static final String INDEX_SUFFIX = ".index";
  private SortCompleteCallback sortCompleteCallBack = null;

  public ShuffleFileSortTask(File originFile, long chunkSize, double maxRatio, long originFileLen) {
    this.originFile = originFile;
    sortedFile = originFile + SORTED_SUFFIX;
    indexFile = originFile + INDEX_SUFFIX;
    this.chunkSize = chunkSize;
    long maxDirectMem = VM.maxDirectMemory();
    this.maxInMemSortSize = (long) (maxDirectMem * maxRatio);
    this.originFileLen = originFileLen;
  }

  public void setSortCompleteCallback(SortCompleteCallback callback) {
    this.sortCompleteCallBack = callback;
  }

  public void sort() {
    int fileLen = (int) originFileLen;

    try {
      int batchHeaderLen = 16;
      byte[] batchHeader = new byte[batchHeaderLen];

      FileChannel originFileChannel = new FileInputStream(originFile).getChannel();
      FileChannel sortedFileChannel = new FileOutputStream(this.sortedFile).getChannel();

      Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();
      Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();

      boolean inMem = wholeFileInMemory();
      ByteBuffer headerBuf = null;
      ByteBuffer originFileBuf = null;
      if (inMem) {
        originFileBuf = ByteBuffer.allocateDirect((int) this.originFileLen);
        int readCount = originFileChannel.read(originFileBuf);
        if (readCount != fileLen) {
          logger.warn("Read shuffle file error, filelen {} read bytes {}", fileLen, readCount);
        }
        originFileChannel.close();
        originFileBuf.flip();
      } else {
        headerBuf = ByteBuffer.allocate(batchHeaderLen);
      }

      int index = 0;
      while (index != this.originFileLen) {
        final int blockStartIndex = index;
        if (inMem) {
          originFileBuf.get(batchHeader);
        } else {
          originFileChannel.read(headerBuf);
          batchHeader = headerBuf.array();
          headerBuf.rewind();
        }

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
        if (inMem) {
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
          if (inMem) {
            originFileBuf.limit(offset + length);
            originFileBuf.position(offset);
            fileIndex += sortedFileChannel.write(originFileBuf.slice());
          } else {
            fileIndex += originFileChannel.transferTo(offset, length, sortedFileChannel);
          }
        }
        sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
      }
      if (inMem) {
        cleanBuffer(originFileBuf);
      }

      sortedFileChannel.close();
      writeIndex(sortedBlockInfoMap);
    } catch (Exception e) {
      logger.error("Shuffle sort file failed , detail : {}", e);
    } finally {
      if (this.sortCompleteCallBack != null) {
        try {
          this.sortCompleteCallBack.onComplete();
        } catch (Exception e) {
          logger.warn("Invoke sort complete callback failed, detail:", e);
        } finally {
          synchronized (this) {
            this.notify();
          }
        }
      }
    }
  }

  public FileInfo resolve(int startMapIndex, int endMapIndex) {
    try (FileInputStream indexStream = new FileInputStream(indexFile)) {
      File indexFile = new File(this.indexFile);
      int indexSize = (int) indexFile.length();
      ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
      indexStream.getChannel().read(indexBuf);
      indexBuf.flip();
      Map<Integer, List<ShuffleBlockInfo>> indexMap = readIndex(indexBuf);
      return new FileInfo(new File(this.sortedFile),
        refreshChunkOffsets(startMapIndex, endMapIndex, indexMap));
    } catch (Exception e) {
      logger.error("Read sorted shuffle file error , detail : {}", e);
    }
    return null;
  }

  private void writeIndex(Map<Integer, List<ShuffleBlockInfo>> indexMap)
    throws IOException {
    File indexFile = new File(this.indexFile);
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
    cleanBuffer(indexBuf);
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

  private ArrayList<Long> refreshChunkOffsets(int startMapIndex, int endMapIndex,
    Map<Integer, List<ShuffleBlockInfo>> indexMap) {
    ArrayList<Long> sortedChunkOffset = new ArrayList<>();
    ShuffleBlockInfo lastBlock = null;
    logger.debug("Refresh offsets for file {} , startMapIndex {} endMapIndex {}",
      this.sortedFile, startMapIndex, endMapIndex);
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
        if (lastBlock != null) {
          long endChunkOffset = lastBlock.length + lastBlock.offset;
          if (!sortedChunkOffset.contains(endChunkOffset)) {
            sortedChunkOffset.add(endChunkOffset);
          }
        }
      }
    }
    return sortedChunkOffset;
  }

  private void cleanBuffer(ByteBuffer buf) {
    ((DirectBuffer) buf).cleaner().clean();
  }

  public String getOriginFile() {
    return this.originFile.getAbsolutePath();
  }

  public long getOriginFileLen() {
    return originFileLen;
  }

  public boolean wholeFileInMemory() {
    return this.originFileLen < this.maxInMemSortSize;
  }

  class ShuffleBlockInfo {
    protected int offset;
    protected int length;
  }

  public interface SortCompleteCallback {
    void onComplete();
  }
}
