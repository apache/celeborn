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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.unsafe.Platform;

public class ShuffleFileSorter {
  private static Logger logger = LoggerFactory.getLogger(ShuffleFileSorter.class);
  private File originFile;
  private String sortedFile;
  private String indexFile;
  private final long chunkSize;
  private PooledByteBufAllocator allocator;
  public static final String SORTED_SUFFIX = ".sorted";
  public static final String INDEX_SUFFIX = ".index";

  public ShuffleFileSorter(File originFile, long chunkSize, PooledByteBufAllocator allocator) {
    this.originFile = originFile;
    sortedFile = originFile + SORTED_SUFFIX;
    indexFile = originFile + INDEX_SUFFIX;
    this.chunkSize = chunkSize;
    this.allocator = allocator;
  }

  public void sort() {
    int fileLen = (int) originFile.length();
    MemoryTracker tracker = MemoryTracker.instance();
    boolean memoryReleased = false;

    try {
      tracker.reserveSortMemory(fileLen);

      while (!tracker.sortMemoryReady()) {
        logger.info("Waiting for reserving sort memory");
        Thread.sleep(20);
      }

      ByteBuf originFileBuf = allocator.buffer(fileLen);
      tracker.releaseSortMemory(fileLen);
      memoryReleased = true;
      FileInputStream fis = new FileInputStream(originFile);
      byte[] readBuf = new byte[1024];
      int size = fis.read(readBuf);
      while (size > 0) {
        originFileBuf.writeBytes(readBuf, 0, size);
        size = fis.read(readBuf);
      }

      fis.close();

      int batchHeaderLen = 16;
      byte[] batchHeader = new byte[batchHeaderLen];

      Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();

      int index = 0;
      while (index != originFileBuf.readableBytes()) {
        originFileBuf.getBytes(index, batchHeader);
        final int blockStartIndex = index;
        int mapId = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET);
        final int compressedSize = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12);

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
      }

      File sortedFile = new File(this.sortedFile);
      FileChannel sortedFileChannel = new FileOutputStream(sortedFile).getChannel();

      Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();
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
          fileIndex += sortedFileChannel.write(originFileBuf.slice(offset, length).nioBuffer());
        }
        sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
      }

      sortedFileChannel.close();
      originFileBuf.release();

      writeIndex(sortedBlockInfoMap);
    } catch (Exception e) {
      logger.error("Shuffle sort file failed , detail : {}", e);
    } finally {
      if (!memoryReleased) {
        tracker.releaseSortMemory(fileLen);
      }
    }
  }

  public FileInfo resolve(int startMapIndex, int endMapIndex) {
    try {
      File indexFile = new File(this.indexFile);
      int indexSize = (int) indexFile.length();
      ByteBuf indexBuf = allocator.buffer(indexSize);
      indexBuf.writeBytes(new FileInputStream(indexFile), indexSize);
      indexBuf.resetReaderIndex();
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

    ByteBuf indexBuf = allocator.buffer(indexSize);
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      int mapId = entry.getKey();
      List<ShuffleBlockInfo> list = entry.getValue();
      indexBuf.writeInt(mapId);
      indexBuf.writeInt(list.size());
      list.forEach(info -> {
        indexBuf.writeInt(info.offset);
        indexBuf.writeInt(info.length);
      });
    }

    indexFileChannel.write(indexBuf.nioBuffer());
    indexFileChannel.close();
    indexBuf.release();
  }

  private Map<Integer, List<ShuffleBlockInfo>> readIndex(ByteBuf indexBuf) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    while (indexBuf.isReadable()) {
      int mapId = indexBuf.readInt();
      int count = indexBuf.readInt();
      List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int offset = indexBuf.readInt();
        int length = indexBuf.readInt();
        ShuffleBlockInfo info = new ShuffleBlockInfo();
        info.offset = offset;
        info.length = length;
        blockInfos.add(info);
      }
      indexMap.put(mapId, blockInfos);
    }
    indexBuf.release();
    return indexMap;
  }

  private ArrayList<Long> refreshChunkOffsets(int startMapIndex, int endMapIndex,
    Map<Integer, List<ShuffleBlockInfo>> indexMap) {
    ArrayList<Long> sortedChunkOffset = new ArrayList<>();
    ShuffleBlockInfo lastBlock = null;
    logger.debug("refresh offsets for file {} , startMapIndex {} endMapIndex {}",
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

  class ShuffleBlockInfo {
    protected int offset;
    protected int length;
  }
}
