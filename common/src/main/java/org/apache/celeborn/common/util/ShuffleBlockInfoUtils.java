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

package org.apache.celeborn.common.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.CompositeByteBuf;

public class ShuffleBlockInfoUtils {

  public static class ShuffleBlockInfo {
    public long offset;
    public long length;
  }

  public static List<Long> getChunkOffsetsFromShuffleBlockInfos(
      int startMapIndex,
      int endMapIndex,
      long fetchChunkSize,
      Map<Integer, List<ShuffleBlockInfo>> indexMap,
      boolean isInMemory) {
    List<Long> sortedChunkOffset = new ArrayList<>();
    ShuffleBlockInfo lastBlock = null;
    int maxMapIndex = endMapIndex;
    if (endMapIndex == Integer.MAX_VALUE) {
      // not a range read
      maxMapIndex = indexMap.keySet().stream().max(Integer::compareTo).get() + 1;
    }

    if (isInMemory) {
      long currentChunkOffset = 0;
      long lastChunkOffset = 0;
      // This sorted chunk offsets are used for fetch handler.
      // Sorted byte buf is a new composite byte buf containing the required data.
      // It will not reuse the old buffer of memory file, so the offset starts from 0.
      sortedChunkOffset.add(0l);
      for (int i = startMapIndex; i < maxMapIndex; i++) {
        List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
        if (blockInfos != null) {
          for (ShuffleBlockInfo info : blockInfos) {
            currentChunkOffset += info.length;
            if (currentChunkOffset - lastChunkOffset > fetchChunkSize) {
              lastChunkOffset = currentChunkOffset;
              sortedChunkOffset.add(currentChunkOffset);
            }
          }
        }
      }
      if (lastChunkOffset != currentChunkOffset) {
        sortedChunkOffset.add(currentChunkOffset);
      }
    } else {
      for (int i = startMapIndex; i < maxMapIndex; i++) {
        List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
        if (blockInfos != null) {
          for (ShuffleBlockInfo info : blockInfos) {
            if (sortedChunkOffset.size() == 0) {
              sortedChunkOffset.add(info.offset);
            }
            if (info.offset - sortedChunkOffset.get(sortedChunkOffset.size() - 1)
                >= fetchChunkSize) {
              sortedChunkOffset.add(info.offset);
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
    }
    return sortedChunkOffset;
  }

  public static Map<Integer, List<ShuffleBlockInfo>> parseShuffleBlockInfosFromByteBuffer(
      byte[] buffer) {
    return parseShuffleBlockInfosFromByteBuffer(ByteBuffer.wrap(buffer));
  }

  public static Map<Integer, List<ShuffleBlockInfo>> parseShuffleBlockInfosFromByteBuffer(
      ByteBuffer buffer) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();
    while (buffer.hasRemaining()) {
      int mapId = buffer.getInt();
      int count = buffer.getInt();
      List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        long offset = buffer.getLong();
        long length = buffer.getLong();
        ShuffleBlockInfo info = new ShuffleBlockInfo();
        info.offset = offset;
        info.length = length;
        blockInfos.add(info);
      }
      indexMap.put(mapId, blockInfos);
    }
    return indexMap;
  }

  public static void sliceSortedBufferByMapRange(
      int startMapIndex,
      int endMapIndex,
      Map<Integer, List<ShuffleBlockInfo>> indexMap,
      CompositeByteBuf sortedByteBuf,
      CompositeByteBuf targetByteBuf,
      long shuffleChunkSize) {
    int offset = 0;
    int length = 0;
    for (int i = startMapIndex; i < endMapIndex; i++) {
      List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
      if (blockInfos != null) {
        for (ShuffleBlockInfo blockInfo : blockInfos) {
          offset = (int) blockInfo.offset;
          length += (int) blockInfo.length;
          if (length - offset > shuffleChunkSize) {
            // Do not retain this buffer because this buffer
            // will be released when the fileinfo is released
            targetByteBuf.addComponent(sortedByteBuf.slice(offset, length));
            offset = (int) blockInfo.offset;
            length = 0;
          }
        }
      }
    }
    // process last small block
    if (length != 0) {
      // Do not retain this buffer because this buffer
      // will be released when the fileinfo is released
      targetByteBuf.addComponent(sortedByteBuf.slice(offset, length));
    }
  }
}
