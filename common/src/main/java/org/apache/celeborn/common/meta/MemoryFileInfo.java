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

package org.apache.celeborn.common.meta;

import java.util.*;

import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.util.ShuffleBlockInfoUtils.ShuffleBlockInfo;

public class MemoryFileInfo extends FileInfo {
  Logger logger = LoggerFactory.getLogger(MemoryFileInfo.class);
  private CompositeByteBuf buffer;
  private CompositeByteBuf sortedBuffer;
  private Map<Integer, List<ShuffleBlockInfo>> sortedIndexes;

  public MemoryFileInfo(
      UserIdentifier userIdentifier, boolean partitionSplitEnabled, FileMeta fileMeta) {
    super(userIdentifier, partitionSplitEnabled, fileMeta);
  }

  // This constructor is only used in partition sorter for temp new memory file
  public MemoryFileInfo(
      UserIdentifier userIdentifier,
      boolean partitionSplitEnabled,
      FileMeta fileMeta,
      CompositeByteBuf buffer) {
    super(userIdentifier, partitionSplitEnabled, fileMeta);
    this.buffer = buffer;
  }

  public CompositeByteBuf getBuffer() {
    return buffer;
  }

  public void setBuffer(CompositeByteBuf buffer) {
    this.buffer = buffer;
  }

  public CompositeByteBuf getSortedBuffer() {
    return sortedBuffer;
  }

  public void setSortedBuffer(CompositeByteBuf sortedBuffer) {
    this.sortedBuffer = sortedBuffer;
  }

  public Map<Integer, List<ShuffleBlockInfo>> getSortedIndexes() {
    return sortedIndexes;
  }

  public void setSortedIndexes(Map<Integer, List<ShuffleBlockInfo>> sortedIndexes) {
    this.sortedIndexes = sortedIndexes;
  }

  public int releaseMemoryBuffers() {
    int bufferSize = 0;
    if (buffer != null) {
      bufferSize = buffer.writerIndex();
      buffer.release();
    }
    logger.info("Memory File Info {} expire, removed {}", this, bufferSize);
    return bufferSize;
  }

  @Override
  public String getFilePath() {
    return "";
  }
}
