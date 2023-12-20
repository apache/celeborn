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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReduceFileMeta implements FileMeta {
  private List<Long> chunkOffsets;
  private AtomicBoolean sorted = new AtomicBoolean(false);

  public ReduceFileMeta() {
    this.chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
  }

  public ReduceFileMeta(List<Long> chunkOffsets) {
    this.chunkOffsets = chunkOffsets;
  }

  @Override
  public synchronized List<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  @Override
  public synchronized void addChunkOffset(long offset) {
    chunkOffsets.add(offset);
  }

  public synchronized long getLastChunkOffset() {
    return chunkOffsets.get(chunkOffsets.size() - 1);
  }

  @Override
  public synchronized int getNumChunks() {
    if (chunkOffsets.isEmpty()) {
      return 0;
    } else {
      return chunkOffsets.size() - 1;
    }
  }

  @Override
  public synchronized void setSorted() {
    sorted.set(true);
  }

  @Override
  public boolean getSorted() {
    return sorted.get();
  }

  public synchronized int numChunks() {
    if (!chunkOffsets.isEmpty()) {
      return chunkOffsets.size() - 1;
    } else {
      return 0;
    }
  }
}
