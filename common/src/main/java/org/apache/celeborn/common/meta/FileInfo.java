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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.celeborn.common.identity.UserIdentifier;

public abstract class FileInfo {
  private final UserIdentifier userIdentifier;
  // whether to split is decided by client side.
  // now it's just used for mappartition to compatible with old client which can't support split
  private boolean partitionSplitEnabled;
  protected FileMeta fileMeta;
  protected final Set<Long> streams = ConcurrentHashMap.newKeySet();
  protected volatile long bytesFlushed;
  private boolean isReduceFileMeta;

  public FileInfo(UserIdentifier userIdentifier, boolean partitionSplitEnabled, FileMeta fileMeta) {
    this.userIdentifier = userIdentifier;
    this.partitionSplitEnabled = partitionSplitEnabled;
    this.fileMeta = fileMeta;
    this.isReduceFileMeta = fileMeta instanceof ReduceFileMeta;
  }

  public void replaceFileMeta(FileMeta meta) {
    this.fileMeta = meta;
    this.isReduceFileMeta = meta instanceof ReduceFileMeta;
  }

  public FileMeta getFileMeta() {
    return fileMeta;
  }

  public ReduceFileMeta getReduceFileMeta() {
    return (ReduceFileMeta) fileMeta;
  }

  public long getFileLength() {
    return bytesFlushed;
  }

  public synchronized void updateBytesFlushed(long bytes) {
    bytesFlushed += bytes;
    if (isReduceFileMeta) {
      getReduceFileMeta().updateChunkOffset(bytesFlushed, false);
    }
  }

  public UserIdentifier getUserIdentifier() {
    return userIdentifier;
  }

  public boolean isPartitionSplitEnabled() {
    return partitionSplitEnabled;
  }

  public void setPartitionSplitEnabled(boolean partitionSplitEnabled) {
    this.partitionSplitEnabled = partitionSplitEnabled;
  }

  public boolean addStream(long streamId) {
    if (!isReduceFileMeta) {
      throw new IllegalStateException("In addStream, filemeta cannot be MapFileMeta");
    }
    synchronized (getReduceFileMeta().getSorted()) {
      if (getReduceFileMeta().getSorted().get()) {
        return false;
      } else {
        streams.add(streamId);
        return true;
      }
    }
  }

  public void closeStream(long streamId) {
    if (!isReduceFileMeta) {
      throw new IllegalStateException("In closeStream, filemeta cannot be MapFileMeta");
    }
    synchronized (getReduceFileMeta().getSorted()) {
      streams.remove(streamId);
    }
  }

  public boolean isStreamsEmpty() {
    if (!isReduceFileMeta) {
      throw new IllegalStateException("In isStreamsEmpty, filemeta cannot be MapFileMeta");
    }
    synchronized (getReduceFileMeta().getSorted()) {
      return streams.isEmpty();
    }
  }
}
