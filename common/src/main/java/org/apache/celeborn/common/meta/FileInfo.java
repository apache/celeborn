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

import org.roaringbitmap.RoaringBitmap;

import org.apache.celeborn.common.identity.UserIdentifier;

public abstract class FileInfo {
  private final UserIdentifier userIdentifier;
  // whether to split is decided by client side.
  // now it's just used for mappartition to compatible with old client which can't support split
  private boolean partitionSplitEnabled;
  protected FileMeta fileMeta;
  protected final Set<Long> streams = ConcurrentHashMap.newKeySet();

  public FileInfo(UserIdentifier userIdentifier, boolean partitionSplitEnabled, FileMeta fileMeta) {
    this.userIdentifier = userIdentifier;
    this.partitionSplitEnabled = partitionSplitEnabled;
    this.fileMeta = fileMeta;
  }

  public void replaceFileMeta(FileMeta meta) {
    this.fileMeta = meta;
  }

  public FileMeta getFileMeta() {
    return fileMeta;
  }

  public ReduceFileMeta getReduceFileMeta() {
    return (ReduceFileMeta) fileMeta;
  }

  public abstract long getFileLength();

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
    ReduceFileMeta reduceFileMeta = (ReduceFileMeta) fileMeta;
    synchronized (reduceFileMeta.getSorted()) {
      if (reduceFileMeta.getSorted().get()) {
        return false;
      } else {
        streams.add(streamId);
        return true;
      }
    }
  }

  public void closeStream(long streamId) {
    ReduceFileMeta reduceFileMeta = (ReduceFileMeta) fileMeta;
    synchronized (reduceFileMeta.getSorted()) {
      streams.remove(streamId);
    }
  }

  public boolean isStreamsEmpty() {
    ReduceFileMeta reduceFileMeta = (ReduceFileMeta) fileMeta;
    synchronized (reduceFileMeta.getSorted()) {
      return streams.isEmpty();
    }
  }

  public boolean isFullyRead() {
    ReduceFileMeta reduceFileMeta = ((ReduceFileMeta) fileMeta);
    RoaringBitmap mapIds = reduceFileMeta.getMapIds();
    if (mapIds == null) {
      return isStreamsEmpty();
    } else {
      return mapIds.isEmpty();
    }
  }
}
