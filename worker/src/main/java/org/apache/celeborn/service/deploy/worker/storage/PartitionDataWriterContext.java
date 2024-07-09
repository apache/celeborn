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

package org.apache.celeborn.service.deploy.worker.storage;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.Utils;

public class PartitionDataWriterContext {
  private final long splitThreshold;
  private final PartitionSplitMode partitionSplitMode;
  private final boolean rangeReadFilter;
  private final PartitionLocation partitionLocation;
  private final String appId;
  private final int shuffleId;
  private final UserIdentifier userIdentifier;
  private final boolean partitionSplitEnabled;
  private final String shuffleKey;
  private final PartitionType partitionType;
  private StorageInfo.Type storageType = null;
  private PartitionDataWriter partitionDataWriter;
  private boolean isSegmentGranularityVisible;

  public PartitionDataWriterContext(
      long splitThreshold,
      PartitionSplitMode partitionSplitMode,
      boolean rangeReadFilter,
      PartitionLocation partitionLocation,
      String appId,
      int shuffleId,
      UserIdentifier userIdentifier,
      PartitionType partitionType,
      boolean partitionSplitEnabled) {
    this.splitThreshold = splitThreshold;
    this.partitionSplitMode = partitionSplitMode;
    this.rangeReadFilter = rangeReadFilter;
    this.partitionLocation = partitionLocation;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.userIdentifier = userIdentifier;
    this.partitionSplitEnabled = partitionSplitEnabled;
    this.partitionType = partitionType;
    this.shuffleKey = Utils.makeShuffleKey(appId, shuffleId);
  }

  public long getSplitThreshold() {
    return splitThreshold;
  }

  public PartitionSplitMode getPartitionSplitMode() {
    return partitionSplitMode;
  }

  public boolean isRangeReadFilter() {
    return rangeReadFilter;
  }

  public PartitionLocation getPartitionLocation() {
    return partitionLocation;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public UserIdentifier getUserIdentifier() {
    return userIdentifier;
  }

  public boolean isPartitionSplitEnabled() {
    return partitionSplitEnabled;
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public StorageInfo.Type getStorageType() {
    return storageType;
  }

  public void setStorageType(StorageInfo.Type storageType) {
    this.storageType = storageType;
  }

  public PartitionDataWriter getPartitionDataWriter() {
    return partitionDataWriter;
  }

  public void setPartitionDataWriter(PartitionDataWriter partitionDataWriter) {
    this.partitionDataWriter = partitionDataWriter;
  }

  public boolean isSegmentGranularityVisible() {
    return isSegmentGranularityVisible;
  }

  public void setSegmentGranularityVisible(boolean segmentGranularityVisible) {
    isSegmentGranularityVisible = segmentGranularityVisible;
  }
}
