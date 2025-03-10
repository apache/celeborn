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

import java.io.File;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
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
  private final boolean isSegmentGranularityVisible;

  private File workingDir;
  private PartitionDataWriter partitionDataWriter;
  private DeviceMonitor deviceMonitor;

  public PartitionDataWriterContext(
      long splitThreshold,
      PartitionSplitMode partitionSplitMode,
      boolean rangeReadFilter,
      PartitionLocation partitionLocation,
      String appId,
      int shuffleId,
      UserIdentifier userIdentifier,
      PartitionType partitionType,
      boolean partitionSplitEnabled,
      boolean isSegmentGranularityVisible) {
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
    this.isSegmentGranularityVisible = isSegmentGranularityVisible;
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

  public boolean isSegmentGranularityVisible() {
    return isSegmentGranularityVisible;
  }

  public File getWorkingDir() {
    return workingDir;
  }

  public void setWorkingDir(File workingDir) {
    this.workingDir = workingDir;
  }

  public PartitionDataWriter getPartitionDataWriter() {
    return partitionDataWriter;
  }

  public void setPartitionDataWriter(PartitionDataWriter partitionDataWriter) {
    this.partitionDataWriter = partitionDataWriter;
  }

  public DeviceMonitor getDeviceMonitor() {
    return deviceMonitor;
  }

  public void setDeviceMonitor(DeviceMonitor deviceMonitor) {
    this.deviceMonitor = deviceMonitor;
  }

  @Override
  public String toString() {
    return "PartitionDataWriterContext{"
        + "splitThreshold="
        + splitThreshold
        + ", partitionSplitMode="
        + partitionSplitMode
        + ", rangeReadFilter="
        + rangeReadFilter
        + ", partitionLocation="
        + partitionLocation
        + ", appId='"
        + appId
        + '\''
        + ", shuffleId="
        + shuffleId
        + ", userIdentifier="
        + userIdentifier
        + ", partitionSplitEnabled="
        + partitionSplitEnabled
        + ", shuffleKey='"
        + shuffleKey
        + '\''
        + ", partitionType="
        + partitionType
        + ", isSegmentGranularityVisible="
        + isSegmentGranularityVisible
        + '}';
  }
}
