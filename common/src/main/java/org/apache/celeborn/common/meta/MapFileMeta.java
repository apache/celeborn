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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapFileMeta implements FileMeta {
  private int bufferSize;
  private int numSubPartitions;
  private String mountPoint;
  private Map<Integer, Integer> subPartitionWritingSegmentId;
  private List<SegmentIndex> subPartitionSegmentIndexes;
  private volatile boolean hasWriteFinished;
  private boolean isSegmentGranularityVisible;

  public MapFileMeta() {}

  public MapFileMeta(int bufferSize, int numSubPartitions) {
    this(bufferSize, numSubPartitions, false, Collections.emptyMap(), Collections.emptyList());
  }

  public MapFileMeta(
      int bufferSize,
      int numSubPartitions,
      boolean isSegmentGranularityVisible,
      Map<Integer, Integer> subPartitionWritingSegmentId,
      List<SegmentIndex> subPartitionSegmentIndexes) {
    this.bufferSize = bufferSize;
    this.numSubPartitions = numSubPartitions;
    this.isSegmentGranularityVisible = isSegmentGranularityVisible;
    this.subPartitionWritingSegmentId = subPartitionWritingSegmentId;
    this.subPartitionSegmentIndexes = subPartitionSegmentIndexes;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getNumSubPartitions() {
    return numSubPartitions;
  }

  public void setNumSubPartitions(int numSubPartitions) {
    this.numSubPartitions = numSubPartitions;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public void setHasWriteFinished(boolean hasWriteFinished) {
    this.hasWriteFinished = hasWriteFinished;
  }

  public void setSubPartitionWritingSegmentId(Map<Integer, Integer> subPartitionWritingSegmentId) {
    this.subPartitionWritingSegmentId = subPartitionWritingSegmentId;
  }

  public void setSubPartitionSegmentIndexes(List<SegmentIndex> subPartitionSegmentIndexes) {
    this.subPartitionSegmentIndexes = subPartitionSegmentIndexes;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public int getNumSubpartitions() {
    return numSubPartitions;
  }

  public Map<Integer, Integer> getSubPartitionWritingSegmentId() {
    return subPartitionWritingSegmentId;
  }

  public List<SegmentIndex> getSubPartitionSegmentIndexes() {
    return subPartitionSegmentIndexes;
  }

  public boolean hasWriteFinished() {
    return hasWriteFinished;
  }

  public boolean isSegmentGranularityVisible() {
    return isSegmentGranularityVisible;
  }

  public void setSegmentGranularityVisible(boolean segmentGranularityVisible) {
    isSegmentGranularityVisible = segmentGranularityVisible;
  }

  public synchronized void addPartitionSegmentId(int supPartitionId, int segmentId) {
    if (subPartitionWritingSegmentId == null) {
      subPartitionWritingSegmentId = new HashMap<>();
    }
    subPartitionWritingSegmentId.put(supPartitionId, segmentId);
  }

  public synchronized boolean hasPartitionSegmentIds() {
    return subPartitionWritingSegmentId != null;
  }

  public synchronized int getPartitionWritingSegmentId(int supPartitionId) {
    return subPartitionWritingSegmentId.get(supPartitionId);
  }

  public synchronized void addSegmentIdAndFirstBufferIndex(
      int subPartitionId, int firstBufferIndex, int segmentId) {
    if (subPartitionSegmentIndexes == null) {
      subPartitionSegmentIndexes = new ArrayList<>();
      for (int i = 0; i < numSubPartitions; ++i) {
        subPartitionSegmentIndexes.add(new SegmentIndex(new HashMap<>()));
      }
    }
    subPartitionSegmentIndexes
        .get(subPartitionId)
        .addFirstBufferIndexAndSegmentId(firstBufferIndex, segmentId);
  }

  public synchronized Integer getSegmentIdByFirstBufferIndex(int subPartitionId, int bufferIndex) {
    if (subPartitionSegmentIndexes == null) {
      return null;
    }
    return subPartitionSegmentIndexes.size() > subPartitionId
        ? subPartitionSegmentIndexes
            .get(subPartitionId)
            .getFirstBufferIndexToSegment()
            .get(bufferIndex)
        : null;
  }

  /**
   * The segment indexes of a sub partition. The segment index may contain multiple segments. Each
   * segment index is an entry (<firstBufferIndex, segmentId>) in the map.
   */
  public static class SegmentIndex {

    // This is the map to store the segment indexes of a sub partition.
    // <firstBufferIndex, segmentId>
    // where firstBufferIndex is the first buffer index of the segment.
    // segmentId is the segment id of the segment.
    private final Map<Integer, Integer> firstBufferIndexToSegment;

    public SegmentIndex(Map<Integer, Integer> firstBufferIndexToSegment) {
      this.firstBufferIndexToSegment = firstBufferIndexToSegment;
    }

    public Map<Integer, Integer> getFirstBufferIndexToSegment() {
      return firstBufferIndexToSegment;
    }

    void addFirstBufferIndexAndSegmentId(int firstBufferIndex, int segmentId) {
      firstBufferIndexToSegment.put(firstBufferIndex, segmentId);
    }
  }
}
