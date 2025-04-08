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

package org.apache.spark.shuffle.celeborn;

import java.util.*;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class CelebornPartitionUtil {
  /**
   * The general idea is to divide each skew partition into smaller partitions:
   *
   * <p>- Spark driver will calculate the number of sub-partitions: {@code subPartitionSize =
   * skewPartitionTotalSize / subPartitionTargetSize}
   *
   * <p>- In Celeborn, we divide the skew partition into {@code subPartitionSize} small partitions
   * by PartitionLocation chunk offsets. This allows them to run in parallel Spark tasks.
   *
   * <p>For example, one skewed partition has 2 PartitionLocation:
   *
   * <ul>
   *   <li>PartitionLocation 0 with chunk offset [0L, 100L, 200L, 300L, 500L, 1000L]
   *   <li>PartitionLocation 1 with chunk offset [0L, 200L, 500L, 800L, 900L, 1000L]
   * </ul>
   *
   * If we want to divide it into 3 sub-partitions (each sub-partition target size is 2000/3), the
   * result will be:
   *
   * <ul>
   *   <li>sub-partition 0: uniqueId0 -> (0, 3)
   *   <li>sub-partition 1: uniqueId0 -> (4, 4), uniqueId1 -> (0, 0)
   *   <li>sub-partition 2: uniqueId1 -> (1, 4)
   * </ul>
   *
   * Note: (0, 3) means chunks with chunkIndex 0-1-2-3, four chunks.
   *
   * @param locations PartitionLocation information belonging to the reduce partition
   * @param subPartitionSize the number of sub-partitions separated from the reduce partition
   * @param subPartitionIndex current sub-partition index
   * @return a map of partitionUniqueId to chunkRange pairs for one subtask of skew partitions
   */
  public static Map<String, Pair<Integer, Integer>> splitSkewedPartitionLocations(
      ArrayList<PartitionLocation> locations, int subPartitionSize, int subPartitionIndex) {
    locations.sort(Comparator.comparing((PartitionLocation p) -> p.getUniqueId()));
    long totalPartitionSize =
        locations.stream().mapToLong((PartitionLocation p) -> p.getStorageInfo().fileSize).sum();
    long step = totalPartitionSize / subPartitionSize;
    long startOffset = step * subPartitionIndex;
    long endOffset =
        subPartitionIndex < subPartitionSize - 1
            ? step * (subPartitionIndex + 1)
            : totalPartitionSize + 1; // last subPartition should include all remaining data

    long partitionLocationOffset = 0;
    Map<String, Pair<Integer, Integer>> chunkRange = new HashMap<>();
    for (PartitionLocation p : locations) {
      int left = -1;
      int right = -1;
      Iterator<Long> chunkOffsets = p.getStorageInfo().getChunkOffsets().iterator();
      // Start from index 1 since the first chunk offset is always 0.
      chunkOffsets.next();
      int j = 1;
      while (chunkOffsets.hasNext()) {
        long currentOffset = partitionLocationOffset + chunkOffsets.next();
        if (currentOffset > startOffset && left < 0) {
          left = j - 1;
        }
        if (currentOffset <= endOffset) {
          right = j - 1;
        }
        if (left >= 0 && right >= 0 && right >= left) {
          chunkRange.put(p.getUniqueId(), Pair.of(left, right));
        }
        j++;
      }
      partitionLocationOffset += p.getStorageInfo().getFileSize();
      if (partitionLocationOffset > endOffset) {
        break;
      }
    }
    return chunkRange;
  }
}
