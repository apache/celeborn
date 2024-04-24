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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class CelebornPartitionUtil {
  public static Map<String, Pair<Integer, Integer>> splitSkewedPartitionLocations(
      ArrayList<PartitionLocation> locations, int subPartitionSize, int subPartitionIndex) {
    locations.sort(Comparator.comparing((PartitionLocation p) -> p.getUniqueId()));
    long totalPartitionSize =
        locations.stream().mapToLong((PartitionLocation p) -> p.getStorageInfo().fileSize).sum();
    long step = totalPartitionSize / subPartitionSize;
    long startOffset = step * subPartitionIndex;
    long endOffset = 0;
    if (subPartitionIndex == subPartitionSize - 1) {
      // last subPartition should include all remaining data
      endOffset = totalPartitionSize + 1;
    } else {
      endOffset = step * (subPartitionIndex + 1);
    }

    long partitionLocationOffset = 0;
    Map<String, Pair<Integer, Integer>> chunkRange = new HashMap<>();
    for (int i = 0; i < locations.size(); i++) {
      PartitionLocation p = locations.get(i);
      int left = -1;
      int right = -1;
      // Start from index 1 since the first chunk offset is always 0.
      for (int j = 1; j < p.getStorageInfo().getChunkOffsets().size(); j++) {
        long currentOffset = partitionLocationOffset + p.getStorageInfo().getChunkOffsets().get(j);
        if (currentOffset > startOffset && left < 0) {
          left = j - 1;
        }
        if (currentOffset <= endOffset) {
          right = j - 1;
        }
        if (left >= 0 && right >= 0) {
          chunkRange.put(p.getUniqueId(), Pair.of(left, right));
        }
      }
      partitionLocationOffset += p.getStorageInfo().getFileSize();
    }
    return chunkRange;
  }
}
