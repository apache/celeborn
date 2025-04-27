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

package org.apache.celeborn.client.commit

import java.util
import java.util.Comparator
import com.google.common.base.Preconditions.{checkArgument, checkState}
import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.JavaUtils

class PartitionCompletenessValidator extends Logging {

  private val actualCommitMetadataForReducer = {
    JavaUtils.newConcurrentHashMap[Int, java.util.TreeMap[(Int, Int), CommitMetadata]]()
  }
  private val currentCommitMetadataForReducer = JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()
  private val currentTotalMapRangeSumForReducer = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val comparator: Comparator[(Int, Int)] = (o1: (Int, Int), o2: (Int, Int)) => {
    val comparator = Integer.compare(o1._1, o2._1)
    if (comparator != 0)
      comparator
    else
      Integer.compare(o1._2, o2._2)
  }

  def validateSubPartition(
                            partitionId: Int,
                            startMapIndex: Int,
                            endMapIndex: Int,
                            actualCommitMetadata: CommitMetadata,
                            expectedCommitMetadata: CommitMetadata,
                            expectedTotalMapperCountForParent: Int): (Boolean, String) = {
    checkArgument(
      startMapIndex < endMapIndex,
      "startMapIndex %s must be less than endMapIndex %s",
      startMapIndex,
      endMapIndex)
    logError(
      s"Validate partition invoked for partitionId $partitionId startMapIndex $startMapIndex endMapIndex $endMapIndex")
    val subRangeToCommitMetadataMap = actualCommitMetadataForReducer.computeIfAbsent(
      partitionId,
      _ => new util.TreeMap[(Int, Int), CommitMetadata](comparator))
    subRangeToCommitMetadataMap.synchronized {
      val count = subRangeToCommitMetadataMap.get((startMapIndex, endMapIndex))
      if (count == null) {
        val (isOverlapping, overlappingEntry) =
          checkOverlappingRange(subRangeToCommitMetadataMap, startMapIndex, endMapIndex)
        if (isOverlapping) {
          val errorMessage = s"Encountered overlapping map range for partitionId: $partitionId " +
            s" while processing range with startMapIndex: $startMapIndex and endMapIndex: $endMapIndex " +
            s"existing range map: $subRangeToCommitMetadataMap " +
            s"overlapped with Entry((startMapIndex, endMapIndex), count): $overlappingEntry"
          logError(errorMessage)
          return (false, errorMessage)
        } else {
          subRangeToCommitMetadataMap.put((startMapIndex, endMapIndex), actualCommitMetadata)
          currentCommitMetadataForReducer.merge(partitionId, actualCommitMetadata, CommitMetadata.combineMetadata)
          currentTotalMapRangeSumForReducer.merge(partitionId, endMapIndex - startMapIndex, Integer.sum)
        }
      } else if (count != actualCommitMetadata) {
        val errorMessage = s"Commit Metadata for partition: $partitionId " +
          s"not matching for sub-partition with startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
          s"previous count: $count new count: $actualCommitMetadata"
        logError(errorMessage)
        return (false, errorMessage)
      }
      return isPartitionValid(
        partitionId,
        expectedCommitMetadata,
        expectedTotalMapperCountForParent,
        startMapIndex,
        endMapIndex)
    }
  }

  private def checkOverlappingRange(
      treeMap: java.util.TreeMap[(Int, Int), CommitMetadata],
      startMapIndex: Int,
      endMapIndex: Int): (Boolean, ((Int, Int), CommitMetadata)) = {
    val floorEntry: util.Map.Entry[(Int, Int), CommitMetadata] =
      treeMap.floorEntry((startMapIndex, endMapIndex))
    val ceilingEntry: util.Map.Entry[(Int, Int), CommitMetadata] =
      treeMap.ceilingEntry((startMapIndex, endMapIndex))

    if (floorEntry != null) {
      if (startMapIndex < floorEntry.getKey._2) {
        return (true, ((floorEntry.getKey._1, floorEntry.getKey._2), floorEntry.getValue))
      }
    }
    if (ceilingEntry != null) {
      if (endMapIndex > ceilingEntry.getKey._1) {
        return (true, ((ceilingEntry.getKey._1, ceilingEntry.getKey._2), ceilingEntry.getValue))
      }
    }
    (false, ((0, 0), new CommitMetadata()))
  }

  private def isPartitionValid(
      partitionId: Int,
      expectedCommitMetadata: CommitMetadata,
      totalMapperCountForPartition: Int,
      startMapIndex: Int,
      endMapIndex: Int): (Boolean, String) = {
    if (!currentTotalMapRangeSumForReducer.containsKey(partitionId)) {
      checkState(!currentCommitMetadataForReducer.containsKey(
        partitionId,
        "mapper total count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
        partitionId,
        startMapIndex,
        endMapIndex))
      currentTotalMapRangeSumForReducer.put(partitionId, 0)
      currentCommitMetadataForReducer.put(partitionId, new CommitMetadata())
    }
    checkState(
      currentCommitMetadataForReducer.containsKey(partitionId),
      "mapper written count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
      partitionId,
      startMapIndex,
      endMapIndex)
    val sumOfMapRanges: Int = currentTotalMapRangeSumForReducer.get(partitionId)
    val currentCommitMetadata: CommitMetadata  =
      currentCommitMetadataForReducer.get(partitionId)

    if (sumOfMapRanges > totalMapperCountForPartition) {
      val errorMsg = s"AQE Partition $partitionId failed validation check " +
        s"while processing startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
        s"ActualCommitMatadata $currentCommitMetadata > ExpectedTotalMapperCount $totalMapperCountForPartition"
      logError(errorMsg)
      return (false, errorMsg)
    }
    if (sumOfMapRanges == totalMapperCountForPartition) {
      if (CommitMetadata.checkCommitMetadata(currentCommitMetadata, expectedCommitMetadata)) {
        logInfo(
          s"AQE Partition $partitionId completed validation check, " +
            s"expectedCommitMetadata $expectedCommitMetadata, " +
            s"totalMapperCount $totalMapperCountForPartition")
        return (true, "Partition is complete")
      } else {

        val entryList = new StringBuilder("Map of range -> actual count\n");
        val mapRangeToCountMap = actualCommitMetadataForReducer.get(partitionId)
        mapRangeToCountMap.entrySet().forEach(entry => {
          entryList.append(
            "Range " + entry.getKey + " commit metadata: " + entry.getValue + "\n")
        })
        val errorMsg = s"AQE Partition $partitionId failed validation check " +
          s"while processing range startMapIndex: $startMapIndex endMapIndex: $endMapIndex" +
          s"ExpectedCommitMetadata $expectedCommitMetadata, " +
          s"ActualCommitMetadata $currentCommitMetadata, " +
          s"totalMapperCount $totalMapperCountForPartition, " +
          s"debugInfo ${entryList.toString()}"
        logError(errorMsg)
        return (false, errorMsg)
      }
    }
    (true, "Partition is valid but still waiting for more data")
  }
}
