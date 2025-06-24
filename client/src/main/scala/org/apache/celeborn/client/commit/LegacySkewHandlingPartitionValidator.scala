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
import java.util.function.BiFunction

import com.google.common.base.Preconditions.{checkArgument, checkState}

import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.JavaUtils

class LegacySkewHandlingPartitionValidator extends AbstractPartitionCompletenessValidator
  with Logging {
  private val subRangeToCommitMetadataPerReducer = {
    JavaUtils.newConcurrentHashMap[Int, java.util.TreeMap[(Int, Int), CommitMetadata]]()
  }
  private val partitionToSubPartitionCount = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val currentCommitMetadataForReducer =
    JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()
  private val currentTotalMapIdCountForReducer = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val comparator: java.util.Comparator[(Int, Int)] = new Comparator[(Int, Int)] {
    override def compare(o1: (Int, Int), o2: (Int, Int)): Int = {
      val comparator = Integer.compare(o1._1, o2._1)
      if (comparator != 0)
        comparator
      else
        Integer.compare(o1._2, o2._2)
    }
  }
  private val mapCountMergeBiFunction = new BiFunction[Int, Int, Int] {
    override def apply(t: Int, u: Int): Int =
      Integer.sum(t, u)
  }
  private val metadataMergeBiFunction: BiFunction[CommitMetadata, CommitMetadata, CommitMetadata] =
    new BiFunction[CommitMetadata, CommitMetadata, CommitMetadata] {
      override def apply(
          existing: CommitMetadata,
          incoming: CommitMetadata): CommitMetadata = {
        if (existing == null) {
          if (incoming != null) {
            return new CommitMetadata(incoming.getChecksum, incoming.getBytes)
          } else {
            return incoming
          }
        }
        if (incoming == null) {
          return existing
        }
        existing.addCommitData(incoming)
        existing
      }
    }

  private def checkOverlappingRange(
      treeMap: java.util.TreeMap[(Int, Int), CommitMetadata],
      rangeKey: (Int, Int)): (Boolean, ((Int, Int), CommitMetadata)) = {
    val floorEntry: util.Map.Entry[(Int, Int), CommitMetadata] =
      treeMap.floorEntry(rangeKey)
    val ceilingEntry: util.Map.Entry[(Int, Int), CommitMetadata] =
      treeMap.ceilingEntry(rangeKey)

    if (floorEntry != null) {
      if (rangeKey._1 < floorEntry.getKey._2) {
        return (true, ((floorEntry.getKey._1, floorEntry.getKey._2), floorEntry.getValue))
      }
    }
    if (ceilingEntry != null) {
      if (rangeKey._2 > ceilingEntry.getKey._1) {
        return (true, ((ceilingEntry.getKey._1, ceilingEntry.getKey._2), ceilingEntry.getValue))
      }
    }
    (false, ((0, 0), new CommitMetadata()))
  }

  override def processSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedTotalMapperCount: Int): (Boolean, String) = {
    checkArgument(
      startMapIndex < endMapIndex,
      "startMapIndex %s must be less than endMapIndex %s",
      startMapIndex,
      endMapIndex)
    logDebug(
      s"Validate partition invoked for partitionId $partitionId startMapIndex $startMapIndex endMapIndex $endMapIndex")
    partitionToSubPartitionCount.put(partitionId, expectedTotalMapperCount)
    val subRangeToCommitMetadataMap = subRangeToCommitMetadataPerReducer.computeIfAbsent(
      partitionId,
      new java.util.function.Function[Int, util.TreeMap[(Int, Int), CommitMetadata]] {
        override def apply(key: Int): util.TreeMap[(Int, Int), CommitMetadata] =
          new util.TreeMap[(Int, Int), CommitMetadata](comparator)
      })
    val rangeKey = (startMapIndex, endMapIndex)
    subRangeToCommitMetadataMap.synchronized {
      val existingMetadata = subRangeToCommitMetadataMap.get(rangeKey)
      if (existingMetadata == null) {
        val (isOverlapping, overlappingEntry) =
          checkOverlappingRange(subRangeToCommitMetadataMap, rangeKey)
        if (isOverlapping) {
          val errorMessage = s"Encountered overlapping map range for partitionId: $partitionId " +
            s" while processing range with startMapIndex: $startMapIndex and endMapIndex: $endMapIndex " +
            s"existing range map: $subRangeToCommitMetadataMap " +
            s"overlapped with Entry((startMapIndex, endMapIndex), count): $overlappingEntry"
          logError(errorMessage)
          return (false, errorMessage)
        }

        // Process new range
        subRangeToCommitMetadataMap.put(rangeKey, actualCommitMetadata)
        currentCommitMetadataForReducer.merge(
          partitionId,
          actualCommitMetadata,
          metadataMergeBiFunction)
        currentTotalMapIdCountForReducer.merge(
          partitionId,
          endMapIndex - startMapIndex,
          mapCountMergeBiFunction)
      } else if (existingMetadata != actualCommitMetadata) {
        val errorMessage = s"Commit Metadata for partition: $partitionId " +
          s"not matching for sub-partition with startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
          s"previous count: $existingMetadata new count: $actualCommitMetadata"
        logError(errorMessage)
        return (false, errorMessage)
      }
      if (!currentTotalMapIdCountForReducer.containsKey(partitionId)) {
        checkState(!currentCommitMetadataForReducer.containsKey(
          partitionId,
          "mapper total count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
          partitionId,
          startMapIndex,
          endMapIndex))
        currentTotalMapIdCountForReducer.put(partitionId, 0)
        currentCommitMetadataForReducer.put(partitionId, new CommitMetadata())
      }
      checkState(
        currentCommitMetadataForReducer.containsKey(partitionId),
        "mapper written count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
        partitionId,
        startMapIndex,
        endMapIndex)
      val sumOfMapRanges: Int = currentTotalMapIdCountForReducer.get(partitionId)
      val currentCommitMetadata: CommitMetadata =
        currentCommitMetadataForReducer.get(partitionId)

      if (sumOfMapRanges > expectedTotalMapperCount) {
        val errorMsg = s"AQE Partition $partitionId failed validation check " +
          s"while processing startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
          s"ActualCommitMetadata $currentCommitMetadata > ExpectedTotalMapperCount $expectedTotalMapperCount"
        logError(errorMsg)
        return (false, errorMsg)
      }
    }
    (true, "")
  }

  override def currentCommitMetadata(partitionId: Int): CommitMetadata = {
    currentCommitMetadataForReducer.get(partitionId)
  }

  override def isPartitionComplete(partitionId: Int): Boolean = {
    val sumOfMapRanges: Int = currentTotalMapIdCountForReducer.get(partitionId)
    sumOfMapRanges == partitionToSubPartitionCount.get(partitionId)
  }
}
