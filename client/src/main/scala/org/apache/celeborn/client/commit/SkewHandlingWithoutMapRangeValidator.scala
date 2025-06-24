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

import com.google.common.base.Preconditions.{checkArgument, checkState}

import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.util.JavaUtils

class SkewHandlingWithoutMapRangeValidator extends AbstractPartitionCompletenessValidator {

  private val totalSubPartitionsProcessed =
    JavaUtils.newConcurrentHashMap[Int, util.HashMap[Int, CommitMetadata]]()
  private val partitionToSubPartitionCount = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val currentCommitMetadataForReducer =
    JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()

  override def processSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedTotalMapperCount: Int): (Boolean, String) = {
    checkArgument(
      endMapIndex >= 0,
      "index of sub-partition %s must be greater than or equal to 0",
      endMapIndex)
    checkArgument(
      startMapIndex > endMapIndex,
      "startMapIndex %s must be greater than endMapIndex %s",
      startMapIndex,
      endMapIndex)
    totalSubPartitionsProcessed.synchronized {
      if (totalSubPartitionsProcessed.containsKey(partitionId)) {
        val currentSubPartitionCount = partitionToSubPartitionCount.getOrDefault(partitionId, -1)
        checkState(
          currentSubPartitionCount == startMapIndex,
          "total subpartition count mismatch for partitionId %s existing %s new %s",
          partitionId,
          currentSubPartitionCount,
          startMapIndex)
      } else {
        totalSubPartitionsProcessed.put(partitionId, new util.HashMap[Int, CommitMetadata]())
        partitionToSubPartitionCount.put(partitionId, startMapIndex)
      }
      val subPartitionsProcessed = totalSubPartitionsProcessed.get(partitionId)
      if (subPartitionsProcessed.containsKey(endMapIndex)) {
        // check if previous entry matches
        val existingCommitMetadata = subPartitionsProcessed.get(endMapIndex)
        if (existingCommitMetadata != actualCommitMetadata) {
          return (
            false,
            s"Mismatch in metadata for the same chunk range on retry: $endMapIndex existing: $existingCommitMetadata new: $actualCommitMetadata")
        }
      }
      subPartitionsProcessed.put(endMapIndex, actualCommitMetadata)
      val partitionProcessed = getTotalNumberOfSubPartitionsProcessed(partitionId)
      checkState(
        partitionProcessed <= startMapIndex,
        "Number of sub-partitions processed %s should less than total number of sub-partitions %s",
        partitionProcessed,
        startMapIndex)
    }
    updateCommitMetadata(partitionId, actualCommitMetadata)
    (true, "")
  }

  private def updateCommitMetadata(partitionId: Int, actualCommitMetadata: CommitMetadata): Unit = {
    val currentCommitMetadata =
      currentCommitMetadataForReducer.getOrDefault(partitionId, new CommitMetadata())
    currentCommitMetadata.addCommitData(actualCommitMetadata)
  }

  private def getTotalNumberOfSubPartitionsProcessed(partitionId: Int) = {
    totalSubPartitionsProcessed.get(partitionId).size()
  }

  override def currentCommitMetadata(partitionId: Int): CommitMetadata = {
    currentCommitMetadataForReducer.get(partitionId)
  }

  override def isPartitionComplete(partitionId: Int): Boolean = {
    getTotalNumberOfSubPartitionsProcessed(partitionId) == partitionToSubPartitionCount.get(
      partitionId)
  }
}
