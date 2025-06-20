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

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.util.JavaUtils

class SkewHandlingWithoutMapRangeValidator extends AbstractPartitionCompletenessValidator {

  private val totalSubPartitionsProcessed = JavaUtils.newConcurrentHashMap[Int, RoaringBitmap]()
  private val partitionToSubPartitionCount = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val subPartitionToCommitMetadata = JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()
  private val currentCommitMetadataForReducer =
    JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()

  override def processSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedTotalMapperCount: Int): (Boolean, String) = {
    totalSubPartitionsProcessed.synchronized {
      var bitmap: RoaringBitmap = null
      if (totalSubPartitionsProcessed.containsKey(partitionId)) {
        bitmap = totalSubPartitionsProcessed.get(partitionId)
      } else {
        bitmap = new RoaringBitmap()
        totalSubPartitionsProcessed.put(partitionId, bitmap)
        partitionToSubPartitionCount.put(partitionId, startMapIndex)
      }
      if (bitmap.contains(endMapIndex)) {
        // check if previous entry matches
        val existingCommitMetadata = subPartitionToCommitMetadata.get(endMapIndex)
        if (existingCommitMetadata != actualCommitMetadata) {
          return (
            false,
            s"Mismatch in metadata for the same chunk range on retry: $endMapIndex existing: $existingCommitMetadata new: $actualCommitMetadata")
        }
      } else {
        bitmap.add(endMapIndex)
        subPartitionToCommitMetadata.put(endMapIndex, actualCommitMetadata)
      }
    }
    if (!currentCommitMetadataForReducer.containsKey(partitionId)) {
      currentCommitMetadataForReducer.put(
        partitionId,
        new CommitMetadata(actualCommitMetadata.getChecksum, actualCommitMetadata.getBytes))
    } else {
      currentCommitMetadataForReducer.get(partitionId).addCommitData(actualCommitMetadata)
    }
    (true, "")
  }

  override def currentCommitMetadata(partitionId: Int): CommitMetadata = {
    currentCommitMetadataForReducer.get(partitionId)
  }

  override def isPartitionComplete(partitionId: Int): Boolean = {
    totalSubPartitionsProcessed.get(
      partitionId).getCardinality == partitionToSubPartitionCount.get(partitionId)
  }
}
