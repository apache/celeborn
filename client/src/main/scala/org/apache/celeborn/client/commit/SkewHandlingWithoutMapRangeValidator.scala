package org.apache.celeborn.client.commit

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.util.JavaUtils

class SkewHandlingWithoutMapRangeValidator extends AbstractPartitionCompletenessValidator {

  private val totalSubPartitionsProcessed = JavaUtils.newConcurrentHashMap[Int, RoaringBitmap]()
  private val partitionToSubPartitionCount = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val subPartitionToCommiMetadata = JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()
  private val currentCommitMetadataForReducer =
    JavaUtils.newConcurrentHashMap[Int, CommitMetadata]()

  override def processSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedTotalMapperCount: Int): (Boolean, String) = {
    totalSubPartitionsProcessed.synchronized {
      var bitmap: RoaringBitmap = new RoaringBitmap()
      if (totalSubPartitionsProcessed.containsKey(partitionId)) {
        bitmap = totalSubPartitionsProcessed.get(partitionId)
      } else {
        totalSubPartitionsProcessed.put(partitionId, bitmap)
        partitionToSubPartitionCount.put(partitionId, startMapIndex)
      }
      if (bitmap.contains(endMapIndex)) {
        // check if previous entry matches
        if (subPartitionToCommiMetadata.get(endMapIndex) != actualCommitMetadata) {
          (false, "")
        }
      } else {
        bitmap.add(endMapIndex)
        subPartitionToCommiMetadata.put(endMapIndex, actualCommitMetadata)
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
