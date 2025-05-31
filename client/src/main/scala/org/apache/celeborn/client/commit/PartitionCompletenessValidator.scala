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

import org.apache.celeborn.common.CommitMetadata
import org.apache.celeborn.common.internal.Logging

abstract class AbstractPartitionCompletenessValidator {
  def processSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedTotalMapperCount: Int): (Boolean, String)

  def currentCommitMetadata(partitionId: Int): CommitMetadata

  def isPartitionComplete(partitionId: Int): Boolean
}

class PartitionCompletenessValidator extends Logging {

  private val skewHandlingValidator: AbstractPartitionCompletenessValidator =
    new SkewHandlingWithoutMapRangeValidator
  private val legacySkewHandlingValidator: AbstractPartitionCompletenessValidator =
    new LegacySkewHandlingPartitionValidator

  def validateSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      actualCommitMetadata: CommitMetadata,
      expectedCommitMetadata: CommitMetadata,
      expectedTotalMapperCountForParent: Int,
      skewPartitionHandlingWithoutMapRange: Boolean): (Boolean, String) = {
    var validator = legacySkewHandlingValidator
    if (skewPartitionHandlingWithoutMapRange) {
      validator = skewHandlingValidator
    }
    val (successfullyProcessed, error) = validator.processSubPartition(
      partitionId,
      startMapIndex,
      endMapIndex,
      actualCommitMetadata,
      expectedTotalMapperCountForParent)
    if (!successfullyProcessed) {
      return (false, error)
    }
    if (validator.isPartitionComplete(partitionId)) {
      val currentCommitMetadata = validator.currentCommitMetadata(partitionId)
      val matchesMetadata =
        CommitMetadata.checkCommitMetadata(expectedCommitMetadata, currentCommitMetadata)

      if (matchesMetadata) {
        logInfo(
          s"AQE Partition $partitionId completed validation check , " +
            s"expectedCommitMetadata $expectedCommitMetadata, " +
            s"actualCommitMetadata $currentCommitMetadata")
        return (true, "Partition is complete")
      } else {
        val errorMsg =
          s"AQE Partition $partitionId failed validation check" +
            s"while processing range startMapIndex: $startMapIndex endMapIndex: $endMapIndex" +
            s"ExpectedCommitMetadata $expectedCommitMetadata, " +
            s"ActualCommitMetadata $currentCommitMetadata, "
        logError(errorMsg)
        return (false, errorMsg)
      }
    }
    (true, "Partition is valid but still waiting for more data")
  }
}
