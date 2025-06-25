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

import org.scalatest.matchers.must.Matchers.{be, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CommitMetadata

class SkewHandlingWithoutMapRangeValidatorTest extends CelebornFunSuite {

  private var validator: SkewHandlingWithoutMapRangeValidator = _

  test("testProcessSubPartitionFirstTime") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10
    val endMapIndex = 0
    val metadata = new CommitMetadata
    val expectedMapperCount = 20

    // Execute
    val (success, message) = validator.processSubPartition(
      partitionId,
      startMapIndex,
      endMapIndex,
      metadata,
      expectedMapperCount)

    success shouldBe true
    message shouldBe ""
    validator.isPartitionComplete(partitionId) shouldBe false

    // Verify current metadata
    val currentMetadata = validator.currentCommitMetadata(partitionId)
    currentMetadata shouldNot be(null)
    currentMetadata shouldBe metadata
  }

  test("testProcessMultipleSubPartitions") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10
    val metadata1 = new CommitMetadata(5, 1000)
    val metadata2 = new CommitMetadata(3, 500)

    // Process first sub-partition
    validator.processSubPartition(partitionId, startMapIndex, 0, metadata1, startMapIndex)

    // Process second sub-partition
    val (success, message) = validator.processSubPartition(
      partitionId,
      startMapIndex,
      1,
      metadata2,
      startMapIndex)

    // Verify
    success shouldBe true
    message shouldBe ""
    validator.isPartitionComplete(partitionId) shouldBe false

    // Verify current metadata
    val currentMetadata = validator.currentCommitMetadata(partitionId)
    currentMetadata shouldNot be(null)
    metadata1.addCommitData(metadata2)
    currentMetadata shouldBe metadata1

  }

  test("testPartitionComplete") {
    // Setup - processing all sub-partitions from 0 to 9 for a total of 10
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10

    // Process all sub-partitions
    for (i <- 0 until startMapIndex) {
      val metadata = new CommitMetadata()
      validator.processSubPartition(partitionId, startMapIndex, i, metadata, startMapIndex)
    }

    validator.isPartitionComplete(partitionId) shouldBe true
  }

  test("testInvalidStartEndMapIndex") {
    // Setup - invalid case where startMapIndex <= endMapIndex
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 5
    val endMapIndex = 5 // Equal, which should fail
    val metadata = new CommitMetadata()

    // This should throw IllegalArgumentException
    intercept[IllegalArgumentException] {
      validator.processSubPartition(partitionId, startMapIndex, endMapIndex, metadata, 20)
    }
  }

  test("testMismatchSubPartitionCount") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val metadata = new CommitMetadata()

    // First call with startMapIndex = 10
    validator.processSubPartition(partitionId, 10, 0, metadata, 10)

    // Second call with different startMapIndex, which should fail
    intercept[IllegalStateException] {
      validator.processSubPartition(partitionId, 12, 1, metadata, 12)
    }
  }

  test("testDuplicateSubPartitionWithSameMetadata") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10
    val endMapIndex = 5
    val metadata = new CommitMetadata(3, 100)

    // Process sub-partition first time
    validator.processSubPartition(partitionId, startMapIndex, endMapIndex, metadata, startMapIndex)

    // Process same sub-partition again with identical metadata
    val (success, message) = validator.processSubPartition(
      partitionId,
      startMapIndex,
      endMapIndex,
      metadata,
      startMapIndex)
    success shouldBe true
    message shouldBe ""
    validator.isPartitionComplete(partitionId) shouldBe false
  }

  test("testDuplicateSubPartitionWithDifferentMetadata") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10
    val endMapIndex = 5
    val metadata1 = new CommitMetadata(3, 100)

    val metadata2 = new CommitMetadata(4, 200)

    // Process sub-partition first time
    validator.processSubPartition(partitionId, startMapIndex, endMapIndex, metadata1, startMapIndex)

    // Process same sub-partition again with different metadata
    val (success, message) = validator.processSubPartition(
      partitionId,
      startMapIndex,
      endMapIndex,
      metadata2,
      startMapIndex)
    success shouldBe false
    message should include("Mismatch in metadata")
    validator.isPartitionComplete(partitionId) shouldBe false
  }

  test("testProcessTooManySubPartitions") {
    // Setup
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 10

    // Process all sub-partitions
    for (i <- 0 until startMapIndex) {
      val metadata = new CommitMetadata()
      validator.processSubPartition(partitionId, startMapIndex, i, metadata, startMapIndex)
    }

    // Try to process one more sub-partition, which should exceed the total count
    val extraMetadata = new CommitMetadata()
    // This should throw IllegalStateException
    intercept[IllegalArgumentException] {
      validator.processSubPartition(
        partitionId,
        startMapIndex,
        startMapIndex,
        extraMetadata,
        startMapIndex)
    }
  }

  test("multiple complete partitions") {
    // Setup - we'll use 3 different partitions
    validator = new SkewHandlingWithoutMapRangeValidator
    val partition1 = 1
    val partition2 = 2
    val partition3 = 3

    // Different sizes for each partition
    val subPartitions1 = 5
    val subPartitions2 = 8
    val subPartitions3 = 3

    // Process all sub-partitions for partition1
    val expectedMetadata1 = new CommitMetadata()
    for (i <- 0 until subPartitions1) {
      val metadata = new CommitMetadata(i, 100 + i)
      validator.processSubPartition(partition1, subPartitions1, i, metadata, subPartitions1)
      expectedMetadata1.addCommitData(metadata)
    }

    // Process all sub-partitions for partition2
    val expectedMetadata2 = new CommitMetadata()
    for (i <- 0 until subPartitions2) {
      val metadata = new CommitMetadata(i + 1, 200 + i)
      validator.processSubPartition(partition2, subPartitions2, i, metadata, subPartitions2)
      expectedMetadata2.addCommitData(metadata)
    }

    // Process only some sub-partitions for partition3
    val expectedMetadata3 = new CommitMetadata()
    for (i <- 0 until subPartitions3 - 1) { // Deliberately leave one out
      val metadata = new CommitMetadata(i + 2, 300 + i)
      validator.processSubPartition(partition3, subPartitions3, i, metadata, subPartitions3)
      expectedMetadata3.addCommitData(metadata)
    }

    // Verify completion status for each partition
    validator.isPartitionComplete(partition1) shouldBe true
    validator.isPartitionComplete(partition2) shouldBe true
    validator.isPartitionComplete(partition3) shouldBe false

    validator.currentCommitMetadata(partition1) shouldBe expectedMetadata1
    validator.currentCommitMetadata(partition2) shouldBe expectedMetadata2
    validator.currentCommitMetadata(partition3) shouldBe expectedMetadata3
  }
}
