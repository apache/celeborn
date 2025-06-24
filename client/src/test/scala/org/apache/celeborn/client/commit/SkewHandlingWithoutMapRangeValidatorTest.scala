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
    currentMetadata shouldBe metadata1.addCommitData(metadata2)

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

    validator.isPartitionComplete(partitionId) shouldBe false
  }

  test("testInvalidStartEndMapIndex") {
    // Setup - invalid case where startMapIndex <= endMapIndex
    validator = new SkewHandlingWithoutMapRangeValidator
    val partitionId = 1
    val startMapIndex = 5
    val endMapIndex = 5 // Equal, which should fail
    val metadata = new CommitMetadata()

    // This should throw IllegalArgumentException
    try {
      validator.processSubPartition(partitionId, startMapIndex, endMapIndex, metadata, 20)
    } catch {
      case e: IllegalArgumentException => e.isInstanceOf[IllegalArgumentException] shouldBe true
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
    try {
      validator.processSubPartition(partitionId, 12, 1, metadata, 12)
    } catch {
      case e: IllegalStateException => e.isInstanceOf[IllegalStateException] shouldBe true
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
    try {
      validator.processSubPartition(
        partitionId,
        startMapIndex,
        startMapIndex,
        extraMetadata,
        startMapIndex)
    } catch {
      case e: IllegalArgumentException => e.isInstanceOf[IllegalArgumentException] shouldBe true
    }
  }
}
