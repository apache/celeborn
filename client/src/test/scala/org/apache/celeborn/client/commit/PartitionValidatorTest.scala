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

import org.mockito.ArgumentMatchers.any
import org.scalatest.matchers.must.Matchers.{be, include}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CommitMetadata

class PartitionValidatorTest extends CelebornFunSuite {

  var validator: PartitionCompletenessValidator = _

  var mockCommitMetadata: CommitMetadata = new CommitMetadata()
  test("AQEPartitionCompletenessValidator should validate a new sub-partition correctly when there are no overlapping ranges") {
    validator = new PartitionCompletenessValidator
    val (isValid, message) =
      validator.validateSubPartition(1, 0, 10, mockCommitMetadata, mockCommitMetadata, 20, false)

    isValid shouldBe (true)
    message shouldBe ("Partition is valid but still waiting for more data")
  }

  test("AQEPartitionCompletenessValidator should fail validation for overlapping map ranges") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      0,
      10,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        5,
        15,
        mockCommitMetadata,
        mockCommitMetadata,
        20,
        false
      ) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test(
    "AQEPartitionCompletenessValidator should fail validation for overlapping map ranges- case 2") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      0,
      1,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    validator.validateSubPartition(
      1,
      2,
      3,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        1,
        2,
        mockCommitMetadata,
        mockCommitMetadata,
        20,
        false
      ) // This overlaps

    isValid shouldBe (true)
  }

  test("AQEPartitionCompletenessValidator should fail validation for overlapping map ranges - edge cases") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      0,
      10,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        0,
        5,
        mockCommitMetadata,
        mockCommitMetadata,
        20,
        false
      ) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test("AQEPartitionCompletenessValidator should fail validation for one map range subsuming another map range") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      5,
      10,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        0,
        15,
        mockCommitMetadata,
        mockCommitMetadata,
        20,
        false
      ) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test("AQEPartitionCompletenessValidator should fail validation for one map range subsuming another map range - 2") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      0,
      15,
      mockCommitMetadata,
      mockCommitMetadata,
      20,
      false
    ) // First call should add the range
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        5,
        10,
        mockCommitMetadata,
        mockCommitMetadata,
        20,
        false
      ) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test(
    "AQEPartitionCompletenessValidator should fail validation if commit metadata does not match") {
    val expectedCommitMetadata = new CommitMetadata()
    val actuaCommitMetadata = new CommitMetadata()
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(
      1,
      0,
      10,
      actuaCommitMetadata,
      expectedCommitMetadata,
      20,
      false
    ) // Write first partition

    // Second one with a different count
    val (isValid, message) =
      validator.validateSubPartition(
        1,
        0,
        10,
        new CommitMetadata(3, 3),
        expectedCommitMetadata,
        20,
        false)

    isValid should be(false)
    message should include("Commit Metadata for partition: 1 not matching for sub-partition")
  }

  test("pass validation if written counts are correct after all updates") {
    validator = new PartitionCompletenessValidator
    val expectedCommitMetadata = new CommitMetadata(0, 10)
    validator.validateSubPartition(
      1,
      0,
      10,
      new CommitMetadata(0, 5),
      expectedCommitMetadata,
      20,
      false)
    val (isValid, message) = validator.validateSubPartition(
      1,
      10,
      20,
      new CommitMetadata(0, 5),
      expectedCommitMetadata,
      20,
      false)

    isValid should be(true)
    message should be("Partition is complete")
  }

  test("handle multiple partitions correctly") {
    validator = new PartitionCompletenessValidator
    // Testing with multiple partitions to check isolation
    val expectedCommitMetadataForPartition1 = new CommitMetadata(0, 10)
    val expectedCommitMetadataForPartition2 = new CommitMetadata(0, 2)
    validator.validateSubPartition(
      1,
      0,
      10,
      new CommitMetadata(0, 5),
      expectedCommitMetadataForPartition1,
      20,
      false
    ) // Validate partition 1
    validator.validateSubPartition(
      2,
      0,
      5,
      new CommitMetadata(0, 2),
      expectedCommitMetadataForPartition2,
      10,
      false
    ) // Validate partition 2

    val (isValid1, message1) = validator.validateSubPartition(
      1,
      0,
      10,
      new CommitMetadata(0, 5),
      expectedCommitMetadataForPartition1,
      20,
      false)
    val (isValid2, message2) = validator.validateSubPartition(
      2,
      0,
      5,
      new CommitMetadata(0, 2),
      expectedCommitMetadataForPartition2,
      10,
      false)

    isValid1 should be(true)
    isValid2 should be(true)
    message1 should be("Partition is valid but still waiting for more data")
    message2 should be("Partition is valid but still waiting for more data")

    val (isValid3, message3) = validator.validateSubPartition(
      1,
      10,
      20,
      new CommitMetadata(0, 5),
      expectedCommitMetadataForPartition1,
      20,
      false)
    val (isValid4, message4) = validator.validateSubPartition(
      2,
      5,
      10,
      new CommitMetadata(),
      expectedCommitMetadataForPartition2,
      10,
      false)
    isValid3 should be(true)
    isValid4 should be(true)
    message3 should be("Partition is complete")
    message4 should be("Partition is complete")

  }
}
