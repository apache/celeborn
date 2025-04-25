package org.apache.celeborn.client.commit

import org.scalatest.matchers.must.Matchers.{be, include}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import org.apache.celeborn.CelebornFunSuite

class AQEPartitionValidatorTest extends CelebornFunSuite {

  var validator: PartitionCompletenessValidator = _

  test("AQEPartitionCompletenessValidator should validate a new sub-partition correctly when there are no overlapping ranges") {
    validator = new PartitionCompletenessValidator
    val (isValid, message) = validator.validateSubPartition(1, 0, 10, 5, 10, 20)

    isValid shouldBe (true)
    message shouldBe ("Partition is valid but still waiting for more data")
  }

  test("AQEPartitionCompletenessValidator should fail validation for overlapping map ranges") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 10, 5, 10, 20) // First call should add the range
    val (isValid, message) = validator.validateSubPartition(1, 5, 15, 3, 10, 20) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test(
    "AQEPartitionCompletenessValidator should fail validation for overlapping map ranges- case 2") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 1, 1, 10, 20) // First call should add the range
    validator.validateSubPartition(1, 2, 3, 1, 10, 20) // First call should add the range
    val (isValid, message) = validator.validateSubPartition(1, 1, 2, 1, 10, 20) // This overlaps

    isValid shouldBe (true)
  }

  test("AQEPartitionCompletenessValidator should fail validation for overlapping map ranges - edge cases") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 10, 5, 10, 20) // First call should add the range
    val (isValid, message) = validator.validateSubPartition(1, 0, 5, 3, 10, 20) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test("AQEPartitionCompletenessValidator should fail validation for one map range subsuming another map range") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 5, 10, 5, 10, 20) // First call should add the range
    val (isValid, message) = validator.validateSubPartition(1, 0, 15, 3, 10, 20) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test("AQEPartitionCompletenessValidator should fail validation for one map range subsuming another map range - 2") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 15, 5, 10, 20) // First call should add the range
    val (isValid, message) = validator.validateSubPartition(1, 5, 10, 3, 10, 20) // This overlaps

    isValid shouldBe (false)
    message should include("Encountered overlapping map range for partitionId: 1")
  }

  test("AQEPartitionCompletenessValidator should fail validation if written mapper count does not match") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 10, 5, 5, 20) // Write first partition

    // Second one with a different count
    val (isValid, message) = validator.validateSubPartition(1, 0, 10, 3, 5, 20)

    isValid should be(false)
    message should include("Written mapper count for partition: 1 not matching for sub-partition")
  }

  test("pass validation if written counts are correct after all updates") {
    validator = new PartitionCompletenessValidator
    validator.validateSubPartition(1, 0, 10, 5, 10, 20)
    val (isValid, message) = validator.validateSubPartition(1, 10, 20, 5, 10, 20)

    isValid should be(true)
    message should be("Partition is complete")
  }

  test("handle multiple partitions correctly") {
    validator = new PartitionCompletenessValidator
    // Testing with multiple partitions to check isolation
    validator.validateSubPartition(1, 0, 10, 5, 10, 20) // Validate partition 1
    validator.validateSubPartition(2, 0, 5, 2, 2, 10) // Validate partition 2

    val (isValid1, message1) = validator.validateSubPartition(1, 0, 10, 5, 10, 20)
    val (isValid2, message2) = validator.validateSubPartition(2, 0, 5, 2, 2, 10)

    isValid1 should be(true)
    isValid2 should be(true)
    message1 should be("Partition is valid but still waiting for more data")
    message2 should be("Partition is valid but still waiting for more data")

    val (isValid3, message3) = validator.validateSubPartition(1, 10, 20, 5, 10, 20)
    val (isValid4, message4) = validator.validateSubPartition(2, 5, 10, 0, 2, 10)
    isValid3 should be(true)
    isValid4 should be(true)
    message3 should be("Partition is complete")
    message4 should be("Partition is complete")

  }
}
