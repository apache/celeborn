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

import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, include, not}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CommitMetadata

class LegacySkewHandlingPartitionValidatorTest extends CelebornFunSuite {

  // Reproduces the object-aliasing bug:
  // When sub-range A [0,585) is processed first, its CommitMetadata object (OBJ_A) is stored
  // in both subRangeToCommitMetadataMap and currentCommitMetadataForReducer as the same
  // reference. When sibling sub-range B [585,910) arrives, metadataMergeBiFunction mutates
  // OBJ_A in-place via addCommitData(). The TreeMap entry for [0,585) now silently holds
  // bytes(A) + bytes(B) instead of bytes(A). Any retry of sub-range A sends bytes(A) but the
  // validator finds bytes(A)+bytes(B) stored, causing a false mismatch.
  test("sibling sub-range arrival must not corrupt the stored metadata of an earlier sub-range") {
    val validator = new LegacySkewHandlingPartitionValidator
    val partitionId = 387
    val numMappers = 910

    val bytesA = 650884788L
    val bytesB = 1158249332L
    val metadataA = new CommitMetadata(1, bytesA)
    val metadataB = new CommitMetadata(2, bytesB)

    // Sub-range [0, 585) arrives first and is stored successfully.
    val (ok1, _) = validator.processSubPartition(partitionId, 0, 585, metadataA, numMappers)
    ok1 shouldBe true

    // Sibling sub-range [585, 910) arrives. Before the fix, this mutates the object stored
    // for [0, 585), corrupting it from bytesA to bytesA+bytesB.
    val (ok2, _) = validator.processSubPartition(partitionId, 585, 910, metadataB, numMappers)
    ok2 shouldBe true

    // Now simulate a task retry: sub-range [0, 585) is submitted again with the same correct
    // metadata. Before the fix this fails because the stored value is now bytesA+bytesB.
    val metadataARetry = new CommitMetadata(1, bytesA)
    val (ok3, msg3) =
      validator.processSubPartition(partitionId, 0, 585, metadataARetry, numMappers)
    ok3 shouldBe true
    msg3 should not include "not matching"
  }

  test("two sub-ranges complete a partition when bytes sum matches expected") {
    val validator = new LegacySkewHandlingPartitionValidator
    val partitionId = 1
    val numMappers = 10

    val (ok1, _) =
      validator.processSubPartition(partitionId, 0, 5, new CommitMetadata(0, 100), numMappers)
    ok1 shouldBe true
    validator.isPartitionComplete(partitionId) shouldBe false

    val (ok2, _) =
      validator.processSubPartition(partitionId, 5, 10, new CommitMetadata(0, 200), numMappers)
    ok2 shouldBe true
    validator.isPartitionComplete(partitionId) shouldBe true

    validator.currentCommitMetadata(partitionId).getBytes shouldBe 300L
  }

  test("retry of a sub-range after all siblings completed must succeed") {
    val validator = new LegacySkewHandlingPartitionValidator
    val partitionId = 5
    val numMappers = 4

    // Process 4 non-overlapping sub-ranges of size 1 each
    validator.processSubPartition(partitionId, 0, 1, new CommitMetadata(1, 100), numMappers)
    validator.processSubPartition(partitionId, 1, 2, new CommitMetadata(2, 200), numMappers)
    validator.processSubPartition(partitionId, 2, 3, new CommitMetadata(3, 300), numMappers)
    validator.processSubPartition(partitionId, 3, 4, new CommitMetadata(4, 400), numMappers)
    validator.isPartitionComplete(partitionId) shouldBe true

    // Retry of the first sub-range — must not be poisoned by sibling accumulation
    val (ok, msg) =
      validator.processSubPartition(partitionId, 0, 1, new CommitMetadata(1, 100), numMappers)
    ok shouldBe true
    msg should not include "not matching"
  }
}
