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

import java.util.concurrent.{Callable, CountDownLatch, FutureTask, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CommitMetadata

class PartitionValidatorRetryTest extends CelebornFunSuite {

  private def report(
      validator: PartitionCompletenessValidator,
      withoutMapRange: Boolean,
      index: Int,
      metadata: CommitMetadata,
      expected: CommitMetadata = new CommitMetadata(30, 300),
      partitionId: Int = 0): (Boolean, String) = {
    // Each RPC constructs a new actual-metadata instance on the driver.
    validator.validateSubPartition(
      partitionId,
      if (withoutMapRange) 2 else index,
      if (withoutMapRange) index else index + 1,
      new CommitMetadata(metadata.getChecksum, metadata.getBytes),
      expected,
      2,
      withoutMapRange)
  }

  Seq(false, true).foreach { withoutMapRange =>
    for {
      badIndex <- Seq(0, 1)
      corruptChecksum <- Seq(false, true)
    } {
      test(s"recover a failed report without double counting: withoutMapRange=$withoutMapRange, badIndex=$badIndex, corruptChecksum=$corruptChecksum") {
        val validator = new PartitionCompletenessValidator
        val correct = Seq(new CommitMetadata(10, 100), new CommitMetadata(20, 200))
        val bad = new CommitMetadata(
          correct(badIndex).getChecksum + (if (corruptChecksum) 1 else 0),
          correct(badIndex).getBytes - (if (corruptChecksum) 0 else 1))

        assert(report(validator, withoutMapRange, 0, if (badIndex == 0) bad else correct(0))._1)
        val failed =
          report(validator, withoutMapRange, 1, if (badIndex == 1) bad else correct(1))
        assert(!failed._1)
        assert(failed._2.contains("failed validation check"))
        assert(!report(validator, withoutMapRange, badIndex, bad)._1)

        val recovered = report(validator, withoutMapRange, badIndex, correct(badIndex))
        assert(recovered == ((true, "Partition is complete")))
        correct.indices.foreach { index =>
          assert(report(validator, withoutMapRange, index, correct(index))._1)
        }

        // A late report from the failed read must not replace a validated result.
        assert(!report(validator, withoutMapRange, badIndex, bad)._1)
        assert(report(validator, withoutMapRange, badIndex, correct(badIndex))._1)
      }
    }

    test(
      s"recover multiple failed sub-partitions incrementally: withoutMapRange=$withoutMapRange") {
      val validator = new PartitionCompletenessValidator
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(11, 100))._1)
      assert(!report(validator, withoutMapRange, 1, new CommitMetadata(20, 199))._1)

      // Replacing only the first bad report still leaves the byte count incorrect.
      val partialRecovery = report(validator, withoutMapRange, 0, new CommitMetadata(10, 100))
      assert(!partialRecovery._1)
      assert(partialRecovery._2.contains("failed validation check"))
      assert(!report(validator, withoutMapRange, 0, new CommitMetadata(10, 100))._1)

      assert(report(validator, withoutMapRange, 1, new CommitMetadata(20, 200)) ==
        ((true, "Partition is complete")))
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(10, 100))._1)
    }

    test(s"recovery is isolated to the failed parent partition: withoutMapRange=$withoutMapRange") {
      val validator = new PartitionCompletenessValidator
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(11, 100))._1)
      assert(!report(validator, withoutMapRange, 1, new CommitMetadata(20, 200))._1)

      assert(report(
        validator,
        withoutMapRange,
        0,
        new CommitMetadata(10, 100),
        partitionId = 1)._1)
      assert(!report(
        validator,
        withoutMapRange,
        0,
        new CommitMetadata(11, 100),
        partitionId = 1)._1)
      assert(report(
        validator,
        withoutMapRange,
        1,
        new CommitMetadata(20, 200),
        partitionId = 1)._1)

      assert(report(validator, withoutMapRange, 0, new CommitMetadata(10, 100))._1)
    }

    test(s"recovery preserves the existing split boundaries: withoutMapRange=$withoutMapRange") {
      val validator = new PartitionCompletenessValidator
      val expected = new CommitMetadata(30, 300)
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(11, 100))._1)
      assert(!report(validator, withoutMapRange, 1, new CommitMetadata(20, 200))._1)

      if (withoutMapRange) {
        intercept[IllegalStateException] {
          validator.validateSubPartition(0, 3, 0, new CommitMetadata(10, 100), expected, 2, true)
        }
      } else {
        val overlap =
          validator.validateSubPartition(0, 0, 2, expected, expected, 2, false)
        assert(!overlap._1)
        assert(overlap._2.contains("overlapping map range"))
      }
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(10, 100))._1)
    }

    test(s"publish recovery before admitting a concurrent stale report: withoutMapRange=$withoutMapRange") {
      val validator = new PartitionCompletenessValidator
      val expected = new PausedExpectedMetadata
      assert(report(validator, withoutMapRange, 0, new CommitMetadata(11, 100), expected)._1)
      assert(!report(validator, withoutMapRange, 1, new CommitMetadata(20, 200), expected)._1)

      expected.pauseNextCheck.set(true)
      val (retryThread, retry) = startReport {
        report(validator, withoutMapRange, 0, new CommitMetadata(10, 100), expected)
      }
      var staleThread: Thread = null
      try {
        assert(
          expected.entered.await(10, TimeUnit.SECONDS),
          "correct retry did not reach aggregate validation")
        val staleStarted = new CountDownLatch(1)
        val (thread, stale) = startReport {
          staleStarted.countDown()
          report(validator, withoutMapRange, 0, new CommitMetadata(11, 100), expected)
        }
        staleThread = thread
        assert(staleStarted.await(10, TimeUnit.SECONDS), "stale report did not start")
        // A stale report must wait for the paused validation to publish its recovery state.
        intercept[TimeoutException] {
          stale.get(100, TimeUnit.MILLISECONDS)
        }

        expected.resume.countDown()
        assert(retry.get(10, TimeUnit.SECONDS) == ((true, "Partition is complete")))
        assert(!stale.get(10, TimeUnit.SECONDS)._1)
        assert(report(validator, withoutMapRange, 0, new CommitMetadata(10, 100), expected)._1)
      } finally {
        expected.resume.countDown()
        Seq(retryThread, staleThread).filter(_ != null).foreach { thread =>
          thread.join(10000)
          assert(!thread.isAlive, "validation thread did not exit")
        }
      }
    }
  }

  private class PausedExpectedMetadata extends CommitMetadata(30, 300) {
    val pauseNextCheck = new AtomicBoolean(false)
    val entered = new CountDownLatch(1)
    val resume = new CountDownLatch(1)

    override def getChecksum: Int = {
      if (pauseNextCheck.getAndSet(false)) {
        entered.countDown()
        assert(resume.await(30, TimeUnit.SECONDS), "validation was not resumed")
      }
      super.getChecksum
    }
  }

  private def startReport(
      body: => (Boolean, String)): (Thread, FutureTask[(Boolean, String)]) = {
    val task = new FutureTask[(Boolean, String)](new Callable[(Boolean, String)] {
      override def call(): (Boolean, String) = body
    })
    val thread = new Thread(task, "test-partition-validation")
    thread.setDaemon(true)
    thread.start()
    (thread, task)
  }
}
