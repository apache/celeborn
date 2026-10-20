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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, mockStatic, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.commit.MissingPartitionIntegrityTestSupport._
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.util.ThreadUtils

class MissingPartitionShuffleReaderSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var previousStreamCreatorPool: java.util.concurrent.ThreadPoolExecutor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    previousStreamCreatorPool = CelebornShuffleReader.streamCreatorPool
  }

  override def afterAll(): Unit = {
    try {
      if (previousStreamCreatorPool == null && CelebornShuffleReader.streamCreatorPool != null) {
        ThreadUtils.shutdown(CelebornShuffleReader.streamCreatorPool)
        CelebornShuffleReader.streamCreatorPool = null
      }
    } finally {
      super.afterAll()
    }
  }

  private def withReader(
      nonempty: Boolean = true,
      emptyLocationEntry: Boolean = false,
      integrityCheckEnabled: Boolean = true,
      stageRerunEnabled: Boolean = false)(
      body: (Fixture, CelebornShuffleReader[Int, Int]) => Unit): Unit = {
    val fixture = new Fixture(
      nonempty = nonempty,
      emptyLocationEntry = emptyLocationEntry,
      integrityCheckEnabled = integrityCheckEnabled)
    val clientFactory = mockStatic(classOf[ShuffleClient])
    try {
      clientFactory.when(() =>
        ShuffleClient.get(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(fixture.client)
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(new JavaSerializer(new SparkConf(false)))
      when(dependency.keyOrdering).thenReturn(None)
      when(dependency.aggregator).thenReturn(None)
      val handle = new CelebornShuffleHandle[Int, Int, Int](
        "missing-partition-repro",
        "localhost",
        0,
        UserIdentifier("repro", "repro"),
        shuffleId,
        stageRerunEnabled,
        1,
        dependency)
      val context = mock(classOf[TaskContext])
      when(context.taskMetrics()).thenReturn(TaskMetrics.empty)
      val reader = new CelebornShuffleReader[Int, Int](
        handle,
        partitionId,
        partitionId + 1,
        0,
        Int.MaxValue,
        context,
        fixture.conf,
        mock(classOf[ShuffleReadMetricsReporter]),
        new ExecutorShuffleIdTracker())
      body(fixture, reader)
    } finally {
      clientFactory.close()
      fixture.close()
    }
  }

  for {
    emptyLocationEntry <- Seq(false, true)
    stageRerunEnabled <- Seq(false, true)
  } {
    test(s"requested nonempty partition cannot disappear from a Spark read: emptyLocationEntry=$emptyLocationEntry, stageRerun=$stageRerunEnabled") {
      withReader(
        emptyLocationEntry = emptyLocationEntry,
        stageRerunEnabled = stageRerunEnabled) { (fixture, reader) =>
        val outcome = scala.util.Try(reader.read().toList)
        info(s"SPARK REPRO expectedBytes=${fixture.expected.getBytes}, outcome=$outcome, " +
          s"integrityReports=${fixture.reports.size}, fetchFailures=${fixture.fetchFailures.get()}")
        val error = outcome.failed.toOption.getOrElse(
          fail(s"Spark reader accepted missing output: ${outcome.get}"))
        if (stageRerunEnabled) {
          assert(error.isInstanceOf[FetchFailedException])
          assert(error.getCause.isInstanceOf[CelebornIOException])
          assert(error.getCause.getMessage.contains("CommitMetadata mismatch"))
          assert(fixture.fetchFailures.get() == 1)
        } else {
          assert(error.isInstanceOf[CelebornIOException])
          assert(error.getMessage.contains("CommitMetadata mismatch"))
          assert(fixture.fetchFailures.get() == 0)
        }
        assert(fixture.fileGroupRequests.get() == 1)
        assert(fixture.reports.size == 1)
        assert(fixture.reports.head.getPartitionId == partitionId)
        assert(fixture.reports.head.getBytesWritten == 0)
      }
    }
  }

  test("Spark validates a legitimate empty requested partition and returns no records") {
    withReader(nonempty = false) { (fixture, reader) =>
      assert(reader.read().toList.isEmpty)
      assert(fixture.reports.size == 1)
      assert(fixture.reports.head.getPartitionId == partitionId)
      assert(fixture.reports.head.getBytesWritten == 0)
    }
  }

  test("Spark preserves the empty-partition fast path when integrity checking is disabled") {
    withReader(integrityCheckEnabled = false) { (fixture, reader) =>
      assert(reader.read().toList.isEmpty)
      assert(fixture.reports.isEmpty)
    }
  }
}
