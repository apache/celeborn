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

import java.nio.file.Files
import java.util.concurrent.TimeoutException

import org.apache.spark.{Dependency, ShuffleDependency, TaskContext}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{DummyShuffleClient, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier

class CelebornShuffleReaderSuite extends AnyFunSuite {

  /**
   * Due to spark limitations, spark local mode can not test speculation tasks ,
   * test the method `checkAndReportFetchFailureForUpdateFileGroupFailure`
   */
  test("CELEBORN-1838 test check report fetch failure exceptions ") {
    val dependency = Mockito.mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handler = new CelebornShuffleHandle[Int, Int, Int](
      "APP",
      "HOST1",
      1,
      UserIdentifier.apply("a", "b"),
      0,
      true,
      1,
      dependency)
    val context = Mockito.mock(classOf[TaskContext])
    val metricReporter = Mockito.mock(classOf[ShuffleReadMetricsReporter])
    val conf = new CelebornConf()

    val tmpFile = Files.createTempFile("test", ".tmp").toFile
    mockStatic(classOf[ShuffleClient]).when(() =>
      ShuffleClient.get(any(), any(), any(), any(), any(), any())).thenReturn(
      new DummyShuffleClient(conf, tmpFile))

    val shuffleReader =
      new CelebornShuffleReader[Int, Int](handler, 0, 0, 0, 0, context, conf, metricReporter, null)

    val exception1: Throwable = new CelebornIOException("test1", new InterruptedException("test1"))
    val exception2: Throwable = new CelebornIOException("test2", new TimeoutException("test2"))
    val exception3: Throwable = new CelebornIOException("test3")
    val exception4: Throwable = new CelebornIOException("test4")

    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception1)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception2)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception3)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 1)
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception4)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 2)

  }
}
