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

package org.apache.spark.shuffle.celeborn.ui

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.celeborn.{CelebornListener, CelebornStatusStore}
import org.apache.spark.shuffle.celeborn.events._
import org.apache.spark.util.kvstore.InMemoryStore
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.protocol.message.{PushWorkerStats, ReadMetrics, WorkerReadCost, WriteMetrics}

/**
 * Renders the page content against an in-memory store. buildContent is exercised directly
 * (no SparkUI / servlet request) so the assertions stay at the XML-string level.
 */
@RunWith(classOf[JUnit4])
class CelebornShufflePageSuite {

  @Test
  def rendersAllSectionsOnEmptyStore(): Unit = {
    val statusStore = new CelebornStatusStore(new InMemoryStore())
    val xml = CelebornShufflePage.buildContent(statusStore).toString
    Seq(
      "Celeborn Build Information",
      "Celeborn Properties",
      "Per-Shuffle Details",
      "Shuffle Throughput",
      "Shuffle Write Times",
      "Shuffle Read Times",
      "Shuffle Write Servers",
      "Shuffle Read Servers",
      "Shuffle Assignments",
      "Shuffle Fallback",
      "No fallback recorded.").foreach { section =>
      assertTrue(s"missing section: $section", xml.contains(section))
    }
    // Empty store must not surface raw null / exception placeholders.
    assertFalse(xml.contains("null"))
  }

  @Test
  def rendersPopulatedSectionsAndDerivedColumns(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornBuildInfoEvent(Map("Spark Version" -> "3.5.0")))
    listener.onOtherEvent(CelebornShuffleAssignmentEvent(
      0,
      100,
      java.util.Arrays.asList("host1:9097", "host2:9097"),
      16,
      1L))
    listener.onOtherEvent(CelebornFallbackEvent(
      java.util.Collections.singletonMap("pushTimeout", 2L: java.lang.Long),
      1L))
    listener.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
      java.util.Arrays.asList(
        new PushWorkerStats("hostA:9097", 10L, 5000L, 10000000L, 0L, 0L, 0L, 0L, ""),
        new PushWorkerStats("hostB:9097", 4L, 1000L, 1000000L, 0L, 0L, 0L, 0L, "")),
      1L))
    listener.onOtherEvent(CelebornReadMetricsEvent(
      0,
      new ReadMetrics(1L, 2L, 3L, 4L, 0L, 0L, 0L, 0L, 0L, 0L),
      java.util.Arrays.asList(
        new WorkerReadCost("hostC:9097", 5L, 900L, 4000000L, 2000000L),
        new WorkerReadCost("hostD:9097", 3L, 300L, 3000000L, 1000000L)),
      2L))
    listener.flush()

    val xml = CelebornShufflePage.buildContent(statusStore).toString

    // Build info and fallback sections surface their stored values.
    assertTrue(xml.contains("Spark Version"))
    assertTrue(xml.contains("3.5.0"))
    assertTrue(xml.contains("pushTimeout"))

    // Per-shuffle details: the assignment row joins with the read metrics. Read bytes
    // = 900 + 300 = 1200 B over 10 ms fetch time -> 0.12 MB/s derived speed.
    assertTrue(xml.contains("100"))
    assertTrue(xml.contains("0.12"))

    // Read worker table: hostC (900 B) sorts before hostD (300 B), and avg RTT is
    // rendered (hostC: 4 ms / 5 chunks = 0.80, hostD: 3 ms / 3 chunks = 1.00).
    assertTrue(xml.indexOf("hostC:9097") < xml.indexOf("hostD:9097"))
    assertTrue(xml.contains("0.80"))

    // Write worker table: hostA (5000 B) sorts before hostB (1000 B).
    assertTrue(xml.indexOf("hostA:9097") < xml.indexOf("hostB:9097"))
  }
}
