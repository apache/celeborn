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

import java.util.Locale

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.rpc.{RpcMethodMetrics, RpcMetricsSnapshot}

@RunWith(classOf[JUnit4])
class CelebornRpcMetricsSectionSuite {

  @Test
  def displayNameShortensClassAndProtobufNames(): Unit = {
    assertEquals(
      "Revive",
      CelebornRpcMetricsSection.displayName(
        "class org.apache.celeborn.common.protocol.message.ControlMessages$Revive"))
    assertEquals(
      "PbRevive",
      CelebornRpcMetricsSection.displayName("org.apache.celeborn.proto.PbRevive"))
    assertEquals("RpcQueueTime", CelebornRpcMetricsSection.displayName("RpcQueueTime"))
    assertEquals("unknown", CelebornRpcMetricsSection.displayName("unknown"))
  }

  @Test
  def formatsTimesAndCountsIndependentOfDefaultLocale(): Unit = {
    val previous = Locale.getDefault
    try {
      Locale.setDefault(Locale.GERMANY)
      assertEquals("1.50", CelebornRpcMetricsSection.formatMs(1500000.0))
      assertEquals("0.00", CelebornRpcMetricsSection.formatMs(0.0))
      assertEquals("12,345", CelebornRpcMetricsSection.formatCount(12345L))
    } finally {
      Locale.setDefault(previous)
    }
  }

  @Test
  def renderIsEmptyWithoutPerMethodData(): Unit = {
    val (noSummary, noSection) = CelebornRpcMetricsSection.render(None)
    assertTrue(noSummary.isEmpty)
    assertTrue(noSection.isEmpty)

    val onlyAggregates = RpcMetricsSnapshot(
      "LifecycleManagerEndpoint",
      0,
      0,
      Seq(RpcMethodMetrics("RpcQueueTime", 1, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)),
      0L)
    val (summary, section) = CelebornRpcMetricsSection.render(Some(onlyAggregates))
    assertTrue(summary.isEmpty)
    assertTrue(section.isEmpty)
  }

  @Test
  def renderSummaryAndSectionWithData(): Unit = {
    val snapshot = RpcMetricsSnapshot(
      "LifecycleManagerEndpoint",
      1,
      2,
      Seq(
        RpcMethodMetrics("RpcQueueTime", 3, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1),
        RpcMethodMetrics(
          "class a.b.ControlMessages$Revive",
          2,
          1000000,
          2000000.0,
          1000000.0,
          1000000.0,
          1000000.0,
          3000000.0,
          4000000),
        RpcMethodMetrics(
          "MapperEnd",
          1,
          500000,
          500000.0,
          500000.0,
          500000.0,
          500000.0,
          500000.0,
          500000)),
      1234L)

    val (summary, section) = CelebornRpcMetricsSection.render(Some(snapshot))
    val summaryText = summary.map(_.toString).mkString
    assertTrue(summaryText.contains("2 types / 3 calls"))
    assertTrue(summaryText.contains("slowest processing P99 Revive 3.00 ms"))

    val sectionText = section.map(_.toString).mkString
    assertTrue(sectionText.contains("LifecycleManager RPC Metrics"))
    assertTrue(sectionText.contains("aggregated-celebornRpcMetrics"))
    assertTrue(sectionText.contains("Aggregate queue/process time"))
    assertTrue(sectionText.contains("Per RPC method processing time (sorted by P99 desc)"))
    assertTrue(sectionText.contains("queueing, network, and client wait time"))
    assertTrue(sectionText.indexOf("Revive") < sectionText.indexOf("MapperEnd"))
  }
}
