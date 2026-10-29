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

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.xml.Node

import org.apache.spark.ui.UIUtils

import org.apache.celeborn.common.rpc.{RpcMethodMetrics, RpcMetricsSnapshot, RpcSource}

/**
 * Renders the RPC latency histograms of the driver's LifecycleManager as part of the Celeborn
 * tab: a one line summary plus a collapsible section, using the same markup as the other
 * sections of the tab. The data is read when the page is rendered because it is process local
 * and is not part of the Spark event log.
 */
private[celeborn] object CelebornRpcMetricsSection {

  /** Queue and process time are tracked next to the per method histograms in the same map. */
  val aggregateKeys: Set[String] = Set(RpcSource.QUEUE_TIME, RpcSource.PROCESS_TIME)

  private val sectionId = "aggregated-celebornRpcMetrics"
  private val timeColumns = Seq("Min", "Mean", "P50", "P75", "P95", "P99", "Max")
  private val headerClasses = Seq("sorttable_alpha") ++ Seq.fill(8)("sorttable_numeric")

  /**
   * Returns the summary line and the collapsible section for the given snapshot. Both are empty
   * when no per method RPC data is available, e.g. in the History Server, before the first
   * shuffle, or after the LifecycleManager has stopped.
   */
  def render(snapshot: Option[RpcMetricsSnapshot]): (Seq[Node], Seq[Node]) = {
    snapshot.flatMap { s =>
      val methods = perMethod(s)
      if (methods.isEmpty) {
        None
      } else {
        Some((summaryLine(methods), section(s, methods)))
      }
    }.getOrElse((Seq.empty, Seq.empty))
  }

  def perMethod(snapshot: RpcMetricsSnapshot): Seq[RpcMethodMetrics] =
    snapshot.methods.filterNot(m => aggregateKeys.contains(m.name))

  private def summaryLine(methods: Seq[RpcMethodMetrics]): Seq[Node] = {
    val slowest = methods.maxBy(_.p99)
    val totalCalls = methods.map(_.count).sum
    val text = methods.size + " types / " + formatCount(totalCalls) +
      " calls, slowest processing P99 " +
      displayName(slowest.name) + " " + formatMs(slowest.p99) + " ms"
    <li>
      <strong>LifecycleManager RPC: </strong>{text}
    </li>
  }

  private def section(snapshot: RpcMetricsSnapshot, methods: Seq[RpcMethodMetrics]): Seq[Node] = {
    val aggregateRows = snapshot.methods.filter(m => aggregateKeys.contains(m.name))
    val methodRows = methods.sortBy(m => -m.p99)
    val totalCalls = methods.map(_.count).sum

    val summary =
      <ul class="list-unstyled">
        <li>
          <strong>Endpoint: </strong>{snapshot.endpointName} (driver)
        </li>
        <li>
          <strong>Snapshot time: </strong>{formatTimestamp(snapshot.timestamp)}
        </li>
        <li>
          <strong>RPC queue size: </strong>
          {snapshot.queueSize} (max {snapshot.maxQueueLength})
        </li>
        <li>
          <strong>RPC types / total calls: </strong>
          {methods.size} / {formatCount(totalCalls)}
        </li>
        <li>
          <strong>Note: </strong>
          Times are in milliseconds and are cumulative since the LifecycleManager started;
          the histograms are never reset. Per-method times include processing only and exclude
          queueing, network, and client wait time.
        </li>
      </ul>

    val aggregateTable = UIUtils.listingTable(
      Seq("Metric", "Count") ++ timeColumns,
      metricRow,
      aggregateRows,
      fixedWidth = true,
      headerClasses = headerClasses)
    val methodTable = UIUtils.listingTable(
      Seq("Method", "Count") ++ timeColumns,
      metricRow,
      methodRows,
      fixedWidth = true,
      headerClasses = headerClasses)

    <span>
      <span class={s"collapse-$sectionId collapse-table"}
          onClick={s"collapseTable('collapse-$sectionId','$sectionId')"}>
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>LifecycleManager RPC Metrics</a>
        </h4>
      </span>
      <div class={s"$sectionId collapsible-table collapsed"}>
        {summary}
        <h4>
          <strong>Aggregate queue/process time</strong>
        </h4>
        {aggregateTable}
        <h4>
          <strong>Per RPC method processing time (sorted by P99 desc)</strong>
        </h4>
        {methodTable}
      </div>
    </span>
  }

  private def metricRow(method: RpcMethodMetrics): Seq[Node] =
    <tr>
      <td title={method.name}>{displayName(method.name)}</td>
      <td>{formatCount(method.count)}</td>
      <td>{formatMs(method.min)}</td>
      <td>{formatMs(method.mean)}</td>
      <td>{formatMs(method.p50)}</td>
      <td>{formatMs(method.p75)}</td>
      <td>{formatMs(method.p95)}</td>
      <td>{formatMs(method.p99)}</td>
      <td>{formatMs(method.max)}</td>
    </tr>

  /** Histogram keys are JVM class names or protobuf full names; only show the simple name. */
  def displayName(rawName: String): String = rawName.split('.').last.split('$').last

  def formatMs(nanos: Double): String =
    String.format(Locale.ROOT, "%.2f", Double.box(nanos / 1000000.0))

  def formatCount(count: Long): String =
    String.format(Locale.ROOT, "%,d", Long.box(count))

  def formatTimestamp(timestamp: Long): String =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).format(new Date(timestamp))
}
