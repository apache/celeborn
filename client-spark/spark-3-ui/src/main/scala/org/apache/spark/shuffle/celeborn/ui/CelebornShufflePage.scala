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

import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.RpcMetricsProvider
import org.apache.spark.shuffle.celeborn.ShuffleAssignmentUIData
import org.apache.spark.shuffle.celeborn.ui.SparkServletBridge.HttpServletRequest
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[celeborn] class CelebornShufflePage(parent: CelebornUITab)
  extends WebUIPage("") with Logging {

  private val store = parent.store

  override def render(request: HttpServletRequest): Seq[Node] = {
    try {
      renderBody(request)
    } catch {
      case e: Throwable =>
        logError("Failed to render Celeborn Shuffle page", e)
        val errorContent =
          <div class="row-fluid">
            <div class="span12">
              <h4>
                <strong>Celeborn Shuffle</strong>
              </h4>
              <div class="alert alert-error">
                <pre>Failed to render the Celeborn page: {e.getMessage}</pre>
              </div>
            </div>
          </div>
        UIUtils.headerSparkPage(request, "Celeborn Shuffle", errorContent, parent)
    }
  }

  private def renderBody(request: HttpServletRequest): Seq[Node] = {
    val taskInfo = store.aggregatedTaskInfo()
    val properties = store.celebornProperties()
    val buildInfo = store.buildInfo()
    val assignments = store.shuffleAssignments().sortBy(-_.shuffleId)
    val workerSlots = store.workerSlots()
    val workerRows = workerSlots.workerSlots.toSeq.map { case (worker, slots) =>
      (worker, workerSlots.workerShuffles.getOrElse(worker, 0), slots)
    }.sortBy { case (worker, _, slots) => (-slots, worker) }

    val (rpcSummary, rpcSection) =
      CelebornRpcMetricsSection.render(RpcMetricsProvider.snapshot())

    val writeBytes = taskInfo.shuffleWriteBytes
    val readBytes = taskInfo.shuffleReadBytes
    val writeMs = taskInfo.shuffleWriteTimeMs
    val readMs = taskInfo.shuffleFetchWaitTimeMs
    val durationMs = taskInfo.taskDurationMs
    val uncompressedBytes = taskInfo.uncompressedShuffleBytes
    val compressedBytes = taskInfo.compressedShuffleBytes

    def mbps(bytes: Long, ms: Long): String =
      if (ms <= 0) "N/A" else f"${bytes.toDouble / 1000.0 / 1000.0 / (ms.toDouble / 1000.0)}%.2f"
    def pct(part: Long, total: Long): String =
      if (total <= 0) "N/A" else f"${part.toDouble * 100.0 / total.toDouble}%.1f%%"
    def assignmentSummary: String =
      if (workerSlots.shuffles == 0) "N/A"
      else {
        s"Shuffles: ${workerSlots.shuffles} | Workers: ${workerRows.size} | " +
          s"Reserved Slots: ${workerRows.map(_._3.toLong).sum}"
      }
    val assignmentsTitle =
      if (workerSlots.shuffles > assignments.size) {
        s"Shuffle Assignments (${workerSlots.shuffles}, only showing ${assignments.size})"
      } else {
        "Shuffle Assignments"
      }

    // 0 when not reported, e.g. by old clients.
    def compressionRatio: String =
      if (uncompressedBytes <= 0 || compressedBytes <= 0) "N/A"
      else {
        s"${Utils.bytesToString(uncompressedBytes)}/${Utils.bytesToString(compressedBytes)} = " +
          f"${uncompressedBytes.toDouble / compressedBytes}%.2f"
      }

    val summary =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Shuffle Write: </strong>
            {
        s"${Utils.bytesToString(writeBytes)} | Time: ${UIUtils.formatDuration(
          writeMs)} | Speed: ${mbps(writeBytes, writeMs)} MB/s"
      }
          </li>
          <li>
            <strong>Shuffle Read: </strong>
            {
        s"${Utils.bytesToString(readBytes)} | Time: ${UIUtils.formatDuration(
          readMs)} | Speed: ${mbps(readBytes, readMs)} MB/s"
      }
          </li>
          <li>
            <strong>Compression Ratio (uncompressed/compressed): </strong>
            {compressionRatio}
          </li>
          <li>
            <strong>Shuffle Duration (write+read) / Task Duration: </strong>
            {
        s"${pct(writeMs + readMs, durationMs)} (Write ${pct(
          writeMs,
          durationMs)}, Read ${pct(readMs, durationMs)})"
      }
          </li>
          <li>
            <strong>Shuffle Assignments: </strong>
            {assignmentSummary}
          </li>
          {rpcSummary}
        </ul>
      </div>

    val propertiesTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      properties.info,
      fixedWidth = true,
      headerClasses = headerClasses)

    val shuffleAssignmentTable = UIUtils.listingTable(
      Seq(
        "Shuffle ID",
        "Celeborn Shuffle ID",
        "Workers",
        "Reserved Slots",
        "Slots per Worker (Min / Avg / Max)"),
      shuffleAssignmentRow,
      assignments,
      fixedWidth = true,
      headerClasses = Seq.fill(5)("sorttable_numeric"))

    val workerSlotsTable = UIUtils.listingTable(
      Seq("Worker", "Shuffles", "Reserved Slots"),
      workerSlotsRow,
      workerRows,
      fixedWidth = true,
      headerClasses = Seq("sorttable_alpha", "sorttable_numeric", "sorttable_numeric"))

    val buildInfoTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true,
      headerClasses = headerClasses)

    val content =
      <span>
        {summary}
        {rpcSection}
        <span class="collapse-aggregated-celebornBuildInfo collapse-table"
            onClick="collapseTable('collapse-aggregated-celebornBuildInfo',
            'aggregated-celebornBuildInfo')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Celeborn Build Information</a>
          </h4>
        </span>
        <div class="aggregated-celebornBuildInfo collapsible-table collapsed">
          {buildInfoTable}
        </div>
        <span class="collapse-aggregated-celebornProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-celebornProperties',
            'aggregated-celebornProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Celeborn Properties</a>
          </h4>
        </span>
        <div class="aggregated-celebornProperties collapsible-table collapsed">
          {propertiesTable}
        </div>
        <span class="collapse-aggregated-celebornShuffleAssignments collapse-table"
            onClick="collapseTable('collapse-aggregated-celebornShuffleAssignments',
            'aggregated-celebornShuffleAssignments')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>{assignmentsTitle}</a>
          </h4>
        </span>
        <div class="aggregated-celebornShuffleAssignments collapsible-table collapsed">
          {shuffleAssignmentTable}
        </div>
        <span class="collapse-aggregated-celebornWorkerSlots collapse-table"
            onClick="collapseTable('collapse-aggregated-celebornWorkerSlots',
            'aggregated-celebornWorkerSlots')">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Worker Slots</a>
          </h4>
        </span>
        <div class="aggregated-celebornWorkerSlots collapsible-table collapsed">
          {workerSlotsTable}
        </div>
      </span>

    UIUtils.headerSparkPage(request, "Celeborn Shuffle", content, parent)
  }

  private def propertyHeader: Seq[String] = Seq("Name", "Value")
  private def headerClasses: Seq[String] = Seq("sorttable_alpha", "sorttable_alpha")

  private def shuffleAssignmentRow(assignment: ShuffleAssignmentUIData): Seq[Node] = {
    val perWorker =
      if (assignment.workers == 0) "N/A"
      else {
        f"${assignment.minWorkerSlots} / ${assignment.slots.toDouble / assignment.workers}%.1f / " +
          s"${assignment.maxWorkerSlots}"
      }
    <tr>
      <td>{assignment.appShuffleId}</td>
      <td>{assignment.shuffleId}</td>
      <td>{assignment.workers}</td>
      <td>{assignment.slots}</td>
      <td sorttable_customkey={assignment.maxWorkerSlots.toString}>{perWorker}</td>
    </tr>
  }

  private def workerSlotsRow(row: (String, Int, Int)): Seq[Node] =
    <tr>
      <td>{row._1}</td>
      <td>{row._2}</td>
      <td>{row._3}</td>
    </tr>

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <tr>
      <td>{kv._1}</td>
      <td>{kv._2}</td>
    </tr>
}
