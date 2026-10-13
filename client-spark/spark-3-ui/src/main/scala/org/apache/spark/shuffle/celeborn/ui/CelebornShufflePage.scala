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

import scala.collection.JavaConverters._
import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.CelebornStatusStore
import org.apache.spark.shuffle.celeborn.ui.SparkServletBridge.HttpServletRequest
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * Page rendered under the Celeborn tab. Uses [[SparkServletBridge]] to bridge the
 *  javax.servlet (Spark 3.x) / jakarta.servlet (Spark 4.x) parameter type. The content
 *  sections are built by the companion object's buildContent so they can be unit-tested
 *  against an in-memory store without a live SparkUI or servlet request.
 */
private[celeborn] class CelebornShufflePage(parent: CelebornUITab)
  extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {
    try {
      UIUtils.headerSparkPage(
        request,
        "Celeborn Shuffle Service",
        CelebornShufflePage.buildContent(parent.store),
        parent)
    } catch {
      case e: Throwable =>
        logError("Failed to render Celeborn Shuffle page", e)
        val errorContent =
          <div class="row-fluid">
            <div class="span12">
              <h4>
                <strong>Celeborn Shuffle Service</strong>
              </h4>
              <div class="alert alert-error">
                <pre>Failed to render the Celeborn page: {e.getMessage}</pre>
              </div>
            </div>
          </div>
        UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", errorContent, parent)
    }
  }
}

private[celeborn] object CelebornShufflePage {

  /** Renders everything below the Spark UI header: stat cards, summary, sections. */
  def buildContent(statusStore: CelebornStatusStore): Seq[Node] = {
    val assignments = statusStore.assignmentInfos()
    val properties = statusStore.celebornProperties()
    val reassign = statusStore.reassignStats()
    val taskInfo = statusStore.aggregatedTaskInfo()
    val writeTimes = statusStore.writeTimes()
    val perWorkerStats = statusStore.perWorkerWriteStats()
    val readTimes = statusStore.readTimes()
    val perWorkerReadStats = statusStore.perWorkerReadStats()
    // Entities persisted by the listener but rendered only by the enhanced page:
    // build/version info, per-policy fallback counts and per-shuffle write metrics.
    val buildInfo = statusStore.buildInfo()
    val fallback = statusStore.fallbackStats()
    val perShuffleWrite = statusStore.aggregatedWriteMetrics()
    val perShuffleRead = statusStore.aggregatedReadMetrics()

    // --- Summary derived from onTaskEnd TaskMetrics (plan A: no executor->driver metric RPC) ---
    val writeBytes = taskInfo.shuffleWriteBytes
    val readBytes = taskInfo.shuffleReadBytes
    val writeMs = taskInfo.shuffleWriteTimeMs
    val readMs = taskInfo.shuffleFetchWaitTimeMs
    val durationMs = taskInfo.taskDurationMs
    def mbps(bytes: Long, ms: Long): String =
      if (ms <= 0) "N/A" else f"${bytes.toDouble / 1000.0 / 1000.0 / (ms.toDouble / 1000.0)}%.2f"
    def pct(part: Long, total: Long): String =
      if (total <= 0) "N/A" else f"${part.toDouble * 100.0 / total.toDouble}%.1f%%"
    val compressionRatio: String =
      if (writeBytes > 0) f"${writeTimes.uncompressedBytes.toDouble / writeBytes.toDouble}%.2f"
      else "N/A"

    // Per-shuffle join: assignments (primary side) outer-joined with the write and read
    // metric maps on the shuffle id. Assignments can be evicted by retainedShuffles while
    // the metric aggregations (keyed by shuffleId, not row-capped) are still present, so
    // metric-only ids are kept as rows with default columns instead of being dropped.
    val writeMetrics = perShuffleWrite.metrics.asScala
    val readMetrics = perShuffleRead.metrics.asScala
    val assignmentIds = assignments.map(_.appShuffleId).toSet
    val perShuffleIds =
      (assignmentIds ++ (writeMetrics.keySet ++ readMetrics.keySet)).toSeq.sorted

    val summary: Seq[Node] =
      <div>
        <ul class="list-unstyled">
          <li>Total Shuffle Write Bytes: {Utils.bytesToString(writeBytes)}</li>
          <li>Total Shuffle Read Bytes: {Utils.bytesToString(readBytes)}</li>
          <li>Compression Ratio: {compressionRatio}
            (uncompressed {Utils.bytesToString(writeTimes.uncompressedBytes)}
            / compressed {Utils.bytesToString(writeBytes)})</li>
          <li>Client Observed Write Speed: {mbps(writeBytes, writeMs)} MB/s</li>
          <li>Client Observed Read Speed: {mbps(readBytes, readMs)} MB/s</li>
          <li>Shuffle Write Time / Task Duration: {pct(writeMs, durationMs)}</li>
          <li>Shuffle Read Time / Task Duration: {pct(readMs, durationMs)}</li>
          <li>Shuffle Duration (write+read) / Task Duration: {pct(writeMs + readMs, durationMs)}</li>
          <li>Shuffle Adjustments: partitionSplit={reassign.partitionSplit},
            reviveTriggered={reassign.reviveTriggered}, stageRetry={reassign.stageRetry}</li>
          <li>
            <a href="#build-info">Celeborn Build Information</a>
          </li>
          <li>
            <a href="#properties">Celeborn Properties</a> ({properties.info.length} entries)
          </li>
          <li>
            <a href="#per-shuffle">Per-Shuffle Details</a> ({perShuffleIds.size} shuffles)
          </li>
          <li>
            <a href="#throughput">Shuffle Throughput</a>
          </li>
          <li>
            <a href="#write-times">Shuffle Write Times</a>
          </li>
          <li>
            <a href="#read-times">Shuffle Read Times</a>
          </li>
          <li>
            <a href="#write-servers">Shuffle Write Servers</a>
          </li>
          <li>
            <a href="#read-servers">Shuffle Read Servers</a>
          </li>
          <li>
            <a href="#assignments">Shuffle Assignments</a> ({assignments.length} shuffles)
          </li>
          <li>
            <a href="#fallback">Shuffle Fallback</a> ({fallback.counts.size} policies)
          </li>
        </ul>
      </div>

    // Uniffle-style summary cards: headline numbers rendered as tiles above the detail
    // sections. Only Spark UI's own grid classes are used (row-fluid/span2, valid through
    // Spark 3.5); on Spark 4.x (Bootstrap 5) the unknown classes degrade to a vertical
    // stack, which stays readable without shipping any extra CSS/JS.
    val workerCount =
      (perWorkerStats.map(_.workerId) ++ perWorkerReadStats.map(_.workerId)).toSet.size
    val fallbackShuffleTotal = fallback.counts.asScala.values.map(_.toLong).sum
    val summaryCards: Seq[Node] =
      <div class="row-fluid">
        {Seq(
          ("Shuffles", perShuffleIds.size.toString),
          ("Workers", workerCount.toString),
          ("Total Write Bytes", Utils.bytesToString(writeBytes)),
          ("Total Read Bytes", Utils.bytesToString(readBytes)),
          ("Compression Ratio", compressionRatio),
          ("Fallback Shuffles", fallbackShuffleTotal.toString)).map { case (label, value) =>
          statTile(label, value)
        }}
      </div>

    // Build info uses the same two-column listing as the properties section; both are
    // Seq[(String, String)] key/value summaries written once at startup.
    val buildInfoTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true)

    // Fallback counts are a policy -> count snapshot; sorted by policy name for a
    // stable display. An empty map still renders the section (with a placeholder row)
    // so the anchor stays valid.
    val fallbackRows = fallback.counts.asScala.toSeq.sortBy(_._1).map { case (policy, count) =>
      <tr>
        <td>{policy}</td>
        <td>{count.toString}</td>
      </tr>
    }
    val fallbackTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Fallback Policy</th>
            <th>Count</th>
          </tr>
        </thead>
        <tbody>
          {
        if (fallbackRows.isEmpty) {
          <tr>
            <td>No fallback recorded.</td>
            <td></td>
          </tr>
        } else {
          fallbackRows
        }
      }
        </tbody>
      </table>

    // Per-Shuffle Details: assignments outer-joined with per-shuffle write and read metrics,
    // with client-side derived columns (% of total write, write/read speed) in the style of
    // Uniffle's per-application table. Rows are sorted by shuffle id (default ordering).
    val perShuffleRows = perShuffleIds.map { shuffleId =>
      val assignment = assignments.find(_.appShuffleId == shuffleId)
      val w = writeMetrics.get(shuffleId)
      val r = readMetrics.get(shuffleId)
      <tr>
        <td>{shuffleId}</td>
        <td>{assignment.fold("N/A")(_.celebornShuffleId.toString)}</td>
        <td>{assignment.fold("N/A")(_.numPartitions.toString)}</td>
        <td>{assignment.fold("N/A")(_.workers.asScala.size.toString)}</td>
        <td>{w.fold("N/A")(m => Utils.bytesToString(m.bytesWritten))}</td>
        <td>{w.fold("N/A")(m => pct(m.bytesWritten, writeBytes))}</td>
        <td>{w.fold("N/A")(m => mbps(m.bytesWritten, m.writeTimeMs))}</td>
        <td>{w.fold("N/A")(_.recordsWritten.toString)}</td>
        <td>{w.fold("N/A")(m => Utils.msDurationToString(m.writeTimeMs))}</td>
        <td>{r.fold("N/A")(m => Utils.bytesToString(m.bytesRead))}</td>
        <td>{r.fold("N/A")(m => mbps(m.bytesRead, m.fetchTimeMs))}</td>
      </tr>
    }
    val perShuffleTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>App Shuffle Id</th>
            <th>Celeborn Shuffle Id</th>
            <th>Partitions</th>
            <th>Workers Count</th>
            <th>Write Bytes</th>
            <th>% of Total Write</th>
            <th>Write Speed (MB/s)</th>
            <th>Records</th>
            <th>Write Time</th>
            <th>Read Bytes</th>
            <th>Read Speed (MB/s)</th>
          </tr>
        </thead>
        <tbody>
          {perShuffleRows}
        </tbody>
      </table>

    val propertiesTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      properties.info,
      fixedWidth = true)

    val assignmentRows = assignments.map { a =>
      <tr>
        <td>{a.appShuffleId}</td>
        <td>{a.celebornShuffleId}</td>
        <td>{a.workers.asScala.mkString(", ")}</td>
        <td>{a.numPartitions}</td>
        <td>{UIUtils.formatDate(new java.util.Date(a.timestamp))}</td>
      </tr>
    }
    val assignmentTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>App Shuffle Id</th>
            <th>Celeborn Shuffle Id</th>
            <th>Assigned Workers</th>
            <th>Num Partitions</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {assignmentRows}
        </tbody>
      </table>

    val throughputRows: Seq[(String, String)] = Seq(
      ("Total Shuffle Write Bytes", Utils.bytesToString(taskInfo.shuffleWriteBytes)),
      ("Total Shuffle Write Time", Utils.msDurationToString(taskInfo.shuffleWriteTimeMs)),
      ("Total Shuffle Read Bytes", Utils.bytesToString(taskInfo.shuffleReadBytes)),
      ("Total Fetch Wait Time", Utils.msDurationToString(taskInfo.shuffleFetchWaitTimeMs)),
      ("Total Task Duration", Utils.msDurationToString(taskInfo.taskDurationMs)))
    val throughputRowsXml = throughputRows.map { case (label, value) =>
      <tr>
        <td>{label}</td>
        <td>{value}</td>
      </tr>
    }
    val throughputTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Metric</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {throughputRowsXml}
        </tbody>
      </table>

    // Vertical Metric | Duration | Count layout so timing breakdowns and occurrence
    // counts stay in separate columns instead of a single mixed "Duration" row
    // (mirrors Uniffle's per-metric rows).
    val writeTimesRows = Seq(
      ("Serialize", Utils.msDurationToString(writeTimes.serializeTimeMs), "–"),
      ("Copy", Utils.msDurationToString(writeTimes.copyTimeMs), "–"),
      ("Queue Wait", Utils.msDurationToString(writeTimes.queueWaitTimeMs), "–"),
      ("Background Compress", Utils.msDurationToString(writeTimes.compressTimeMs), "–"),
      ("Background Queue Stall", Utils.msDurationToString(writeTimes.queueStallTimeMs), "–"),
      ("Background Inflight Wait", Utils.msDurationToString(writeTimes.inflightWaitTimeMs), "–"),
      ("Drain Wait", Utils.msDurationToString(writeTimes.drainWaitTimeMs), "–"),
      ("Max Push RTT", Utils.msDurationToString(writeTimes.maxPushRttMs), "–"),
      ("Slow Push", "–", writeTimes.slowPushCount.toString))
    val writeTimesTable = timesTable(writeTimesRows)

    // Sorted by push bytes (descending) so the busiest workers surface first, mirroring
    // Uniffle's sortable node list with a server-side default ordering.
    val perWorkerRows = perWorkerStats.sortBy(-_.pushBytes).map { s =>
      val totalPushRttMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.totalPushRttNanos)
      val avgPushRttMs =
        if (s.pushCount > 0) f"${totalPushRttMs.toDouble / s.pushCount}%.2f" else "N/A"
      <tr>
        <td>{s.workerId}</td>
        <td>{Utils.bytesToString(s.pushBytes)}</td>
        <td>{s.pushCount}</td>
        <td>{Utils.msDurationToString(totalPushRttMs)}</td>
        <td>{avgPushRttMs}</td>
        <td>{s.softSplitCount}</td>
        <td>{s.hardSplitCount}</td>
        <td>{s.primaryCongestedCount}</td>
        <td>{s.replicaCongestedCount}</td>
        <td>{Option(s.lastPushFailureReason).getOrElse("")}</td>
      </tr>
    }
    val perWorkerTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Worker Id</th>
            <th>Push Bytes</th>
            <th>Push Count</th>
            <th>Total Push RTT</th>
            <th>Avg Push RTT (ms)</th>
            <th>Soft Split</th>
            <th>Hard Split</th>
            <th>Primary Congested</th>
            <th>Replica Congested</th>
            <th>Last Push Failure Reason</th>
          </tr>
        </thead>
        <tbody>
          {perWorkerRows}
        </tbody>
      </table>

    // All 10 read-side fields, including retry/peer-switch/exclude counts that the
    // previous single-row layout dropped.
    val readTimesRows = Seq(
      ("Chunk Wait", Utils.msDurationToString(readTimes.chunkWaitTimeMs), "–"),
      ("Decompress", Utils.msDurationToString(readTimes.decompressTimeMs), "–"),
      ("Deserialize", Utils.msDurationToString(readTimes.deserializeTimeMs), "–"),
      ("Copy", Utils.msDurationToString(readTimes.copyTimeMs), "–"),
      ("Retry Wait", Utils.msDurationToString(readTimes.retryWaitTimeMs),
        s"retries: ${readTimes.retryCount}"),
      ("Peer Switch", "–", readTimes.peerSwitchCount.toString),
      ("Exclude", "–", readTimes.excludeCount.toString),
      ("Max Chunk RTT", Utils.msDurationToString(readTimes.maxChunkRttMs), "–"),
      ("Slow Chunk", "–", readTimes.slowChunkCount.toString))
    val readTimesTable = timesTable(readTimesRows)

    // Sorted by read bytes (descending), same default-ordering rationale as the write table.
    val perWorkerReadRows = perWorkerReadStats.sortBy(-_.bytes).map { s =>
      val totalRttMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.totalRttNanos)
      val maxRttMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.maxRttNanos)
      val avgRttMs =
        if (s.chunkCount > 0) f"${totalRttMs.toDouble / s.chunkCount}%.2f" else "N/A"
      <tr>
        <td>{s.workerId}</td>
        <td>{s.chunkCount}</td>
        <td>{Utils.bytesToString(s.bytes)}</td>
        <td>{Utils.msDurationToString(totalRttMs)}</td>
        <td>{avgRttMs}</td>
        <td>{Utils.msDurationToString(maxRttMs)}</td>
      </tr>
    }
    val perWorkerReadTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Worker Id</th>
            <th>Chunk Count</th>
            <th>Read Bytes</th>
            <th>Total RTT</th>
            <th>Avg RTT (ms)</th>
            <th>Max RTT</th>
          </tr>
        </thead>
        <tbody>
          {perWorkerReadRows}
        </tbody>
      </table>

    val content: Seq[Node] =
      <div>
        {summaryCards}
        <span>{summary}</span>
        <script type="text/javascript">{
        scala.xml.Unparsed("""
          if (typeof window.collapseTable !== 'function') {
            window.collapseTable = function(thisName, table) {
              var thisClass = '.' + thisName;
              var tableDiv = $(thisClass).parent().find('.' + table);
              $(tableDiv).toggleClass('collapsed');
              $(thisClass).find('.collapse-table-arrow')
                .toggleClass('arrow-open').toggleClass('arrow-closed');
            };
          }
        """)
      }</script>
        <a name="build-info"></a>
        {collapsible("build-info", "Celeborn Build Information", buildInfoTable)}
        <a name="properties"></a>
        {collapsible("celeborn-properties", "Celeborn Properties", propertiesTable)}
        <a name="per-shuffle"></a>
        {collapsible("per-shuffle", "Per-Shuffle Details", perShuffleTable)}
        <a name="throughput"></a>
        {collapsible("throughput", "Shuffle Throughput", throughputTable)}
        <a name="write-times"></a>
        {collapsible("write-times", "Shuffle Write Times", writeTimesTable)}
        <a name="read-times"></a>
        {collapsible("read-times", "Shuffle Read Times", readTimesTable)}
        <a name="write-servers"></a>
        {collapsible("shuffle-write-servers", "Shuffle Write Servers", perWorkerTable)}
        <a name="read-servers"></a>
        {collapsible("shuffle-read-servers", "Shuffle Read Servers", perWorkerReadTable)}
        <a name="assignments"></a>
        {collapsible("assignments", "Shuffle Assignments", assignmentTable)}
        <a name="fallback"></a>
        {collapsible("fallback", "Shuffle Fallback", fallbackTable)}
      </div>

    content
  }

  /** A summary stat tile, mirroring Uniffle's dashboard headline cards (no external CSS). */
  private def statTile(label: String, value: String): Node =
    <div class="span2">
      <h5>{label}</h5>
      <div><strong>{value}</strong></div>
    </div>

  /** A vertical Metric | Duration | Count table for the write/read timing breakdowns. */
  private def timesTable(rows: Seq[(String, String, String)]): Seq[Node] = {
    val rowXml = rows.map { case (name, duration, count) =>
      <tr>
        <td>{name}</td>
        <td>{duration}</td>
        <td>{count}</td>
      </tr>
    }
    <table class="table table-bordered table-striped table-sm">
      <thead>
        <tr>
          <th>Metric</th>
          <th>Duration</th>
          <th>Count</th>
        </tr>
      </thead>
      <tbody>
        {rowXml}
      </tbody>
    </table>
  }

  /** A collapsible section (default collapsed), mirroring Uniffle's collapse-table pattern. */
  private def collapsible(id: String, title: String, body: Seq[Node]): Seq[Node] = {
    val spanClass = s"collapse-$id collapse-table"
    val bodyClass = s"$id-table collapsible-table collapsed"
    <div>
      <span class={spanClass} onClick={s"collapseTable('collapse-$id', '$id-table')"}>
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>{title}</a>
        </h4>
      </span>
      <div class={bodyClass}>
        {body}
      </div>
    </div>
  }

  private def propertyHeader: Seq[String] = Seq("Property", "Value")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <tr>
      <td>{kv._1}</td>
      <td>{kv._2}</td>
    </tr>
}
