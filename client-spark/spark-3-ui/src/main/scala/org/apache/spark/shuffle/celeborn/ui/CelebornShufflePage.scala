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

    val writeBytes = taskInfo.shuffleWriteBytes
    val readBytes = taskInfo.shuffleReadBytes
    val writeMs = taskInfo.shuffleWriteTimeMs
    val readMs = taskInfo.shuffleFetchWaitTimeMs
    val durationMs = taskInfo.taskDurationMs

    def mbps(bytes: Long, ms: Long): String =
      if (ms <= 0) "N/A" else f"${bytes.toDouble / 1000.0 / 1000.0 / (ms.toDouble / 1000.0)}%.2f"
    def pct(part: Long, total: Long): String =
      if (total <= 0) "N/A" else f"${part.toDouble * 100.0 / total.toDouble}%.1f%%"

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
            <strong>Shuffle Duration (write+read) / Task Duration: </strong>
            {
        s"${pct(writeMs + readMs, durationMs)} (Write ${pct(
          writeMs,
          durationMs)}, Read ${pct(readMs, durationMs)})"
      }
          </li>
        </ul>
      </div>

    val propertiesTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      properties.info,
      fixedWidth = true,
      headerClasses = headerClasses)

    val content =
      <span>
        {summary}
        <span class="collapse-aggregated-celebornProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-celebornProperties',
            'aggregated-celebornProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Celeborn Properties</a>
          </h4>
        </span>
        <div class="aggregated-celebornProperties collapsible-table">
          {propertiesTable}
        </div>
      </span>

    UIUtils.headerSparkPage(request, "Celeborn Shuffle", content, parent)
  }

  private def propertyHeader: Seq[String] = Seq("Name", "Value")
  private def headerClasses: Seq[String] = Seq("sorttable_alpha", "sorttable_alpha")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <tr>
      <td>{kv._1}</td>
      <td>{kv._2}</td>
    </tr>
}
