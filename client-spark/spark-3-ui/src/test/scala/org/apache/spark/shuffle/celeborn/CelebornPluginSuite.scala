/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.spark.shuffle.celeborn

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.scheduler.{JobSucceeded, ReplayListenerBus, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobEnd}
import org.apache.spark.shuffle.celeborn.ui.CelebornUITab
import org.apache.spark.shuffle.celeborn.ui.SparkServletBridge.HttpServletRequest
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.ui.SparkUI
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.protocol.message.StatusCode

@RunWith(classOf[JUnit4])
class CelebornPluginSuite {

  // Render without binding an HTTP server: Spark 3 and 4 use different Jersey versions.
  private def createUI(sc: SparkContext): SparkUI = SparkUI.create(
    Some(sc),
    sc.statusStore,
    sc.conf,
    new SecurityManager(sc.conf),
    sc.appName,
    "",
    System.currentTimeMillis())

  private def page(ui: SparkUI): String = {
    val request = Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[HttpServletRequest]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "getParameterMap" => java.util.Collections.emptyMap[String, Array[String]]()
            case _ => null
          }
      }).asInstanceOf[HttpServletRequest]
    ui.getTabs.find(_.prefix == "celeborn").get.pages.head.render(request).mkString
  }

  private def reviveSummary(html: String): String =
    """(?s)<strong>Revived Partitions: </strong>\s*([^<]+)""".r
      .findFirstMatchIn(html).get.group(1).trim

  private def hasWorkerRevives(html: String): Boolean = html.contains("<a>Worker Revives</a>")

  @Test
  def historyServerDistinguishesUnavailableRevivesFromZero(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CelebornPluginSuite")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    try {
      val historyPlugin = new CelebornHistoryServerPlugin()
      val listener = historyPlugin.createListeners(
        conf,
        sc.statusStore.store.asInstanceOf[ElementTrackingStore]).head
        .asInstanceOf[CelebornListener]
      val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
      val replayBus = new ReplayListenerBus()
      replayBus.addListener(listener)
      def replay(event: SparkListenerEvent): Unit = {
        assertTrue(replayBus.replay(
          Iterator(mapper.writeValueAsString(event)),
          "revive-events",
          false,
          _ => true))
        listener.flush()
      }

      listener.onEnvironmentUpdate(SparkListenerEnvironmentUpdate(Map(
        "Spark Properties" -> Seq("spark.plugins" -> classOf[CelebornPlugin].getName))))
      replay(CelebornShuffleAssignmentEvent(3, 1, Map("w1" -> 2)))
      val ui = createUI(sc)
      new CelebornHistoryServerPlugin().setupUI(ui)

      def assertRevives(summary: String, tableShown: Boolean): Unit = {
        val html = page(ui)
        assertEquals(summary, reviveSummary(html))
        assertEquals(tableShown, hasWorkerRevives(html))
      }

      // Older clients reported assignments, but did not record revive metrics.
      assertRevives("N/A", tableShown = false)

      replay(CelebornPartitionReviveReportingEvent())
      assertRevives("Soft Split: 0 | Hard Split: 0 | Push Failure: 0", tableShown = false)

      replay(CelebornPartitionReviveEvent(3, 1, Map("w1" -> Map("HARD_SPLIT" -> 2))))
      // Replaying a marker again must not reset the recorded counts.
      replay(CelebornPartitionReviveReportingEvent())
      assertRevives("Soft Split: 0 | Hard Split: 2 | Push Failure: 0", tableShown = true)
    } finally {
      sc.stop()
    }
  }

  @Test
  def shuffleAssignmentsReportedThroughSparkShuffleManager(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CelebornPluginSuite")
      .set("spark.plugins", classOf[CelebornPlugin].getName)
      .set("spark.shuffle.manager", classOf[SparkShuffleManager].getName)
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    try {
      val listener =
        SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager].getShuffleAssignmentListener
      listener.onShuffleAssigned(3, 1, Map[String, Integer]("w1" -> 2, "w2" -> 4).asJava)
      sc.listenerBus.waitUntilEmpty()

      assertEquals(
        Seq(ShuffleAssignmentUIData(3, 1, 2, 6, 2, 4)),
        new CelebornStatusStore(sc.statusStore.store).shuffleAssignments())
    } finally {
      sc.stop()
    }
  }

  @Test
  def partitionRevivesReportedThroughSparkShuffleManager(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CelebornPluginSuite")
      .set("spark.plugins", classOf[CelebornPlugin].getName)
      .set("spark.shuffle.manager", classOf[SparkShuffleManager].getName)
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    try {
      sc.listenerBus.waitUntilEmpty()
      assertEquals(
        Some(WorkerRevivesUIData(Map.empty)),
        new CelebornStatusStore(sc.statusStore.store).workerRevives())
      val ui = createUI(sc)
      new CelebornUITab(new CelebornStatusStore(sc.statusStore.store), ui)
      val emptyHtml = page(ui)
      assertEquals("Soft Split: 0 | Hard Split: 0 | Push Failure: 0", reviveSummary(emptyHtml))
      assertFalse(hasWorkerRevives(emptyHtml))

      val listener =
        SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager].getPartitionReviveListener
      val causes = Map[StatusCode, Integer](StatusCode.HARD_SPLIT -> 2).asJava
      listener.onPartitionsRevived(3, 1, Map("w1" -> causes).asJava)
      sc.listenerBus.post(SparkListenerJobEnd(0, System.currentTimeMillis(), JobSucceeded))
      sc.listenerBus.waitUntilEmpty()

      assertEquals(
        Some(WorkerRevivesUIData(Map("w1" -> Map("HARD_SPLIT" -> 2)))),
        new CelebornStatusStore(sc.statusStore.store).workerRevives())
      val html = page(ui)
      assertEquals("Soft Split: 0 | Hard Split: 2 | Push Failure: 0", reviveSummary(html))
      assertTrue(hasWorkerRevives(html))
      assertTrue(html.contains("Hard Split (Primary)"))
      val note = html.replaceAll("\\s+", " ")
      assertTrue(note.contains("Counts are revived partitions, not individual pushes"))
      assertTrue(note.contains(
        "Splits are counted against the primary worker, even when its replica triggered the split."))
    } finally {
      sc.stop()
    }
  }

  @Test
  def unsupportedShuffleManagerDoesNotReportRevives(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CelebornPluginSuite")
      .set("spark.plugins", classOf[CelebornPlugin].getName)
      .set("spark.shuffle.manager", "sort")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    try {
      sc.listenerBus.waitUntilEmpty()
      assertEquals(None, new CelebornStatusStore(sc.statusStore.store).workerRevives())
    } finally {
      sc.stop()
    }
  }
}
