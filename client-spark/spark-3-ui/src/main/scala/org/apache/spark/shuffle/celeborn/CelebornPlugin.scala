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

import java.util
import java.util.{Collections, LinkedHashMap => JLinkedHashMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.events.CelebornBuildInfoEvent
import org.apache.spark.shuffle.celeborn.ui.CelebornUITab
import org.apache.spark.shuffle.celeborn.ui.CelebornUIUtils

/**
 * SparkPlugin entry point for the Celeborn UI extension.
 *
 * Configure via:
 * {{{
 *   spark.plugins=org.apache.spark.shuffle.celeborn.CelebornPlugin
 * }}}
 *
 * [[init]] registers the Celeborn listener to the status queue and [[registerMetrics]] posts a
 * build-info event and attaches the UI tab. These split so the tab's page servlets are added
 * once the Jetty server is fully started: `registerMetrics` runs AFTER
 * SparkContext.attachAllHandlers, while `init` runs before (attaching during init would race
 * attachAllHandlers and throw IllegalStateException: STARTED on some Spark builds).
 */
class CelebornPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = new CelebornDriverPlugin()

  override def executorPlugin(): org.apache.spark.api.plugin.ExecutorPlugin = null
}

private class CelebornDriverPlugin extends DriverPlugin with Logging {

  private var sc: SparkContext = _

  override def init(
      sc: SparkContext,
      ctx: PluginContext): util.Map[String, String] = {
    logInfo("Initializing CelebornDriverPlugin...")
    this.sc = sc
    if (CelebornUIUtils.isUIEnabled(sc.conf)) {
      CelebornListener.register(sc)
    } else {
      logInfo("Celeborn Spark UI extension is disabled, skipping.")
    }
    Collections.emptyMap[String, String]()
  }

  override def registerMetrics(
      appId: String,
      ctx: PluginContext): Unit = {
    if (sc == null || !CelebornUIUtils.isUIEnabled(sc.conf)) {
      return
    }
    // registerMetrics runs after SparkContext is fully initialized, so applicationId is
    // available here (it's null during init). Post the build-info event now and attach UI.
    sc.listenerBus.post(CelebornBuildInfoEvent(buildInfoMap(sc, appId)))
    CelebornUIUtils.attachUI(sc)
  }

  override def shutdown(): Unit = {}

  private def buildInfoMap(sc: SparkContext, appId: String): Map[String, String] = {
    val info = new JLinkedHashMap[String, String]()
    info.put("Spark Version", sc.version)
    // Prefer the appId passed to registerMetrics (reliable); fall back to sc.applicationId.
    info.put("Application Id", Option(appId).filter(_.nonEmpty).getOrElse(sc.applicationId))
    info.put("Celeborn UI Enabled", "true")
    // Derive key Celeborn runtime config from CelebornConf (rebuild from sc.conf via SparkUtils).
    // Celeborn has no ProjectConstants version class, so runtime config stands in for version —
    // it is more useful for diagnosing shuffle behavior anyway.
    try {
      val celebornConf = SparkUtils.fromSparkConf(sc.conf)
      info.put("Celeborn Compression Codec", celebornConf.shuffleCompressionCodec.toString)
      info.put("Celeborn Shuffle Writer Mode", celebornConf.shuffleWriterMode.toString)
      info.put("Celeborn Push Replicate Enabled", celebornConf.clientPushReplicateEnabled.toString)
      info.put("Celeborn Partition Split Mode", celebornConf.shufflePartitionSplitMode.toString)
      info.put(
        "Celeborn Partition Split Threshold",
        org.apache.spark.util.Utils.bytesToString(celebornConf.shufflePartitionSplitThreshold))
      info.put("Celeborn Fallback Policy", celebornConf.sparkShuffleFallbackPolicy.toString)
    } catch {
      case e: Throwable =>
        logWarning("Failed to derive Celeborn conf for build info", e)
    }
    info.asScala.toMap
  }
}
