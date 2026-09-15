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

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.ui.CelebornUITab

/**
 * SparkPlugin entry point for the Celeborn UI extension.
 *
 * Configure via:
 * {{{
 *   spark.plugins=org.apache.spark.shuffle.celeborn.CelebornPlugin
 * }}}
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
    val kvStore = sc.statusStore.store
    new CelebornListener(kvStore, sc.conf).register(sc)
    java.util.Collections.emptyMap[String, String]()
  }

  override def registerMetrics(
      appId: String,
      ctx: PluginContext): Unit = {
    sc.ui.foreach { ui =>
      new CelebornUITab(new CelebornStatusStore(ui.store.store), ui)
    }
  }

  override def shutdown(): Unit = {}
}
