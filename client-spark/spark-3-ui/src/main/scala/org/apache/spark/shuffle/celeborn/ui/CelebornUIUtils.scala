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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.CelebornStatusStore

import org.apache.celeborn.common.CelebornConf

/** Helpers for the Celeborn UI extension: enablement check and tab attachment. */
private[celeborn] object CelebornUIUtils extends Logging {

  /**
   * Build a [[CelebornConf]] from the `spark.celeborn.*` entries of a SparkConf.
   * SparkUtils.fromSparkConf does the same but its class initialization drags in
   * spark-sql's UnsafeRowSerializer (via the AQE dataSize DynField), which is not on
   * this module's classpath (spark-core only) and absent in RDD-only applications.
   */
  private[celeborn] def celebornConf(conf: SparkConf): CelebornConf = {
    val celebornConf = new CelebornConf()
    conf.getAll.foreach { case (key, value) =>
      if (key.startsWith("spark.celeborn.")) {
        celebornConf.set(key.substring("spark.".length), value)
      }
    }
    celebornConf
  }

  /**
   * The UI extension is on when both celeborn's flag and Spark's own UI flag are on.
   *  The celeborn flag is read through the CelebornConf entry so the documented
   *  `spark.celeborn.client.spark.ui.enabled` key (and the entry's default handling)
   *  applies; a bare-key SparkConf lookup never matches the user-set key because
   *  SparkConf does exact matching.
   */
  def isUIEnabled(conf: SparkConf): Boolean = {
    celebornConf(conf).clientSparkUIEnabled &&
    conf.getBoolean("spark.ui.enabled", true)
  }

  /** Attach the Celeborn tab to the live Spark WebUI. */
  def attachUI(sc: SparkContext): Unit = {
    val store = new CelebornStatusStore(sc.statusStore.store)
    sc.ui.foreach(ui => new CelebornUITab(store, ui))
  }
}
