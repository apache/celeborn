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

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(classOf[JUnit4])
class CelebornPluginSuite {

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
}
