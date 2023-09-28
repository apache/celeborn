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

package org.apache.celeborn.common.metrics.source

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class JVMSourceSuite extends CelebornFunSuite {

  val gcNames = Seq("G1-Young-Generation", "G1-Old-Generation").toBuffer
  val poolNames = Seq("G1-Eden-Space", "G1-Survivor-Space", "G1-Old-Gen").toBuffer

  val JVM_METRIC_PREFIX = "jvm"
  val JVM_MEMORY_PREFIX = "jvm.memory"

  test("Test handleJVMMetricName") {

    val jvmSource = new JVMSource(new CelebornConf(), "test")

    val gcMetric1 = "G1-Old-Generation.time"
    val gcMetric2 = "G1-Young-Generation.count"
    val gcResult1 = jvmSource.handleJVMMetricName(gcMetric1, gcNames, JVM_METRIC_PREFIX, "gc")
    val gcResult2 = jvmSource.handleJVMMetricName(gcMetric2, gcNames, JVM_METRIC_PREFIX, "gc")
    assert(gcResult1._1 == "jvm.gc.time")
    assert(gcResult1._2 == Map("name" -> "G1-Old-Generation"))
    assert(gcResult2._1 == "jvm.gc.count")
    assert(gcResult2._2 == Map("name" -> "G1-Young-Generation"))

    val memoryMetric1 = "total.init"
    val memoryMetrics = "pools.G1-Eden-Space.init"
    val memoryResult1 =
      jvmSource.handleJVMMetricName(memoryMetric1, poolNames, JVM_MEMORY_PREFIX, "")
    val memoryResult2 =
      jvmSource.handleJVMMetricName(memoryMetrics, poolNames, JVM_MEMORY_PREFIX, "")
    assert(memoryResult1._1 == "jvm.memory.total.init")
    assert(memoryResult1._2 == Map.empty[String, String])
    assert(memoryResult2._1 == "jvm.memory.pools.init")
    assert(memoryResult2._2 == Map("name" -> "G1-Eden-Space"))
  }
}
