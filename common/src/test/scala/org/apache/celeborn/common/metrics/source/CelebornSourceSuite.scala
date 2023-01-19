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

class CelebornSourceSuite extends CelebornFunSuite {

  test("test getMetrics with customized label") {
    val mockSource = new AbstractSource(new CelebornConf(), "mock") {
      override def sourceName: String = "mockSource"
    }
    mockSource.addGauge("Gauge1", _ => 1000)
    mockSource.addGauge("Gauge2", _ => 2000, Map("user" -> "user1"))
    mockSource.addCounter("Counter1")
    mockSource.incCounter("Counter1", 3000)
    mockSource.addCounter("Counter2", Map("user" -> "user2"))
    mockSource.incCounter("Counter2", 4000)
    mockSource.addTimer("Timer1")
    mockSource.addTimer("Timer2", Map("user" -> "user3"))

    val res = mockSource.getMetrics()
    val exp1 = "metrics_Gauge1_Value{role=\"mock\"} 1000"
    val exp2 = "metrics_Gauge2_Value{user=\"user1\" role=\"mock\"} 2000"
    val exp3 = "metrics_Counter1_Count{role=\"mock\"} 3000"
    val exp4 = "metrics_Counter2_Count{user=\"user2\" role=\"mock\"} 4000"
    val exp5 = "metrics_Timer1_Count{role=\"mock\"} 0"
    val exp6 = "metrics_Timer2_Count{user=\"user3\" role=\"mock\"} 0"

    assert(res.contains(exp1))
    assert(res.contains(exp2))
    assert(res.contains(exp3))
    assert(res.contains(exp4))
    assert(res.contains(exp5))
    assert(res.contains(exp6))
  }
}
