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

package org.apache.celeborn.common.rpc

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.Role

class RpcMetricsTrackerSuite extends CelebornFunSuite {

  test("close removes the metrics registered with the name label") {
    val conf = new CelebornConf()
    val rpcSource = new RpcSource(conf, Role.WORKER)
    val tracker = new RpcMetricsTracker("endpoint", rpcSource, conf)
    tracker.init(() => 1)

    val instance = rpcSource.instanceLabel("instance")
    val labels = s"""instance="$instance",name="endpoint",role="Worker""""
    val metrics = rpcSource.getMetrics
    assert(metrics.contains(s"""metrics_RpcQueueLength_Value{$labels} 1"""))
    assert(metrics.contains(s"""metrics_RpcQueueTime_Count{$labels}"""))
    assert(metrics.contains(s"""metrics_RpcProcessTime_Count{$labels}"""))
    assert(rpcSource.getAllMetricsNum == 3)

    tracker.close()

    assert(rpcSource.getAllMetricsNum == 0)
    val metricsAfterClose = rpcSource.getMetrics
    assert(!metricsAfterClose.contains("metrics_RpcQueueLength_Value"))
    assert(!metricsAfterClose.contains("metrics_RpcQueueTime_Count"))
    assert(!metricsAfterClose.contains("metrics_RpcProcessTime_Count"))
  }
}
