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

package org.apache.celeborn.service.deploy.master

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.StatusCode

class MasterSourceSuite extends CelebornFunSuite {

  test("test request slots failed metrics") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "status=prod")
    val source = new MasterSource(conf)

    source.incRequestSlotsFailed(StatusCode.SLOT_NOT_AVAILABLE)
    source.incRequestSlotsFailed(StatusCode.WORKER_EXCLUDED)
    source.incRequestSlotsFailed(StatusCode.SUCCESS)

    val metrics = source.getMetrics
    val instance = source.instanceLabel("instance")
    assert(metrics.contains(
      s"""metrics_RequestSlotsFailed_Count{instance="$instance",role="Master",status="SLOT_NOT_AVAILABLE"} 1"""))
    assert(metrics.contains(
      s"""metrics_RequestSlotsFailed_Count{instance="$instance",role="Master",status="WORKER_EXCLUDED"} 1"""))
    assert(!metrics.contains("""status="SUCCESS""""))
  }
}
