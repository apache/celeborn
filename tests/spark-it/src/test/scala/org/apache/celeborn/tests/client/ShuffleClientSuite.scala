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

package org.apache.celeborn.tests.client

import org.apache.celeborn.client.WithShuffleClientSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ShuffleClientSuite extends WithShuffleClientSuite with MiniClusterFeature {
  private val masterPort = 19097
  override def beforeAll(): Unit = {
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> masterPort.toString)
    val workerConf = Map(
      "celeborn.master.endpoints" -> s"localhost:$masterPort")
    setUpMiniCluster(masterConf, workerConf)
  }

  override def afterAll(): Unit = {
    // TODO refactor MiniCluster later
    println("test done")
    sys.exit(0)
  }

  override protected def celebornConf: CelebornConf = new CelebornConf()
    .set("celeborn.master.endpoints", s"localhost:$masterPort")
    .set("celeborn.push.replicate.enabled", "true")
    .set("celeborn.push.buffer.size", "256K")
}
