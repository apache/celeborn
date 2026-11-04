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

package org.apache.celeborn.service.deploy.master.clustermeta.ha

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class HARaftServerLeaderEndpointSuite extends CelebornFunSuite {

  private val prefix = "celeborn.master.ha.node"

  test("leader with an explicit client host is suggested from configuration, unresolved") {
    val conf = new CelebornConf()
      .set(s"$prefix.1.host", "clb-1")
      .set(s"$prefix.1.port", "9097")
      .set(s"$prefix.1.client.host", "clb-1.gateway.invalid")
    // The Raft peer metadata may still carry the client address the group started with.
    val pair = HARaftServer.leaderClientEndpointPair(conf, "1", "127.0.0.1:9097")
    assert(pair._1 === "clb-1.gateway.invalid:9097")
    assert(pair._2 === "clb-1.gateway.invalid:9097")
  }

  test("leader without a client host keeps the resolved (ip, host) pair") {
    val conf = new CelebornConf().set(s"$prefix.1.host", "localhost")
    val pair = HARaftServer.leaderClientEndpointPair(conf, "1", "localhost:9097")
    assert(pair._1 === "127.0.0.1:9097")
    assert(pair._2 === "localhost:9097")
  }

  test("internal endpoint uses the admin address when present") {
    val pair = HARaftServer.leaderInternalEndpointPair("localhost:8097", "localhost:9097")
    assert(pair._1 === "127.0.0.1:8097")
    assert(pair._2 === "localhost:8097")
  }

  test("internal endpoint falls back to the client address when the admin address is absent") {
    val pair = HARaftServer.leaderInternalEndpointPair("", "localhost:9097")
    assert(pair._1 === "127.0.0.1:9097")
    assert(pair._2 === "localhost:9097")
  }
}
