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

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class MasterNodeSuite extends CelebornFunSuite {

  private def haConf(): CelebornConf = {
    new CelebornConf()
      .set("celeborn.master.ha.enabled", "true")
      .set("celeborn.master.ha.node.id", "1")
      .set("celeborn.master.ha.node.1.host", "clb-1")
      .set("celeborn.master.ha.node.1.port", "9097")
      .set("celeborn.master.ha.node.2.host", "clb-2")
      .set("celeborn.master.ha.node.2.port", "9097")
  }

  test("client endpoint falls back to the rpc host when client host is not set") {
    val info = MasterClusterInfo.loadHAConfig(haConf())
    val nodes = info.localNode +: info.peerNodes.asScala
    nodes.foreach { node =>
      assert(node.clientEndpoint === node.rpcEndpoint)
    }
  }

  test("client endpoint uses the client host while other endpoints keep the node host") {
    val conf = haConf().set("celeborn.master.ha.node.1.client.host", "clb-1.external")
    val info = MasterClusterInfo.loadHAConfig(conf)
    val local = info.localNode
    assert(local.clientEndpoint === "clb-1.external:9097")
    assert(local.rpcEndpoint === "clb-1:9097")
    assert(local.internalRpcEndpoint.startsWith("clb-1:"))
    assert(local.ratisHost === "clb-1")
    // Peers without a client host keep advertising their rpc endpoint.
    assert(info.peerNodes.asScala.head.clientEndpoint === "clb-2:9097")
  }

  test("builder seeds client host from setHost and allows overriding it") {
    val node = new MasterNode.Builder().setNodeId("1").setHost("clb-1").setRpcPort(9097).build
    assert(node.clientEndpoint === "clb-1:9097")
    val overridden = new MasterNode.Builder()
      .setNodeId("1")
      .setHost("clb-1")
      .setClientHost("clb-1.external")
      .setRpcPort(9097)
      .build
    assert(overridden.clientEndpoint === "clb-1.external:9097")
    assert(overridden.rpcEndpoint === "clb-1:9097")
  }
}
