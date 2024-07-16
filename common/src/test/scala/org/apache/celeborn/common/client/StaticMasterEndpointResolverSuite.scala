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

package org.apache.celeborn.common.client

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.util.Utils

class StaticMasterEndpointResolverSuite extends CelebornFunSuite {

  test("resolve with default configs") {
    val conf = new CelebornConf()
    val resolver = new StaticMasterEndpointResolver(conf, false)

    assert(resolver.getMasterEndpointName == RpcNameConstants.MASTER_EP)
    assert(!resolver.isUpdated)
    assert(resolver.getActiveMasterEndpoints.size == 1)
    assert(resolver.getActiveMasterEndpoints.get(0) == s"${Utils.localHostName(conf)}:9097")
  }

  test("resolve with internal port enabled and isWorker = false") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, "clb-1:1234,clb-2,<localhost>:9097")
    conf.set(
      CelebornConf.MASTER_INTERNAL_ENDPOINTS.key,
      "clb-internal-1:1234,clb-internal-2,<localhost>:9097")

    val resolver = new StaticMasterEndpointResolver(conf, false)

    assert(resolver.getMasterEndpointName == RpcNameConstants.MASTER_EP)
    assert(!resolver.isUpdated)
    assert(resolver.getActiveMasterEndpoints.size == 3)
    assert(resolver.getActiveMasterEndpoints.contains("clb-1:1234"))
    assert(resolver.getActiveMasterEndpoints.contains(
      s"clb-2:${CelebornConf.HA_MASTER_NODE_PORT.defaultValue.get}"))
    assert(resolver.getActiveMasterEndpoints.contains(s"${Utils.localHostName(conf)}:9097"))
  }

  test("resolve with internal port enabled and isWorker = true") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, "clb-1:1234,clb-2,<localhost>:9097")
    conf.set(
      CelebornConf.MASTER_INTERNAL_ENDPOINTS.key,
      "clb-internal-1:1234,clb-internal-2,<localhost>:8097")

    val resolver = new StaticMasterEndpointResolver(conf, true)

    assert(resolver.getMasterEndpointName == RpcNameConstants.MASTER_INTERNAL_EP)
    assert(!resolver.isUpdated)
    assert(resolver.getActiveMasterEndpoints.size == 3)
    assert(resolver.getActiveMasterEndpoints.contains("clb-internal-1:1234"))
    assert(resolver.getActiveMasterEndpoints.contains(
      s"clb-internal-2:${CelebornConf.HA_MASTER_NODE_INTERNAL_PORT.defaultValue.get}"))
    assert(resolver.getActiveMasterEndpoints.contains(s"${Utils.localHostName(conf)}:8097"))
  }
}
