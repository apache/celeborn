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

import java.util

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.RpcNameConstants

class DummyMasterEndpointResolver(conf: CelebornConf, isWorker: Boolean)
  extends MasterEndpointResolver(conf, isWorker) {

  override def resolve(endpoints: Array[String]): Unit = {
    this.activeMasterEndpoints = Some(endpoints.toList)
  }

  override def update(endpoints: Array[String]): Unit = {
    this.activeMasterEndpoints = Some(endpoints.toList)
    updated.set(true)
  }

  def updateTest(endpoints: Array[String]): Unit = {
    update(endpoints)
  }
}

class MasterEndpointResolverSuite extends CelebornFunSuite {

  test("resolve") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, "clb-1:1234")
    val resolver = new DummyMasterEndpointResolver(conf, false)

    assert(resolver.masterEndpointName == RpcNameConstants.MASTER_EP)
    assert(!resolver.getUpdatedAndReset())
    // isUpdated should be not be reset if isUpdated returns false
    assert(!resolver.getUpdatedAndReset())
    assert(resolver.getActiveMasterEndpoints.size == 1)
    assert(resolver.getActiveMasterEndpoints.get(0) == "clb-1:1234")

    resolver.updateTest(Array("clb-2:1234"))
    assert(resolver.getUpdatedAndReset())
    // isUpdated should be reset after calling isUpdated once
    assert(!resolver.getUpdatedAndReset())
    assert(resolver.getActiveMasterEndpoints.size == 1)
    assert(resolver.getActiveMasterEndpoints.get(0) == "clb-2:1234")
  }

  test("resolve with internal port enabled and isWorker = false") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, "clb-1:1234")
    conf.set(CelebornConf.MASTER_INTERNAL_ENDPOINTS.key, "clb-internal-1:1234")
    conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")

    val resolver = new DummyMasterEndpointResolver(conf, false)

    assert(resolver.masterEndpointName == RpcNameConstants.MASTER_EP)
    assert(resolver.getActiveMasterEndpoints.size == 1)
    assert(resolver.getActiveMasterEndpoints.get(0) == "clb-1:1234")
  }

  test("resolve with internal port enabled and isWorker = true") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, "clb-1:1234")
    conf.set(CelebornConf.MASTER_INTERNAL_ENDPOINTS.key, "clb-internal-1:1234")

    val resolver = new DummyMasterEndpointResolver(conf, true)

    assert(resolver.masterEndpointName == RpcNameConstants.MASTER_INTERNAL_EP)
    assert(resolver.getActiveMasterEndpoints.size == 1)
    assert(resolver.getActiveMasterEndpoints.get(0) == "clb-internal-1:1234")
  }
}
