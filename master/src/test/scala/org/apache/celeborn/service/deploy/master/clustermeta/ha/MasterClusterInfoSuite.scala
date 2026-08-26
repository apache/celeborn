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
import org.apache.celeborn.common.protocol.TransportModuleConstants

class MasterClusterInfoSuite extends CelebornFunSuite {

  private def sslEnabledKey(module: String): String = s"celeborn.ssl.$module.enabled"

  private val ratisKey = sslEnabledKey(TransportModuleConstants.RATIS_MODULE)
  private val rpcServiceKey = sslEnabledKey(TransportModuleConstants.RPC_SERVICE_MODULE)

  test("ratis ssl is disabled when neither ratis nor rpc_service module is enabled") {
    assert(!MasterClusterInfo.ratisSslEnabled(new CelebornConf()))
  }

  test("ratis ssl stays enabled when only rpc_service is enabled") {
    // Legacy deployments configure only rpc_service; they must keep securing Ratis as before.
    val conf = new CelebornConf()
    conf.set(rpcServiceKey, "true")
    assert(MasterClusterInfo.ratisSslEnabled(conf))
  }

  test("ratis ssl is enabled by the dedicated ratis module alone") {
    val conf = new CelebornConf()
    conf.set(ratisKey, "true")
    assert(MasterClusterInfo.ratisSslEnabled(conf))
  }

  test("ratis ssl is enabled by the ratis module even when rpc_service is explicitly disabled") {
    val conf = new CelebornConf()
    conf.set(ratisKey, "true")
    conf.set(rpcServiceKey, "false")
    assert(MasterClusterInfo.ratisSslEnabled(conf))
  }
}
