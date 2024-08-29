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

import scala.util.Random

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{HA_MASTER_NODE_INTERNAL_PORT, HA_MASTER_NODE_PORT}
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.util.Utils

class StaticMasterEndpointResolver(conf: CelebornConf, isWorker: Boolean)
  extends MasterEndpointResolver(conf, isWorker) {

  override def resolve(endpoints: Array[String]): Unit = {
    val haMasterPort =
      if (masterEndpointName == RpcNameConstants.MASTER_INTERNAL_EP) {
        HA_MASTER_NODE_INTERNAL_PORT.defaultValue.get
      } else {
        HA_MASTER_NODE_PORT.defaultValue.get
      }

    this.activeMasterEndpoints = Some(endpoints.map { endpoint =>
      Utils.parseHostPort(endpoint.replace("<localhost>", Utils.localHostName(conf))) match {
        case (host, 0) => s"$host:$haMasterPort"
        case (host, port) => s"$host:$port"
      }
    }.toList)

    Random.shuffle(this.activeMasterEndpoints.get)
    logInfo(s"masterEndpoints = ${activeMasterEndpoints.get}")
  }

  override def update(endpoints: Array[String]): Unit = {}
}
