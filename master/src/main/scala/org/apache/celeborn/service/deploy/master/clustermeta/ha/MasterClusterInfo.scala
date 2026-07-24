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

import java.net.{InetAddress, NetworkInterface}
import java.util

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.TransportModuleConstants

case class MasterClusterInfo(
    localNode: MasterNode,
    peerNodes: util.List[MasterNode])

object MasterClusterInfo extends Logging {

  @throws[IllegalArgumentException]
  def loadHAConfig(conf: CelebornConf): MasterClusterInfo = {
    val localNodeIdOpt = conf.haMasterNodeId
    val clusterNodeIds = conf.haMasterNodeIds
    // Inter-master Ratis TLS is decoupled from the client-facing rpc_service cert:
    // it is enabled if either the dedicated `ratis` module or `rpc_service` has SSL on.
    // This preserves backward compatibility - deployments that only configure
    // rpc_service SSL continue to secure Ratis exactly as before.
    val sslEnabled = ratisSslEnabled(conf)

    val masterNodes = clusterNodeIds.map { nodeId =>
      val ratisHost = conf.haMasterRatisHost(nodeId)
      val ratisPort = conf.haMasterRatisPort(nodeId)
      val rpcHost = conf.haMasterNodeHost(nodeId)
      val rpcPort = conf.haMasterNodePort(nodeId)
      val internalPort =
        if (conf.internalPortEnabled) conf.haMasterNodeInternalPort(nodeId) else rpcPort
      MasterNode(nodeId, ratisHost, ratisPort, rpcHost, rpcPort, internalPort, sslEnabled)
    }

    val (localNodes, peerNodes) = localNodeIdOpt match {
      case Some(localNodeId) =>
        masterNodes.partition { localNodeId == _.nodeId }
      case None =>
        masterNodes.partition { node =>
          !node.isRatisHostUnresolved && isLocalAddress(node.ratisIpAddr)
        }
    }

    if (localNodes.isEmpty)
      throw new IllegalArgumentException("Can not found local node")

    if (localNodes.length > 1) {
      val nodesAddr = localNodes.map(_.ratisEndpoint).mkString(",")
      throw new IllegalArgumentException(
        s"Detecting multi Ratis instances[$nodesAddr] in single node, please specific ${HA_MASTER_NODE_ID.key}.")
    }

    MasterClusterInfo(localNodes.head, peerNodes.toList.asJava)
  }

  /**
   * Whether TLS should be enabled for the inter-master Ratis (Raft consensus) gRPC channel.
   *
   * Ratis TLS is decoupled from the client-facing `rpc_service` SSL module so operators can
   * give Ratis a dedicated certificate/keystore (e.g. one carrying internal pod-FQDN SANs).
   * To stay fully backward compatible, it is considered enabled when EITHER the dedicated
   * `ratis` module OR the legacy `rpc_service` module has SSL enabled. Deployments that only
   * configure `rpc_service` SSL therefore behave exactly as before.
   */
  def ratisSslEnabled(conf: CelebornConf): Boolean = {
    conf.sslEnabled(TransportModuleConstants.RATIS_MODULE) ||
    conf.sslEnabled(TransportModuleConstants.RPC_SERVICE_MODULE)
  }

  /**
   * The transport SSL module whose `celeborn.ssl.<module>.*` config should be used to build the
   * Ratis SSLFactory. Prefer the dedicated `ratis` module when it is explicitly enabled; otherwise
   * fall back to `rpc_service` so existing (rpc_service-only) deployments are byte-for-byte
   * unchanged and continue to use the rpc_service cert for Ratis.
   */
  def ratisSslModule(conf: CelebornConf): String = {
    if (conf.sslEnabled(TransportModuleConstants.RATIS_MODULE)) {
      TransportModuleConstants.RATIS_MODULE
    } else {
      TransportModuleConstants.RPC_SERVICE_MODULE
    }
  }

  private def isLocalAddress(addr: InetAddress): Boolean = {
    if (addr.isAnyLocalAddress || addr.isLoopbackAddress) {
      return true
    }
    Try(NetworkInterface.getByInetAddress(addr)) match {
      case Success(value) => value != null
      case Failure(_) => false
    }
  }
}
