package org.apache.celeborn.common.client

import scala.util.Random

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{HA_MASTER_NODE_INTERNAL_PORT, HA_MASTER_NODE_PORT}
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.util.Utils

class StaticMasterEndpointResolver(conf: CelebornConf, isWorker: Boolean)
  extends MasterEndpointResolver(conf, isWorker) {
  override protected def resolve(endpoints: Array[String]): Unit = {
    val haMasterPort =
      if (getMasterEndpointName == RpcNameConstants.MASTER_INTERNAL_EP) {
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
    this.currentIndex = 0

    Random.shuffle(this.activeMasterEndpoints.get)
    logInfo(s"masterEndpoints = ${activeMasterEndpoints.get}")
  }
  override protected def update(endpoints: Array[String]): Unit = {}
}
