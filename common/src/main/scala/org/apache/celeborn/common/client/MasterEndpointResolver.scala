package org.apache.celeborn.common.client

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.RpcNameConstants

abstract class MasterEndpointResolver(
    private val conf: CelebornConf,
    private val isWorker: Boolean) extends Logging {

  private var masterEndpointName: Option[String] = None

  protected var activeMasterEndpoints: Option[List[String]] = None

  protected var currentIndex = 0

  if (isWorker && conf.internalPortEnabled) {
    // For worker, we should use the internal endpoints if internal port is enabled.
    this.masterEndpointName = Some(RpcNameConstants.MASTER_INTERNAL_EP)
    resolve(conf.masterInternalEndpoints)
  } else {
    this.masterEndpointName = Some(RpcNameConstants.MASTER_EP)
    resolve(conf.masterEndpoints)
  }

  def getMasterEndpointName: String = masterEndpointName.get

  def getActiveMasterEndpoints: List[String] = activeMasterEndpoints.get

  protected def resolve(endpoints: Array[String]): Unit

  protected def update(endpoints: Array[String]): Unit
}
