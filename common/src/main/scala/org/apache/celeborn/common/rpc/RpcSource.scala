package org.apache.celeborn.common.rpc

import java.util.concurrent.ConcurrentHashMap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.AbstractSource

class RpcSource(conf: CelebornConf) extends AbstractSource(conf, RpcSource.ROLE_RPC) {
  override def sourceName: String = RpcSource.ROLE_RPC

  private val msgNameSet = ConcurrentHashMap.newKeySet[String]()

  override def updateTimer(name: String, value: Long): Unit = {
    if (!msgNameSet.contains(name)) {
      addTimer(name)
      msgNameSet.add(name)
    }
    super.updateTimer(name, value)
  }

  override def addTimer(name: String): Unit = {
    if (!msgNameSet.contains(name)) {
      super.addTimer(name)
      msgNameSet.add(name)
    }
  }

  startCleaner()
}

object RpcSource {
  val ROLE_RPC = "RPC"

  val QUEUE_LENGTH = "QueueLength"
  val QUEUE_TIME = "QueueTime"
  val PROCESS_TIME = "ProcessTime"
}
