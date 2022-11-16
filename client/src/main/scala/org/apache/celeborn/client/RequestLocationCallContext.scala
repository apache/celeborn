package org.apache.celeborn.client

import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.{ChangeLocationResponse, RegisterShuffleResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext

trait RequestLocationCallContext {
  def reply(status: StatusCode, partitionLocationOpt: Option[PartitionLocation]): Unit
}

case class ChangeLocationCallContext(context: RpcCallContext) extends RequestLocationCallContext {
  override def reply(status: StatusCode, partitionLocationOpt: Option[PartitionLocation]): Unit = {
    context.reply(ChangeLocationResponse(status, partitionLocationOpt))
  }
}

case class ApplyNewLocationCallContext(context: RpcCallContext) extends RequestLocationCallContext {
  override def reply(status: StatusCode, partitionLocationOpt: Option[PartitionLocation]): Unit = {
    partitionLocationOpt match {
      case Some(partitionLocation) =>
        context.reply(RegisterShuffleResponse(status, Array(partitionLocation)))
      case None => context.reply(RegisterShuffleResponse(status, Array.empty))
    }
  }
}
