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

package org.apache.celeborn.client

import java.util
import java.util.concurrent.atomic.LongAdder

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.{ChangeLocationResponse, RegisterShuffleResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.JavaUtils

trait RequestLocationCallContext {
  def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationsOpt: Option[Seq[PartitionLocation]],
      available: Boolean): Unit
}

case class ChangeLocationsCallContext(
    context: RpcCallContext,
    partitionCount: Int)
  extends RequestLocationCallContext with Logging {
  val endedMapIds = new util.HashSet[Integer]()
  val newLocs =
    JavaUtils.newConcurrentHashMap[Integer, (StatusCode, Boolean, Seq[PartitionLocation])](
      partitionCount)
  private val requestCount = new LongAdder

  def markMapperEnd(mapId: Int): Unit = this.synchronized {
    endedMapIds.add(mapId)
  }

  override def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationsOpt: Option[Seq[PartitionLocation]],
      available: Boolean): Unit = this.synchronized {
    if (newLocs.containsKey(partitionId) && newLocs.get(partitionId)._3 != null && newLocs.get(
        partitionId)._3.length != partitionLocationsOpt.getOrElse(Seq.empty).length) {
      logError(s"PartitionId $partitionId already exists! " +
        s"${newLocs.get(partitionId)._3.length} != ${partitionLocationsOpt.getOrElse(Seq.empty).length}")
    }
    if (partitionLocationsOpt.isDefined && (status == StatusCode.RESERVE_SLOTS_FAILED || status == StatusCode.SLOT_NOT_AVAILABLE)) {
      newLocs.put(partitionId, (StatusCode.SUCCESS, available, partitionLocationsOpt.orNull))
    } else {
      newLocs.put(partitionId, (status, available, partitionLocationsOpt.orNull))
    }
    requestCount.increment()
    if (requestCount.intValue() == partitionCount
      || StatusCode.SHUFFLE_NOT_REGISTERED == status
      || StatusCode.STAGE_ENDED == status) {
      context.reply(ChangeLocationResponse(endedMapIds, newLocs))
    }
  }
}

case class ApplyNewLocationCallContext(context: RpcCallContext) extends RequestLocationCallContext {
  override def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationsOpt: Option[Seq[PartitionLocation]],
      available: Boolean): Unit = {
    partitionLocationsOpt match {
      case Some(partitionLocations) =>
        context.reply(RegisterShuffleResponse(status, partitionLocations.toArray))
      case None => context.reply(RegisterShuffleResponse(status, Array.empty))
    }
  }
}
