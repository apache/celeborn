/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *
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

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.ControlMessages.ChangeLocationResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcAddress, RpcCallContext}

class RequestLocationCallContextSuite extends CelebornFunSuite {

  private class RecordingRpcCallContext extends RpcCallContext {
    val replies = ArrayBuffer[Any]()
    override def reply(response: Any): Unit = replies += response
    override def sendFailure(e: Throwable): Unit = {}
    override def senderAddress: RpcAddress = null
  }

  private def loc(partitionId: Int, epoch: Int): PartitionLocation =
    new PartitionLocation(partitionId, epoch, "host", 9000, 9100, 9200, 9300, Mode.PRIMARY)

  test("duplicate replies of one partition are ignored and the response completes at " +
    "the distinct partition count") {
    val rpcContext = new RecordingRpcCallContext
    // A Revive message carrying two retire reports of partition 0 (different epochs) and
    // one of partition 1 completes when the 2 distinct partitions have been replied.
    val context = ChangeLocationsCallContext(rpcContext, 2, SerdeVersion.V1)

    context.reply(0, StatusCode.SUCCESS, Some(loc(0, 2)), true, util.Collections.emptyList())
    assert(rpcContext.replies.isEmpty)

    // Duplicate reply for partition 0 (the second epoch's answer): ignored, and the
    // response must not complete yet.
    context.reply(0, StatusCode.SUCCESS, Some(loc(0, 2)), true, util.Collections.emptyList())
    assert(rpcContext.replies.isEmpty)

    context.reply(1, StatusCode.SUCCESS, Some(loc(1, 0)), true, util.Collections.emptyList())
    assert(rpcContext.replies.size == 1)
    val response = rpcContext.replies.head.asInstanceOf[ChangeLocationResponse]
    assert(response.newLocs.size() == 2)
    assert(response.newLocs.get(0)._1 == StatusCode.SUCCESS)
    assert(response.newLocs.get(1)._1 == StatusCode.SUCCESS)
  }
}
