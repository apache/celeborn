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

package com.aliyun.emr.rss.client

import java.util.concurrent.{ConcurrentHashMap, ExecutorService}

import scala.util.{Failure, Success}

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.PartitionLocation
import com.aliyun.emr.rss.common.protocol.message.{Message, StatusCode}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.LocationRenewalResponse
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef

object ShuffleClientHelper extends Logging {
  def sendShuffleSplitAsync(endpointRef: RpcEndpointRef, message: Message,
    executors: ExecutorService, reducerSplittingSet: java.util.Set[Integer], reducerId: Int,
    shuffleId: Int, shuffleLocs: ConcurrentHashMap[Integer, PartitionLocation]): Unit = {
    endpointRef.ask[LocationRenewalResponse](message).onComplete {
      case Success(value) =>
        if (value.status == StatusCode.Success) {
          shuffleLocs.put(reducerId, value.partition)
        } else {
          logInfo(s"split failed for ${value.status.toString()}, " +
            s"shuffle file can be larger than expected, try split again");
        }
        reducerSplittingSet.remove(reducerId)
      case Failure(exception) =>
        reducerSplittingSet.remove(reducerId)
        logWarning(s"Shuffle file split failed for map ${shuffleId} reduceId ${reducerId}," +
          s" try again, detail : {}", exception);

    }(concurrent.ExecutionContext.fromExecutorService(executors))
  }
}

