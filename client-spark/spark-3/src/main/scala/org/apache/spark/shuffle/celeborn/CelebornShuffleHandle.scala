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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetPartitionLocation, GetPartitionLocationResponse}
import org.apache.celeborn.common.rpc.RpcEndpointRef

class CelebornShuffleHandle[K, V, C](
    val appUniqueId: String,
    val lifecycleManagerHost: String,
    val lifecycleManagerPort: Int,
    val userIdentifier: UserIdentifier,
    shuffleId: Int,
    val throwsFetchFailure: Boolean,
    val numMappers: Int,
    dependency: ShuffleDependency[K, V, C],
    val extension: Array[Byte],
    val ioCryptoInitializationVector: Array[Byte],
    @transient lifecycleManagerRef: RpcEndpointRef = null)
  extends BaseShuffleHandle(shuffleId, dependency) {

  import org.apache.spark.MapOutputTrackerMaster

  def this(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      userIdentifier: UserIdentifier,
      shuffleId: Int,
      throwsFetchFailure: Boolean,
      numMappers: Int,
      dependency: ShuffleDependency[K, V, C]) = this(
    appUniqueId,
    lifecycleManagerHost,
    lifecycleManagerPort,
    userIdentifier,
    shuffleId,
    throwsFetchFailure,
    numMappers,
    dependency,
    null,
    null)

  // will be called by MapOutputTrackerMaster
  def getReduceTaskPreferLocation(
      tracker: MapOutputTrackerMaster,
      dep: ShuffleDependency[_, _, _],
      partitionId: Int): Seq[String] = {
    if (lifecycleManagerRef == null) {
      return Seq.empty
    }
    try {
      val res = lifecycleManagerRef.askSync[GetPartitionLocationResponse](GetPartitionLocation(
        shuffleId,
        partitionId))
      res.hosts
    } catch {
      case _: Exception =>
        Seq.empty
    }
  }

  def getMapLocation(
      tracker: MapOutputTrackerMaster,
      dep: ShuffleDependency[_, _, _],
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Seq[String] = {
    // endPartition - startPartition != 1 means enabled AQE local shuffle read
    if (endPartition - startPartition != 1) {
      SparkUtils.printSparkAQELocalShuffleReadRecommend()
      return Seq.empty
    }
    getReduceTaskPreferLocation(tracker, dep, startPartition)
  }
}
