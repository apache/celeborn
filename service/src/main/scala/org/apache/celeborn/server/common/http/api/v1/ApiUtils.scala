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

package org.apache.celeborn.server.common.http.api.v1

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.celeborn.common.meta.{WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PbWorkerStatus.State
import org.apache.celeborn.server.common.http.api.v1.dto.{PartitionLocationData, WorkerData}

object ApiUtils {
  def workerData(workerInfo: WorkerInfo): WorkerData = {
    val (diskInfos, slots) =
      if (workerInfo.diskInfos == null) {
        Map.empty[String, String] -> 0L
      } else {
        workerInfo.diskInfos.asScala.map { case (disk, diskInfo) =>
          disk -> diskInfo.toString()
        }.toMap -> workerInfo.usedSlots()
      }
    val userResourceConsumption =
      if (workerInfo.userResourceConsumption == null) {
        Map.empty[String, String]
      } else {
        workerInfo.userResourceConsumption.asScala.map { case (user, resourceConsumption) =>
          user.toString -> resourceConsumption.toString()
        }.toMap
      }

    new WorkerData(
      workerInfo.host,
      workerInfo.rpcPort,
      workerInfo.pushPort,
      workerInfo.fetchPort,
      workerInfo.replicatePort,
      workerInfo.internalPort,
      slots,
      workerInfo.lastHeartbeat,
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - workerInfo.lastHeartbeat),
      diskInfos.asJava,
      userResourceConsumption.asJava,
      Option(workerInfo.endpoint).map(_.toString).orNull,
      workerInfo.workerStatus.getState.toString,
      workerInfo.workerStatus.getStateStartTime)
  }

  def workerData(
      workerInfo: WorkerInfo,
      currentStatus: WorkerStatus,
      isShutdown: Boolean,
      isRegistered: Boolean): WorkerData = {
    val data = workerData(workerInfo)

    data.setWorkerState(currentStatus.getState.toString)
    data.setWorkerStateStartTime(currentStatus.getStateStartTime)

    data.setShutdown(isShutdown)
    data.setRegistered(isRegistered)
    data.setDecommissioning(
      isShutdown && (currentStatus.getState == State.InDecommission || currentStatus.getState == State.InDecommissionThenIdle))

    data
  }

  def partitionLocationData(partitionLocation: PartitionLocation): PartitionLocationData = {
    new PartitionLocationData(
      partitionLocation.getId + "-" + partitionLocation.getEpoch,
      partitionLocation.hostAndPorts(),
      partitionLocation.getMode.toString,
      Option(partitionLocation.getPeer).map(_.hostAndPorts()).orNull,
      partitionLocation.getStorageInfo.toString,
      partitionLocation.getMapIdBitMap.toString)
  }
}
