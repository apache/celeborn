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

package org.apache.celeborn.service.deploy.master.scale

import scala.collection.JavaConverters._

import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PbScaleOperation, PbScaleType}

case class ClusterInfo(
    specWorkerCnt: Int,
    reservedWorkerIndices: Seq[Int],
    isInRunningStatus: Boolean,
    workloadUid: String,
    platformVersion: String) {

  require(Option(workloadUid).exists(_.nonEmpty), "A workload UID is required")
  require(Option(platformVersion).exists(_.nonEmpty), "A platform version is required")

  def expectedWorkerIndices: Seq[Int] =
    ClusterInfo.expectedWorkerIndices(specWorkerCnt, reservedWorkerIndices.toSet).toVector

  def matchesSpec(operation: ScaleOperation): Boolean =
    specWorkerCnt == operation.specWorkerCnt &&
      reservedWorkerIndices.toSet == operation.reservedWorkerIndices.toSet
}

object ClusterInfo {
  private[scale] def expectedWorkerIndices(count: Int, reserved: Set[Int]): Iterator[Int] =
    Iterator.from(0).filterNot(reserved).take(count)

  def expectedWorkerIndices(operation: PbScaleOperation): java.util.Set[Integer] = {
    val reserved = operation.getReserveWorkerNumbersList.asScala.iterator.map(_.intValue()).toSet
    expectedWorkerIndices(operation.getExpectedWorkerReplicaNumber, reserved).map(
      Int.box).toSet.asJava
  }
}

case class ScaleOperation(
    specWorkerCnt: Int,
    reservedWorkerIndices: Seq[Int],
    activateWorkers: Seq[WorkerInfo],
    decommissionWorkers: Seq[WorkerInfo],
    scaleType: PbScaleType,
    lastScaleTime: Long,
    isFinished: Boolean = false) {

  def toPb: PbScaleOperation = PbScaleOperation.newBuilder()
    .setScaleType(scaleType)
    .setExpectedWorkerReplicaNumber(specWorkerCnt)
    .addAllReserveWorkerNumbers(reservedWorkerIndices.map(Int.box).asJava)
    .setLastScaleTime(lastScaleTime)
    .setIsFinished(isFinished)
    .build()
}

object ScaleOperation {
  def fromPb(operation: PbScaleOperation): ScaleOperation = {
    ScaleOperation(
      operation.getExpectedWorkerReplicaNumber,
      operation.getReserveWorkerNumbersList.asScala.map(_.intValue()).toVector,
      Seq.empty,
      Seq.empty,
      operation.getScaleType,
      operation.getLastScaleTime,
      operation.getIsFinished)
  }
}

case class OperationResult(success: Boolean, isFinalStatus: Boolean)
