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
    isInRunningStatus: Boolean) {

  def expectedWorkerIndices: Seq[Int] = {
    val reserved = reservedWorkerIndices.toSet
    Iterator.from(0).filterNot(reserved).take(specWorkerCnt).toVector
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

  def reservedWorkerIndicesJava: java.util.List[Integer] =
    reservedWorkerIndices.map(Int.box).asJava
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

case class OperationResult(
    success: Boolean,
    isFinalStatus: Boolean,
    message: String = "")
