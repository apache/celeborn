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

package org.apache.celeborn.common.metrics

import java.util.Collections

import scala.collection.JavaConverters._

import com.google.protobuf.{Descriptors, DynamicMessage}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.meta.WorkerStatus
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{MessageType, PbHeartbeatFromWorker, PbWorkerStatus}
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromWorker
import org.apache.celeborn.common.util.PbSerDeUtils

class WorkerStatsSuite extends AnyFunSuite {
  private val status = new WorkerStatus(PbWorkerStatus.State.InDecommissionThenIdle_VALUE, 42L)
  private val stats = WorkerStats(
    Map(
      WorkerStats.NettyMemoryUsedRatio -> 0.0,
      WorkerStats.DiskRemainingSize -> 1024.0,
      "CustomMetric" -> 0.75))

  private def heartbeat(workerStats: Option[WorkerStats]): HeartbeatFromWorker = {
    HeartbeatFromWorker(
      "worker",
      1,
      2,
      3,
      4,
      Seq.empty,
      Collections.emptyMap(),
      Collections.emptySet(),
      false,
      status,
      workerStats)
  }

  private def decode(pb: PbHeartbeatFromWorker): HeartbeatFromWorker = {
    ControlMessages.fromTransportMessage(
      new TransportMessage(MessageType.HEARTBEAT_FROM_WORKER, pb.toByteArray))
      .asInstanceOf[HeartbeatFromWorker]
  }

  private def encode(stats: Option[WorkerStats]): PbHeartbeatFromWorker = {
    PbHeartbeatFromWorker.parseFrom(ControlMessages.toTransportMessage(heartbeat(stats)).getPayload)
  }

  private val legacyHeartbeat = {
    val descriptor = PbWorkerStatus.getDescriptor.getFile
    val file = descriptor.toProto.toBuilder
    val index = file.getMessageTypeList.asScala.indexWhere(_.getName == "PbWorkerStatus")
    val workerStatus = file.getMessageType(index)
    file.setMessageType(
      index,
      workerStatus.toBuilder.clearField().clearNestedType()
        .addAllField(workerStatus.getFieldList.asScala.filter(_.getNumber <= 2).asJava))
    Descriptors.FileDescriptor.buildFrom(file.build(), descriptor.getDependencies.asScala.toArray)
      .findMessageTypeByName("PbHeartbeatFromWorker")
  }

  test("round trips numeric and custom metrics inside worker status") {
    val longName = "x" * 256
    val expanded = stats.copy(metrics =
      stats.metrics ++ (1 to 64).map(i => s"$longName$i" -> i.toDouble))
    Seq(stats, expanded).foreach { snapshot =>
      val encoded = encode(Some(snapshot))
      assert(encoded.getWorkerStatus.getStatsCount == snapshot.metrics.size)
      val decoded = decode(encoded)
      assert(decoded.workerStats.contains(snapshot))
      assert(decoded.workerStatus == status)
      assert(!decoded.workerStats.get.metrics.contains(WorkerStats.DiskUsedRatio))
    }
  }

  test("heartbeats without metrics retain the legacy bytes") {
    val encoded = encode(None)
    val legacy = DynamicMessage.parseFrom(legacyHeartbeat, encoded.toByteArray)
    assert(legacy.toByteArray.sameElements(encoded.toByteArray))
    assert(encoded.getWorkerStatus.getStatsCount == 0)
    assert(decode(encoded).workerStats.isEmpty)
  }

  test("legacy readers ignore metrics while retaining worker lifecycle state") {
    val encoded = encode(Some(stats))
    val legacy = DynamicMessage.parseFrom(legacyHeartbeat, encoded.toByteArray)
    val legacyStatus = legacy.getField(legacyHeartbeat.findFieldByName("workerStatus"))
      .asInstanceOf[DynamicMessage]
    assert(legacyStatus.getField(
      legacyStatus.getDescriptorForType.findFieldByName("stateStartTime")) == 42L)
    assert(legacyStatus.getUnknownFields.hasField(3))
    assert(
      PbSerDeUtils.fromPbWorkerStatus(PbWorkerStatus.parseFrom(legacyStatus.toByteArray)) == status)
  }

  test("empty metric names and nonfinite values round trip alongside ordinary metrics") {
    val metrics = stats.metrics ++ Map(
      "" -> 1.0,
      "NaN" -> Double.NaN,
      "PositiveInfinity" -> Double.PositiveInfinity,
      "NegativeInfinity" -> Double.NegativeInfinity)
    val encoded = encode(Some(WorkerStats(metrics)))
    assert(encoded.getWorkerStatus.getStatsCount == metrics.size)
    val decoded = decode(encoded)
    val received = decoded.workerStats.get.metrics
    assert(received.keySet == metrics.keySet)
    metrics.foreach { case (name, value) =>
      val wireValue = encoded.getWorkerStatus.getStatsOrThrow(name)
      if (value.isNaN) {
        assert(wireValue.isNaN)
        assert(received(name).isNaN)
      } else {
        assert(wireValue == value)
        assert(received(name) == value)
      }
    }
    assert(decoded.workerStatus == status)
  }

  test("lifecycle serialization does not copy metrics into metadata messages") {
    val received = encode(Some(stats)).getWorkerStatus
    val lifecycle = PbSerDeUtils.fromPbWorkerStatus(received)
    val metadataStatus = PbSerDeUtils.toPbWorkerStatus(lifecycle)
    assert(metadataStatus.getStatsCount == 0)
    assert(lifecycle == status)
  }
}
