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

package org.apache.celeborn.common.rpc

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.codahale.metrics.{Histogram, UniformReservoir}
import com.google.protobuf.GeneratedMessageV3

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.protocol.message.Message
import org.apache.celeborn.common.rpc.netty.{InboxMessage, OneWayMessage, RpcEndpointVerifier, RpcMessage}
import org.apache.celeborn.common.util.JavaUtils

private[celeborn] class RpcMetricsTracker(
    val name: String,
    rpcSource: RpcSource,
    conf: CelebornConf) extends Logging {

  // Histogram is used for Client, eg LifecycleManager
  val histogramMap: ConcurrentMap[String, Histogram] =
    JavaUtils.newConcurrentHashMap[String, Histogram]

  private val maxQueueLength: AtomicLong = new AtomicLong(0)
  private val slowRpcThreshold: Long = conf.rpcSlowThresholdNs()
  private val slowRpcInterval: Long = conf.rpcSlowIntervalMs()
  private val rpcDumpInterval: Long = conf.rpcDumpIntervalMs()
  private val lastDumpTime: AtomicLong = new AtomicLong(0)
  private val lastSlowLogTime: AtomicLong = new AtomicLong(0)
  final private val useHistogram =
    if (name == RpcNameConstants.LIFECYCLE_MANAGER_EP || name == RpcEndpointVerifier.NAME) {
      true
    } else {
      false
    }
  final private val NAME_LABEL = Map("name" -> name)

  private var queueLengthFunc: () => Long = _

  def init(lengthFunc: () => Long): Unit = {
    queueLengthFunc = lengthFunc
    if (name != null) {
      rpcSource.addGauge(RpcSource.QUEUE_LENGTH, NAME_LABEL)(queueLengthFunc)

      rpcSource.addTimer(RpcSource.QUEUE_TIME, NAME_LABEL)
      rpcSource.addTimer(RpcSource.PROCESS_TIME, NAME_LABEL)
    }
  }

  def updateHistogram(name: String, value: Long): Unit = {
    histogramMap.putIfAbsent(name, new Histogram(new UniformReservoir()))
    val histogram = histogramMap.get(name)
    histogram.update(value)
  }

  def updateMaxLength(): Unit = {
    val len = queueLengthFunc()
    if (len > maxQueueLength.get()) {
      maxQueueLength.set(len)
    }
  }

  private def logSlowRpc(message: InboxMessage, queueTime: Long, processTime: Long): Unit = {
    if (queueTime + processTime > slowRpcThreshold) {
      val lastLogTime = lastSlowLogTime.get()
      if (slowRpcInterval < 0 || System.currentTimeMillis() - lastLogTime > slowRpcInterval &&
        lastSlowLogTime.compareAndSet(lastLogTime, System.currentTimeMillis())) {
        logWarning(
          s"slow rpc detected: currentQueueSize = ${queueLengthFunc()}, queueTime=$queueTime processTime=$processTime message=$message")
      }

      val lastTime = lastDumpTime.get
      if (useHistogram && System.currentTimeMillis() - lastTime > rpcDumpInterval &&
        lastDumpTime.compareAndSet(lastTime, System.currentTimeMillis())) {
        dump()
      }
    }
  }

  def record(message: Any, queueTime: Long, processTime: Long): Unit = {
    def messageName(message: Any): String = {
      message match {
        case legacy: Message =>
          legacy.getClass.toString
        case pb: GeneratedMessageV3 =>
          pb.getDescriptorForType.getFullName
        case _: RpcEndpointVerifier.CheckExistence =>
          "CheckExistence"
        case _ =>
          "unknown"
      }
    }
    val msgName = messageName(message)

    if (useHistogram) {
      updateHistogram(RpcSource.QUEUE_TIME, queueTime)
      updateHistogram(RpcSource.PROCESS_TIME, processTime)
      updateHistogram(msgName, processTime)
    } else {
      rpcSource.updateTimer(RpcSource.QUEUE_TIME, queueTime, NAME_LABEL)
      rpcSource.updateTimer(RpcSource.PROCESS_TIME, processTime, NAME_LABEL)
      rpcSource.updateTimer(msgName, processTime)
    }
  }

  def update(message: InboxMessage): Unit = {
    message match {
      case rpc @ RpcMessage(_, content, _) =>
        val queueTime = rpc.dequeueTime - rpc.enqueueTime
        val processTime = rpc.endProcessTime - rpc.dequeueTime
        record(content, queueTime, processTime)
        logSlowRpc(message, queueTime, processTime)
      case one @ OneWayMessage(_, content) =>
        val queueTime = one.dequeueTime - one.enqueueTime
        val processTime = one.endProcessTime - one.dequeueTime
        record(content, queueTime, processTime)
        logSlowRpc(message, queueTime, processTime)
      case _ =>
    }
  }

  def dump(): Unit = {
    if (!useHistogram)
      return

    val builder = new StringBuilder();
    builder.append(s"RPC statistics for $name").append("\n")
    builder.append(s"current queue size = ${queueLengthFunc()}").append("\n")
    builder.append(s"max queue length = ${maxQueueLength.get()}").append("\n")
    histogramMap.entrySet.asScala.foreach(entry => {
      val histogram = entry.getValue
      val snapshot = histogram.getSnapshot;
      builder.append(s"histogram for $name RPC metrics: ").append(entry.getKey).append("\n")
      builder.append("count: ").append(histogram.getCount).append("\n")
        .append("min: ").append(snapshot.getMin).append("\n")
        .append("mean: ").append(snapshot.getMean).append("\n")
        .append("p50: ").append(snapshot.getMedian).append("\n")
        .append("p75: ").append(snapshot.get75thPercentile).append("\n")
        .append("p95: ").append(snapshot.get95thPercentile).append("\n")
        .append("p99: ").append(snapshot.get99thPercentile()).append("\n")
        .append("max: ").append(snapshot.getMax).append("\n")
    })
    logInfo(builder.toString())
  }
}
