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

  def close(): Unit = {
    histogramMap.clear()
    rpcSource.removeGauge(RpcSource.QUEUE_LENGTH)
    rpcSource.removeTimer(RpcSource.QUEUE_TIME)
    rpcSource.removeTimer(RpcSource.PROCESS_TIME)
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
          s"slow rpc detected: currentQueueSize = ${queueLengthFunc()}, queueTime=$queueTime(ns) processTime=$processTime(ns) message=$message")
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

  /**
   * Point-in-time view of the collected RPC metrics, or None when histogram based metrics are
   * not collected for this endpoint (see [[useHistogram]]). The histograms are never reset, so
   * the values are cumulative since the endpoint started.
   */
  def snapshot(): Option[RpcMetricsSnapshot] = {
    if (!useHistogram) {
      return None
    }
    val queueSize = if (queueLengthFunc == null) 0L else queueLengthFunc()
    val methods = histogramMap.entrySet.asScala.map { entry =>
      val histogram = entry.getValue
      val snapshot = histogram.getSnapshot
      RpcMethodMetrics(
        entry.getKey,
        histogram.getCount,
        snapshot.getMin,
        snapshot.getMean,
        snapshot.getMedian,
        snapshot.get75thPercentile,
        snapshot.get95thPercentile,
        snapshot.get99thPercentile(),
        snapshot.getMax)
    }.toSeq.sortBy(_.name)
    Some(RpcMetricsSnapshot(
      name,
      queueSize,
      maxQueueLength.get(),
      methods,
      System.currentTimeMillis()))
  }

  def dump(): Unit = snapshot().foreach(s => logInfo(renderSnapshot(s)))

  private def renderSnapshot(snapshot: RpcMetricsSnapshot): String = {
    val builder = new StringBuilder()
    builder.append(s"RPC statistics for ${snapshot.endpointName} (time unit: ns)").append("\n")
    builder.append(s"current queue size = ${snapshot.queueSize}").append("\n")
    builder.append(s"max queue length = ${snapshot.maxQueueLength}").append("\n")
    snapshot.methods.foreach { method =>
      builder.append(s"histogram for ${snapshot.endpointName} RPC metrics: ")
        .append(method.name).append("\n")
      builder.append("count: ").append(method.count).append("\n")
        .append("min: ").append(method.min).append("\n")
        .append("mean: ").append(method.mean).append("\n")
        .append("p50: ").append(method.p50).append("\n")
        .append("p75: ").append(method.p75).append("\n")
        .append("p95: ").append(method.p95).append("\n")
        .append("p99: ").append(method.p99).append("\n")
        .append("max: ").append(method.max).append("\n")
    }
    builder.toString()
  }
}
