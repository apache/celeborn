package org.apache.celeborn.common.rpc

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.{Counter, Histogram, UniformReservoir}
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
  private val queueLength: Counter = new Counter()
  private var maxQueueLength: Long = 0
  private val slowRpcThreshold: Long = conf.rpcSlowThreshold()
  private val slowRpcInterval: Long = conf.rpcSlowInterval()
  private val slowDumpInterval: Long = conf.rpcDumpInterval()
  private val lastDumpTime: AtomicLong = new AtomicLong(0)
  private val lastSlowLogTime: AtomicLong = new AtomicLong(0)
  final private val useHistogram =
    if (name == RpcNameConstants.LIFECYCLE_MANAGER_EP || name == RpcEndpointVerifier.NAME) {
      true
    } else {
      false
    }
  final private val QUEUE_LENGTH_METRIC = s"${name}_${RpcSource.QUEUE_LENGTH}"
  final private val QUEUE_TIME_METRIC = s"${name}_${RpcSource.QUEUE_TIME}"
  final private val PROCESS_TIME_METRIC = s"${name}_${RpcSource.PROCESS_TIME}"

  if (name != null) {
    rpcSource.addGauge(QUEUE_LENGTH_METRIC) {
      () => queueSize()
    }
    rpcSource.addTimer(QUEUE_TIME_METRIC)
    rpcSource.addTimer(PROCESS_TIME_METRIC)
  }

  def updateHistogram(name: String, value: Long): Unit = {
    histogramMap.putIfAbsent(name, new Histogram(new UniformReservoir()))
    val histogram = histogramMap.get(name)
    histogram.update(value)
  }

  def queueSize(): Long = {
    queueLength.getCount
  }

  def incQueueLength(): Unit = {
    queueLength.inc()
    if (queueSize() > maxQueueLength) {
      maxQueueLength = queueSize()
    }
  }

  def decQueueLength(): Unit = {
    queueLength.dec()
  }

  private def logSlowRpc(message: InboxMessage, queueTime: Long, processTime: Long): Unit = {
    if (queueTime + processTime > slowRpcThreshold) {
      val lastLogTime = lastSlowLogTime.get()
      if (slowRpcInterval < 0 || System.currentTimeMillis() - lastLogTime > slowRpcInterval &&
        lastSlowLogTime.compareAndSet(lastLogTime, System.currentTimeMillis())) {
        logWarning(
          s"slow rpc detected: currentQueueSize = ${queueSize()}, queueTime=$queueTime processTime=$processTime message=$message")
      }

      val lastTime = lastDumpTime.get
      if (useHistogram && System.currentTimeMillis() - lastTime > slowDumpInterval &&
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
        case _ =>
          "unknown"
      }
    }
    val msgName = messageName(message)

    if (useHistogram) {
      updateHistogram(QUEUE_TIME_METRIC, queueTime)
      updateHistogram(PROCESS_TIME_METRIC, processTime)
      updateHistogram(msgName, processTime)
    } else {
      rpcSource.updateTimer(QUEUE_TIME_METRIC, queueTime)
      rpcSource.updateTimer(PROCESS_TIME_METRIC, processTime)
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
    builder.append(s"current queue size = ${queueSize()}").append("\n")
    builder.append(s"max queue length = $maxQueueLength").append("\n")
    histogramMap.entrySet.forEach(entry => {
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
