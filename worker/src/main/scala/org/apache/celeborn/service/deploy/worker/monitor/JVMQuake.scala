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

package org.apache.celeborn.service.deploy.worker.monitor

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.google.common.annotations.VisibleForTesting
import com.sun.management.HotSpotDiagnosticMXBean
import sun.jvmstat.monitor._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

/**
 * The JVM quake provides granular monitoring of GC behavior, which enables early detection of memory management
 * issues and facilitates fast failure.
 *
 * Note: The principle is in alignment with GC instability detection algorithm for jvmquake project of Netflix:
 * https://github.com/Netflix-Skunkworks/jvmquake.
 *
 * @param conf Celeborn configuration with jvm quake config.
 */
class JVMQuake(conf: CelebornConf, uniqueId: String = UUID.randomUUID().toString) extends Logging {

  import JVMQuake._

  val dumpFile = s"worker-quake-heapdump-$uniqueId.hprof"
  var heapDumped: Boolean = false

  private[this] val enabled = conf.workerJvmQuakeEnabled
  private[this] val checkInterval = conf.workerJvmQuakeCheckInterval
  private[this] val runtimeWeight = conf.workerJvmQuakeRuntimeWeight
  private[this] val dumpThreshold = conf.workerJvmQuakeDumpThreshold.toNanos
  private[this] val killThreshold = conf.workerJvmQuakeKillThreshold.toNanos
  private[this] val dumpEnabled = conf.workerJvmQuakeDumpEnabled
  private[this] val dumpPath = conf.workerJvmQuakeDumpPath
  private[this] val exitCode = conf.workerJvmQuakeExitCode

  private[this] var lastExitTime: Long = 0L
  private[this] var lastGCTime: Long = 0L
  private[this] var bucket: Long = 0L
  private[this] var scheduler: ScheduledExecutorService = _

  def start(): Unit = {
    if (enabled) {
      lastExitTime = getLastExitTime
      lastGCTime = getLastGCTime
      scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-jvm-quake-scheduler")
      scheduler.scheduleWithFixedDelay(
        new Runnable() {
          override def run(): Unit = {
            JVMQuake.this.run()
          }
        },
        0,
        checkInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    if (enabled) {
      scheduler.shutdown()
    }
  }

  private def run(): Unit = {
    val currentExitTime = getLastExitTime
    val currentGCTime = getLastGCTime
    val gcTime = currentGCTime - lastGCTime
    val runTime = currentExitTime - lastExitTime - gcTime

    bucket = Math.max(0, bucket + gcTime - BigDecimal(runTime * runtimeWeight).toLong)
    logDebug(s"Time: (gc time: ${Utils.msDurationToString(gcTime)}, execution time: ${Utils.msDurationToString(runTime)})")
    logDebug(
      s"Capacity: (bucket: $bucket, dump threshold: $dumpThreshold, kill threshold: $killThreshold)")

    if (bucket > dumpThreshold) {
      logError(s"JVM GC has reached the threshold: bucket: $bucket, dumpThreshold: $dumpThreshold.")
      if (shouldHeapDump) {
        val savePath = getHeapDumpSavePath
        val linkPath = getHeapDumpLinkPath
        heapDump(savePath, linkPath)
      } else if (bucket > killThreshold) {
        logError(s"Exit JVM with $exitCode. JVM GC has reached the threshold: bucket: $bucket, killThreshold: $killThreshold.")
        System.exit(exitCode)
      }
    }
    lastExitTime = currentExitTime
    lastGCTime = currentGCTime
  }

  def shouldHeapDump: Boolean = {
    dumpEnabled && !heapDumped
  }

  @VisibleForTesting
  def getHeapDumpSavePath: String =
    dumpPath

  @VisibleForTesting
  def getHeapDumpLinkPath: String =
    s"${new File(dumpPath).getParent}/link/${Utils.getProcessId}"

  private def heapDump(savePath: String, linkPath: String, live: Boolean = false): Unit = {
    val saveDir = new File(savePath)
    if (!saveDir.exists()) {
      saveDir.mkdirs()
    }
    val heapDump = new File(saveDir, dumpFile)
    if (heapDump.exists()) {
      // Each worker process only generates one heap dump. Skip when heap dump of worker already exists.
      logWarning(s"Skip because heap dump of worker already exists: $heapDump.")
      heapDumped = true
      return
    }
    logInfo(s"Starting heap dump: $heapDump.")
    ManagementFactory.newPlatformMXBeanProxy(
      ManagementFactory.getPlatformMBeanServer,
      "com.sun.management:type=HotSpotDiagnostic",
      classOf[HotSpotDiagnosticMXBean]).dumpHeap(heapDump.getAbsolutePath, live)
    val linkDir = new File(linkPath)
    if (linkDir.exists()) {
      // Each worker process only generates one heap dump. Skip when symbolic link of heap dump exists.
      logWarning(s"Skip because symbolic link of heap dump exists: $linkPath.")
    } else if (!linkDir.getParentFile.exists()) {
      linkDir.getParentFile.mkdirs()
    }
    try {
      Files.createSymbolicLink(linkDir.toPath, saveDir.toPath)
      logInfo(s"Created symbolic link: $linkPath.")
    } catch {
      case e: Exception =>
        logError("Create symbolic link failed.", e)
    } finally {
      heapDumped = true
      logInfo(s"Finished heap dump: $dumpFile.")
    }
  }
}

object JVMQuake {

  private[this] var quake: JVMQuake = _

  def create(conf: CelebornConf, uniqueId: String): JVMQuake = {
    set(new JVMQuake(conf, uniqueId))
    quake
  }

  def get: JVMQuake = {
    quake
  }

  def set(quake: JVMQuake): Unit = {
    this.quake = quake
  }

  private[this] lazy val monitoredVm: MonitoredVm = {
    val host = MonitoredHost.getMonitoredHost(new HostIdentifier("localhost"))
    host.getMonitoredVm(new VmIdentifier("local://%s@localhost".format(Utils.getProcessId)))
  }

  private[this] lazy val ygcExitTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.0.lastExitTime")
  private[this] lazy val fgcExitTimeMonitor: Monitor =
    monitoredVm.findByName("sun.gc.collector.1.lastExitTime")
  private[this] lazy val ygcTimeMonitor: Monitor = monitoredVm.findByName("sun.gc.collector.0.time")
  private[this] lazy val fgcTimeMonitor: Monitor = monitoredVm.findByName("sun.gc.collector.1.time")

  private def getLastExitTime: Long = Math.max(
    ygcExitTimeMonitor.getValue.asInstanceOf[Long],
    fgcExitTimeMonitor.getValue.asInstanceOf[Long])

  private def getLastGCTime: Long =
    ygcTimeMonitor.getValue.asInstanceOf[Long] + fgcTimeMonitor.getValue.asInstanceOf[Long]
}
