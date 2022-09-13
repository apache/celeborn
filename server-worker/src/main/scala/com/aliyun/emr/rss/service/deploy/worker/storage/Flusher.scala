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

package com.aliyun.emr.rss.service.deploy.worker.storage

import java.io.IOException
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLongArray, LongAdder}

import scala.collection.JavaConverters._
import scala.util.Random

import io.netty.buffer.{CompositeByteBuf, Unpooled}

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.DiskStatus
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.MemoryTracker
import com.aliyun.emr.rss.common.protocol.StorageInfo
import com.aliyun.emr.rss.service.deploy.worker.WorkerSource

abstract private[worker] class Flusher(
    val workerSource: AbstractSource,
    val threadCount: Int,
    val flushAvgTimeWindowSize: Int,
    val flushAvgTimeMinimumCount: Int) extends Logging {
  protected lazy val flusherId = System.identityHashCode(this)
  protected val workingQueues = new Array[LinkedBlockingQueue[FlushTask]](threadCount)
  protected val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf]()
  protected val workers = new Array[Thread](threadCount)
  protected var nextWorkerIndex: Int = 0
  protected val flushCount = new LongAdder
  protected val flushTotalTime = new LongAdder
  protected val avgTimeWindow = new Array[(Long, Long)](flushAvgTimeWindowSize)
  protected var avgTimeWindowCurrentIndex = 0

  val lastBeginFlushTime: AtomicLongArray = new AtomicLongArray(threadCount)
  val stopFlag = new AtomicBoolean(false)
  val rand = new Random()

  init()

  private def init(): Unit = {
    for (i <- 0 until flushAvgTimeWindowSize) {
      avgTimeWindow(i) = (0L, 0L)
    }
    for (i <- 0 until lastBeginFlushTime.length()) {
      lastBeginFlushTime.set(i, -1)
    }
    for (index <- 0 until threadCount) {
      workingQueues(index) = new LinkedBlockingQueue[FlushTask]()
      workers(index) = new Thread(s"$this-$index") {
        override def run(): Unit = {
          while (!stopFlag.get()) {
            val task = workingQueues(index).take()
            val key = s"Flusher-$this-${rand.nextInt()}"
            workerSource.sample(WorkerSource.FlushDataTime, key) {
              if (!task.notifier.hasException) {
                try {
                  val flushBeginTime = System.nanoTime()
                  lastBeginFlushTime.set(index, flushBeginTime)
                  task.flush()
                  flushTotalTime.add(System.nanoTime() - flushBeginTime)
                  flushCount.increment()
                } catch {
                  case _: ClosedByInterruptException =>
                  case e: IOException =>
                    task.notifier.setException(e)
                    processIOException(e, DiskStatus.READ_OR_WRITE_FAILURE)
                }
                lastBeginFlushTime.set(index, -1)
              }
              returnBuffer(task.buffer)
              task.notifier.numPendingFlushes.decrementAndGet()
            }
          }
        }
      }
      workers(index).setDaemon(true)
      workers(index).setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(s"$this thread terminated.", e)
        }
      })
      workers(index).start()
    }
  }

  def getWorkerIndex: Int = synchronized {
    nextWorkerIndex = (nextWorkerIndex + 1) % threadCount
    nextWorkerIndex
  }

  def averageFlushTime(): Long = {
    if (this.isInstanceOf[LocalFlusher]) {
      logInfo(s"Flush count in ${this.asInstanceOf[LocalFlusher].mountPoint}" +
        s" last heartbeat interval: $flushCount")
    }
    val currentFlushTime = flushTotalTime.sumThenReset()
    val currentFlushCount = flushCount.sumThenReset()
    if (currentFlushCount >= flushAvgTimeMinimumCount) {
      avgTimeWindow(avgTimeWindowCurrentIndex) = (currentFlushTime, currentFlushCount)
      avgTimeWindowCurrentIndex = (avgTimeWindowCurrentIndex + 1) % flushAvgTimeWindowSize
    }

    var totalFlushTime = 0L
    var totalFlushCount = 0L
    avgTimeWindow.foreach { case (flushTime, flushCount) =>
      totalFlushTime = totalFlushTime + flushTime
      totalFlushCount = totalFlushCount + flushCount
    }

    if (totalFlushCount != 0) {
      totalFlushTime / totalFlushCount
    } else {
      0L
    }
  }

  def takeBuffer(): CompositeByteBuf = {
    var buffer = bufferQueue.poll()
    if (buffer == null) {
      buffer = Unpooled.compositeBuffer(256)
    }
    buffer
  }

  def returnBuffer(buffer: CompositeByteBuf): Unit = {
    MemoryTracker.instance().releaseDiskBuffer(buffer.readableBytes())
    buffer.removeComponents(0, buffer.numComponents())
    buffer.clear()

    bufferQueue.put(buffer)
  }

  def addTask(task: FlushTask, timeoutMs: Long, workerIndex: Int): Boolean = {
    workingQueues(workerIndex).offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  def bufferQueueInfo(): String = s"$this used buffers: ${bufferQueue.size()}"

  def stopAndCleanFlusher(): Unit = {
    stopFlag.set(true)
    try {
      workers.foreach(_.interrupt())
    } catch {
      case e: Exception =>
        logError(s"Exception when interrupt worker: ${workers.mkString(",")}, $e")
    }
    workingQueues.foreach { queue =>
      queue.asScala.foreach { task =>
        returnBuffer(task.buffer)
      }
    }
  }

  def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit
}

private[worker] class LocalFlusher(
    workerSource: AbstractSource,
    val deviceMonitor: DeviceMonitor,
    threadCount: Int,
    val mountPoint: String,
    flushAvgTimeWindowSize: Int,
    flushAvgTimeMinimumCount: Int,
    val diskType: StorageInfo.Type) extends Flusher(
    workerSource,
    threadCount,
    flushAvgTimeWindowSize,
    flushAvgTimeMinimumCount)
  with DeviceObserver with Logging {

  deviceMonitor.registerFlusher(this)

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    stopFlag.set(true)
    logError(s"$this write failed, report to DeviceMonitor, eception: $e")
    deviceMonitor.reportDeviceError(mountPoint, e, deviceErrorType)
  }

  override def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = {
    logError(s"$this is notified Disk $mountPoint $diskStatus! Stop LocalFlusher.")
    stopAndCleanFlusher()
    deviceMonitor.unregisterFlusher(this)
  }

  override def hashCode(): Int = {
    mountPoint.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[LocalFlusher] &&
    obj.asInstanceOf[LocalFlusher].mountPoint.equals(mountPoint)
  }

  override def toString(): String = {
    s"LocalFlusher@$flusherId-$mountPoint"
  }
}

final private[worker] class HdfsFlusher(
    workerSource: AbstractSource,
    threadCount: Int,
    flushAvgTimeWindowSize: Int,
    flushAvgTimeMinimumCount: Int) extends Flusher(
    workerSource,
    threadCount,
    flushAvgTimeWindowSize,
    flushAvgTimeMinimumCount) with Logging {

  override def toString: String = s"HdfsFlusher@$flusherId"

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    stopAndCleanFlusher()
    logError(s"$this write failed, reason $deviceErrorType ,exception: $e")
  }
}
