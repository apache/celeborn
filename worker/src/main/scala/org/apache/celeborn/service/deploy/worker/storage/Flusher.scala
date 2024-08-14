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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.IOException
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{ExecutorService, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLongArray}

import scala.collection.JavaConverters._
import scala.util.Random

import io.netty.buffer.{CompositeByteBuf, PooledByteBufAllocator, Unpooled}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskStatus, TimeWindow}
import org.apache.celeborn.common.metrics.source.{AbstractSource, ThreadPoolSource}
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.WorkerSource.FLUSH_WORKING_QUEUE_SIZE
import org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionController
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

abstract private[worker] class Flusher(
    val workerSource: AbstractSource,
    val threadCount: Int,
    val allocator: PooledByteBufAllocator,
    val maxComponents: Int,
    flushTimeMetric: TimeWindow,
    mountPoint: String) extends Logging {
  protected lazy val flusherId: Int = System.identityHashCode(this)
  protected val workingQueues = new Array[LinkedBlockingQueue[FlushTask]](threadCount)
  protected val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf]()
  protected val workers = new Array[ExecutorService](threadCount)
  protected var nextWorkerIndex: Int = 0

  val lastBeginFlushTime: AtomicLongArray = new AtomicLongArray(threadCount)
  val stopFlag = new AtomicBoolean(false)

  init()

  private def init(): Unit = {
    for (i <- 0 until lastBeginFlushTime.length()) {
      lastBeginFlushTime.set(i, -1)
    }
    for (index <- 0 until threadCount) {
      workingQueues(index) = new LinkedBlockingQueue[FlushTask]()
      workers(index) = ThreadUtils.newDaemonSingleThreadExecutor(s"$this-$index")
      workers(index).submit(new Runnable {
        override def run(): Unit = {
          while (!stopFlag.get()) {
            val task = workingQueues(index).take()
            val key = s"Flusher-$this-${Random.nextInt()}"
            workerSource.sample(WorkerSource.FLUSH_DATA_TIME, key) {
              if (!task.notifier.hasException) {
                try {
                  val flushBeginTime = System.nanoTime()
                  lastBeginFlushTime.set(index, flushBeginTime)
                  task.flush()
                  if (flushTimeMetric != null) {
                    val delta = System.nanoTime() - flushBeginTime
                    flushTimeMetric.update(delta)
                  }
                } catch {
                  case t: Throwable =>
                    t match {
                      case exception: IOException =>
                        task.notifier.setException(exception)
                        processIOException(
                          exception,
                          DiskStatus.READ_OR_WRITE_FAILURE)
                      case _ =>
                    }
                    logWarning(s"Flusher-$this-thread-$index encounter exception.", t)
                }
                lastBeginFlushTime.set(index, -1)
              }
              if (task.buffer != null) {
                Utils.tryLogNonFatalError(returnBuffer(task.buffer, task.keepBuffer))
              }
              task.notifier.numPendingFlushes.decrementAndGet()
            }
          }
        }
      })
      workerSource.addGauge(FLUSH_WORKING_QUEUE_SIZE, Map("mountpoint" -> s"$mountPoint-$index")) {
        () =>
          workingQueues(index).size()
      }
    }
    ThreadPoolSource.registerSource(s"$this", workers)
  }

  def getWorkerIndex: Int = synchronized {
    nextWorkerIndex = (nextWorkerIndex + 1) % threadCount
    nextWorkerIndex
  }

  def takeBuffer(): CompositeByteBuf = {
    var buffer = bufferQueue.poll()
    if (buffer == null) {
      buffer = allocator.compositeDirectBuffer(maxComponents)
    }
    buffer
  }

  def returnBuffer(buffer: CompositeByteBuf, keepBuffer: Boolean = false): Unit = {
    val bufferSize = buffer.readableBytes()
    MemoryManager.instance().releaseDiskBuffer(bufferSize)
    Option(CongestionController.instance())
      .foreach(
        _.consumeBytes(bufferSize))
    buffer.removeComponents(0, buffer.numComponents())
    buffer.clear()
    if (keepBuffer) {
      bufferQueue.put(buffer)
    } else {
      buffer.release()
    }
  }

  def addTask(task: FlushTask, timeoutMs: Long, workerIndex: Int): Boolean = {
    workingQueues(workerIndex).offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit
}

private[worker] class LocalFlusher(
    workerSource: AbstractSource,
    val deviceMonitor: DeviceMonitor,
    threadCount: Int,
    allocator: PooledByteBufAllocator,
    maxComponents: Int,
    val mountPoint: String,
    val diskType: StorageInfo.Type,
    timeWindow: TimeWindow) extends Flusher(
    workerSource,
    threadCount,
    allocator,
    maxComponents,
    timeWindow,
    mountPoint)
  with DeviceObserver with Logging {

  deviceMonitor.registerFlusher(this)

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    logError(s"$this write failed, report to DeviceMonitor, exception: $e")
    deviceMonitor.reportNonCriticalError(mountPoint, e, deviceErrorType)
  }

  override def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = {
    logError(s"$this is notified Disk $mountPoint $diskStatus! Won't stop LocalFlusher.")
  }

  override def hashCode(): Int = {
    mountPoint.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[LocalFlusher] &&
    obj.asInstanceOf[LocalFlusher].mountPoint.equals(mountPoint)
  }

  override def toString: String = s"LocalFlusher@$flusherId-$mountPoint"
}

final private[worker] class HdfsFlusher(
    workerSource: AbstractSource,
    hdfsFlusherThreads: Int,
    allocator: PooledByteBufAllocator,
    maxComponents: Int) extends Flusher(
    workerSource,
    hdfsFlusherThreads,
    allocator,
    maxComponents,
    null,
    "HDFS") with Logging {

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    logError(s"$this write failed, reason $deviceErrorType ,exception: $e")
  }

  override def toString: String = s"HdfsFlusher@$flusherId"
}

final private[worker] class S3Flusher(
    workerSource: AbstractSource,
    s3FlusherThreads: Int,
    allocator: PooledByteBufAllocator,
    maxComponents: Int) extends Flusher(
    workerSource,
    s3FlusherThreads,
    allocator,
    maxComponents,
    null,
    "S3") with Logging {

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    logError(s"$this write failed, reason $deviceErrorType ,exception: $e")
  }

  override def toString: String = s"s3Flusher@$flusherId"
}
