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

package org.apache.celeborn.service.deploy.worker.metrics

import java.lang.management.OperatingSystemMXBean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskInfo, DiskStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class DefaultScaleMetricCollectorSuite extends AnyFunSuite {
  private class Fixture(windowSize: Int) {
    val worker = mock[Worker]
    val memory = mock[MemoryManager]
    val storage = mock[StorageManager]
    val os = mock[OperatingSystemMXBean]
    val conf = new CelebornConf()
      .set(CelebornConf.SCALE_METRIC_WINDOW_SIZE, windowSize)
    val collector = new DefaultScaleMetricCollector(conf, os)
    val disk = new DiskInfo("/tmp", 512L, 0L, 0L, 0L)
    disk.setTotalSpace(1024L)
    when(worker.memoryManager).thenReturn(memory)
    when(worker.storageManager).thenReturn(storage)
    memory.maxDirectMemory = 1024L
    when(memory.getMemoryUsage).thenReturn(256L)
    when(storage.localDisksSnapshot()).thenReturn(List(disk))
    when(os.getAvailableProcessors).thenReturn(4)
    when(os.getSystemLoadAverage).thenReturn(2.0)

    def collect(): Map[String, Double] = {
      collector.collectOnce(worker)
      collector.currentWorkerStats().get.metrics
    }
  }

  private def withFixture(windowSize: Int = 1)(f: Fixture => Unit): Unit = {
    val fixture = new Fixture(windowSize)
    try f(fixture)
    finally fixture.collector.stop()
  }

  test("collects memory, normalized system load and aggregated disk capacity") {
    withFixture() { f =>
      val second = new DiskInfo("/data", 256L, 0L, 0L, 0L)
      second.setTotalSpace(1024L)
      when(f.storage.localDisksSnapshot()).thenReturn(List(f.disk, second))
      assert(f.collect() == Map(
        WorkerStats.NettyMemoryUsedRatio -> 0.25,
        WorkerStats.LastMinuteSystemLoadRatio -> 0.5,
        WorkerStats.DiskUsedRatio -> 0.625,
        WorkerStats.DiskRemainingSize -> 768.0))
    }
  }

  test("remote storage does not affect local disk metrics") {
    withFixture() { f =>
      val remote = new DiskInfo("HDFS", Long.MaxValue, 0L, 0L, 0L, StorageInfo.Type.HDFS)
      when(f.storage.allDisksSnapshot()).thenReturn(List(f.disk, remote))
      val metrics = f.collect()
      assert(metrics(WorkerStats.DiskUsedRatio) == 0.5)
      assert(metrics(WorkerStats.DiskRemainingSize) == 512.0)
    }
  }

  test("IO_HANG disks retain their total capacity but contribute no usable space") {
    withFixture() { f =>
      val hung = new DiskInfo("/hung", Long.MaxValue, 0L, 0L, 0L)
      hung.setTotalSpace(1024L).setStatus(DiskStatus.IO_HANG)
      when(f.storage.localDisksSnapshot()).thenReturn(List(f.disk, hung))
      val partial = f.collect()
      assert(partial(WorkerStats.DiskUsedRatio) == 0.75)
      assert(partial(WorkerStats.DiskRemainingSize) == 512.0)

      f.disk.setStatus(DiskStatus.IO_HANG)
      val unavailable = f.collect()
      assert(unavailable(WorkerStats.DiskUsedRatio) == 1.0)
      assert(unavailable(WorkerStats.DiskRemainingSize) == 0.0)

      f.disk.setStatus(DiskStatus.HEALTHY)
      hung.setUsableSpace(256L).setStatus(DiskStatus.HEALTHY)
      val recovered = f.collect()
      assert(recovered(WorkerStats.DiskUsedRatio) == 0.625)
      assert(recovered(WorkerStats.DiskRemainingSize) == 768.0)
    }
  }

  test("reports zero until the memory window fills, then reports the sliding average") {
    withFixture(2) { f =>
      when(f.memory.getMemoryUsage).thenReturn(512L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.0)
      when(f.memory.getMemoryUsage).thenReturn(1024L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.75)
      when(f.memory.getMemoryUsage).thenReturn(0L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.5)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.0)
    }
  }

  test("reports raw ratios for zero memory capacity, negative load and no local disks") {
    withFixture() { f =>
      f.memory.maxDirectMemory = 0
      when(f.os.getSystemLoadAverage).thenReturn(-1.0)
      when(f.storage.localDisksSnapshot()).thenReturn(List.empty)
      val metrics = f.collect()
      assert(metrics.keySet == Set(
        WorkerStats.NettyMemoryUsedRatio,
        WorkerStats.LastMinuteSystemLoadRatio,
        WorkerStats.DiskUsedRatio,
        WorkerStats.DiskRemainingSize))
      assert(metrics(WorkerStats.NettyMemoryUsedRatio) == Double.PositiveInfinity)
      assert(metrics(WorkerStats.LastMinuteSystemLoadRatio) == -0.25)
      assert(metrics(WorkerStats.DiskUsedRatio).isNaN)
      assert(metrics(WorkerStats.DiskRemainingSize) == 0.0)
    }
  }

  test("reports disk metrics even when usable space exceeds a disk's capacity") {
    withFixture() { f =>
      val badDisk = new DiskInfo("/bad", 256L, 0L, 0L, 0L)
      badDisk.setTotalSpace(128L)
      when(f.storage.localDisksSnapshot()).thenReturn(List(f.disk, badDisk))
      val metrics = f.collect()
      assert(math.abs(metrics(WorkerStats.DiskUsedRatio) - 1.0 / 3) < 1e-12)
      assert(metrics(WorkerStats.DiskRemainingSize) == 768.0)
    }
  }

  test("zero memory capacity and negative samples do not reset the memory window") {
    withFixture(2) { f =>
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.0)
      f.memory.maxDirectMemory = 0
      when(f.memory.getMemoryUsage).thenReturn(512L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == Double.PositiveInfinity)
      f.memory.maxDirectMemory = 1024L
      when(f.memory.getMemoryUsage).thenReturn(0L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.25)
      when(f.memory.getMemoryUsage).thenReturn(-256L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == -0.125)
    }
  }

  test("the first background sample waits for the configured metric interval") {
    withFixture() { f =>
      f.conf.set(CelebornConf.SCALE_METRIC_INTERVAL, 1000L)
      val firstSample = new CountDownLatch(1)
      val sampledAt = new AtomicLong()
      when(f.memory.getMemoryUsage).thenAnswer {
        sampledAt.compareAndSet(0L, System.nanoTime())
        firstSample.countDown()
        256L
      }
      val startedAt = System.nanoTime()
      f.collector.init(f.worker)
      assert(firstSample.await(10, TimeUnit.SECONDS))
      assert(sampledAt.get() - startedAt >=
        TimeUnit.MILLISECONDS.toNanos(f.conf.scaleMetricInterval))
    }
  }

  test("a failed collection preserves the last snapshot and memory window samples") {
    withFixture(2) { f =>
      f.collect()
      when(f.memory.getMemoryUsage).thenReturn(512L)
      assert(f.collect()(WorkerStats.NettyMemoryUsedRatio) == 0.375)
      val cached = f.collector.currentWorkerStats()
      when(f.memory.getMemoryUsage).thenReturn(768L)
      when(f.os.getSystemLoadAverage).thenReturn(6.0)
      when(f.storage.localDisksSnapshot()).thenThrow(new IllegalStateException(
        "unavailable disk stats"))
      f.collector.collectOnce(f.worker)
      assert(f.collector.currentWorkerStats() == cached)
      doReturn(List(f.disk)).when(f.storage).localDisksSnapshot()
      when(f.memory.getMemoryUsage).thenReturn(1024L)
      val recovered = f.collect()
      assert(recovered(WorkerStats.NettyMemoryUsedRatio) == 0.875)
      assert(recovered(WorkerStats.LastMinuteSystemLoadRatio) == 1.5)
    }
  }

  test("returns the cached snapshot until the collector stops") {
    withFixture() { f =>
      f.collect()
      val cached = f.collector.currentWorkerStats()
      assert(cached.isDefined)
      assert(f.collector.currentWorkerStats() == cached)
      f.collector.stop()
      assert(f.collector.currentWorkerStats().isEmpty)
    }
  }

  test("a full memory window does not overflow when summing long samples") {
    val window = new MetricSlidingWindow(2)
    window.update(Long.MaxValue)
    window.update(Long.MaxValue)
    assert(window.average == Long.MaxValue.toDouble)
  }
}
