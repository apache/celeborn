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

import org.apache.celeborn.common.util.DiskUtils
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.metrics.ScaleMetricCollectorContext.DiskUsage
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

/** Read-only resource observations supplied to a collector when the worker initializes it. */
final class ScaleMetricCollectorContext private[worker] (
    memoryManager: MemoryManager,
    storageManager: StorageManager) {

  def maxDirectMemory: Long = memoryManager.maxDirectMemory

  /** Current Netty direct memory usage plus memory reserved for sorting. */
  def memoryUsage: Long = memoryManager.getMemoryUsage

  /** Current local disk capacity, accounting for configured limits, reserves and disk health. */
  def localDiskUsage(): DiskUsage = storageManager.synchronized {
    // Share the updateDiskInfos monitor so usable space and capacity come from a complete refresh.
    val disks = storageManager.localDisksSnapshot()
    val usableBytes = disks.filter(_.isHealthy).map(_.actualUsableSpace.toDouble).sum
    val effectiveCapacityBytes = disks.map { disk =>
      val reserveSize = DiskUtils.getActualReserveSize(
        disk.totalSpace,
        storageManager.diskReserveSize,
        storageManager.diskReserveRatio)
      math.min(disk.configuredUsableSpace, math.max(disk.totalSpace - reserveSize, 0L)).toDouble
    }.sum
    DiskUsage(usableBytes, effectiveCapacityBytes)
  }
}

object ScaleMetricCollectorContext {

  /** Unhealthy disks contribute capacity but no usable space. */
  final case class DiskUsage(usableBytes: Double, effectiveCapacityBytes: Double)
}
