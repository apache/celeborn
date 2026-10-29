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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.config.ConfigEntry
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.server.common.service.config.ConfigService

/** Computes a worker count without changing worker or platform state. */
private[master] class WorkerNumCalculator(
    conf: CelebornConf,
    configService: Option[ConfigService] = None) {
  import WorkerNumCalculator._

  def cal(input: Input): Int = {
    val scaleConf = new ScaleConf(conf, configService)
    import scaleConf._

    require(input.currentWorkerCount >= 0, "Current worker count must be non-negative")
    require(input.expectedWorkerCount >= 0, "Expected worker count must be non-negative")
    require(
      input.workerStats.size <= input.currentWorkerCount,
      "Worker metrics must not exceed the registered worker count")
    require(
      input.excludedWorkerCount >= 0 && input.excludedWorkerCount <= input.workerStats.size,
      "Excluded worker count must be within the worker metric inventory")
    require(
      input.newlyExcludedWorkerCount >= 0 &&
        input.newlyExcludedWorkerCount <= input.excludedWorkerCount,
      "Newly excluded worker count must be between zero and total excluded worker count")

    val ratios = dimensions.map { dimension =>
      input.workerStats.map(dimension.value)
    }
    val normalWorkers = input.workerStats.size
    val alignedMinimum =
      if (alignMinimum && minWorkers > input.expectedWorkerCount) minWorkers else 0

    def estimate(up: Boolean): Int = {
      dimensions.zip(ratios).map { case (dimension, values) =>
        val threshold = if (up) dimension.up else dimension.down
        math.ceil(values.sum / threshold).toInt
      }.max
    }

    def compensate(computed: Int, minimum: Int): Int = {
      val result =
        if (input.newlyExcludedWorkerCount > 0) {
          math.min(maxWorkers.toLong, computed.toLong + input.newlyExcludedWorkerCount)
        } else {
          computed.toLong
        }
      val floor = math.min(maxWorkers.toLong, minimum.toLong + input.excludedWorkerCount)
      math.max(result, floor).toInt
    }

    val scaleUp = dimensions.zip(ratios).exists { case (dimension, values) =>
      values.sum / normalWorkers > dimension.up
    }
    val scaleDown = dimensions.zip(ratios).forall { case (dimension, values) =>
      values.sum / normalWorkers < dimension.down
    }

    if ((scaleUp || alignedMinimum > input.expectedWorkerCount) &&
      maxWorkers > input.expectedWorkerCount) {
      val desired = math.max(alignedMinimum, math.min(maxWorkers, estimate(up = true)))
      val budget = math.max(3, math.max(upStep, (upPercent * normalWorkers).toInt))
      val increment = math.min(desired - normalWorkers, budget)
      val candidate = math.max(alignedMinimum, normalWorkers + increment)
      val computed =
        if (increment > 0) {
          candidate
        } else {
          math.max(input.expectedWorkerCount, alignedMinimum)
        }
      compensate(computed, alignedMinimum)
    } else if (scaleDown || maxWorkers < input.expectedWorkerCount) {
      val desired = math.max(minWorkers, math.min(maxWorkers, estimate(up = false)))
      val difference = input.currentWorkerCount - desired
      val decrement =
        if (difference > 0) {
          math.max(
            1,
            math.min(
              difference,
              math.min(downStep, (downPercent * input.currentWorkerCount).toInt)))
        } else {
          0
        }
      val candidate = input.currentWorkerCount - decrement
      val computed =
        if (decrement > 0) {
          candidate
        } else {
          input.expectedWorkerCount
        }
      compensate(computed, minWorkers)
    } else {
      compensate(input.expectedWorkerCount, alignedMinimum)
    }
  }
}

private[master] object WorkerNumCalculator {
  private case class Dimension(key: String, up: Double, down: Double) {
    require(down < up, s"Scale-down threshold must be below scale-up threshold for $key")

    def value(stats: WorkerStats): Double = {
      val ratio = stats.metrics.getOrElse(
        key,
        throw new IllegalArgumentException(s"Missing worker metric: $key"))
      require(
        !ratio.isNaN && !ratio.isInfinity && ratio >= 0,
        s"Worker metric $key must be finite and non-negative")
      ratio
    }
  }

  private class ScaleConf(defaults: CelebornConf, configService: Option[ConfigService]) {
    // FS refresh replaces the map on the existing SystemConfig object. Capture its contents once.
    private val overrides = configService
      .map(_.getSystemConfigFromCache.getConfigs.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    private def get[T](entry: ConfigEntry[T]): T = {
      overrides.get(entry.key).map(entry.valueConverter).getOrElse(defaults.get(entry))
    }

    val minWorkers: Int = get(CelebornConf.SCALE_WORKER_NUMBER_MIN)
    val maxWorkers: Int = get(CelebornConf.SCALE_WORKER_NUMBER_MAX)
    val alignMinimum: Boolean = get(CelebornConf.SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED)
    val upStep: Int = get(CelebornConf.SCALE_UP_POLICY_STEP_NUMBER)
    val downStep: Int = get(CelebornConf.SCALE_DOWN_POLICY_STEP_NUMBER)
    val upPercent: Double = get(CelebornConf.SCALE_UP_POLICY_PERCENT)
    val downPercent: Double = get(CelebornConf.SCALE_DOWN_POLICY_PERCENT)

    val dimensions: Seq[Dimension] = Seq(
      Dimension(
        WorkerStats.NettyMemoryUsedRatio,
        get(CelebornConf.SCALE_UP_MEMORY_RATIO),
        get(CelebornConf.SCALE_DOWN_MEMORY_RATIO)),
      Dimension(
        WorkerStats.LastMinuteSystemLoadRatio,
        get(CelebornConf.SCALE_UP_SYSTEM_LOAD_RATIO),
        get(CelebornConf.SCALE_DOWN_SYSTEM_LOAD_RATIO)),
      Dimension(
        WorkerStats.DiskUsedRatio,
        get(CelebornConf.SCALE_UP_DISK_RATIO),
        get(CelebornConf.SCALE_DOWN_DISK_RATIO)))

    require(minWorkers <= maxWorkers, "Minimum worker count must not exceed maximum worker count")
  }

  /**
   * currentWorkerCount includes registered Idle and automatically decommissioning workers in the
   * platform's expected scope; expectedWorkerCount is the platform's current desired count.
   * workerStats contains the remaining workers, including any excluded workers. Exclusion counts
   * refer to this metric inventory. The caller retains the exclusion list and compensation
   * checkpoint across rounds, and supplies only exclusions not yet compensated as newly excluded.
   */
  case class Input(
      currentWorkerCount: Int,
      expectedWorkerCount: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCount: Int = 0,
      newlyExcludedWorkerCount: Int = 0)
}
