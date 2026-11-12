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

/** Computes a new platform spec worker count without changing worker or platform state. */
private[master] class SpecWorkerCntCalculator(
    conf: CelebornConf,
    configService: Option[ConfigService] = None) {
  import SpecWorkerCntCalculator._

  /**
   * Calculates the next platform spec count. A spec change and the gap to registered workers
   * can have opposite signs; worker lifecycle reconciliation belongs to the caller.
   */
  def calculateSpecWorkerCnt(input: Input): Int = {
    val scaleConf = new ScaleConf(conf, configService)
    import scaleConf._

    val oldSpecWorkerCnt = input.specWorkerCnt

    require(input.registeredWorkerCnt >= 0, "Registered worker count must be non-negative")
    require(oldSpecWorkerCnt >= 0, "Platform spec worker count must be non-negative")
    require(
      input.workerStats.size <= input.registeredWorkerCnt,
      "Worker metrics must not exceed the registered worker count")
    require(
      input.excludedWorkerCnt >= 0 && input.excludedWorkerCnt <= input.workerStats.size,
      "Excluded worker count must be within the worker metric inventory")
    require(
      input.newExcludedWorkerCnt >= 0 &&
        input.newExcludedWorkerCnt <= input.excludedWorkerCnt,
      "New excluded worker count must be between zero and total excluded worker count")

    val ratios = dimensions.map { dimension =>
      input.workerStats.map(dimension.value)
    }
    // The metric inventory includes excluded workers, so this is not the available worker count.
    val registeredWorkerWithMetricCnt = input.workerStats.size
    val alignedMinimum = if (alignMinimum) minWorkers else 0

    def estimate(up: Boolean): Int = {
      dimensions.zip(ratios).map { case (dimension, values) =>
        val threshold = if (up) dimension.up else dimension.down
        math.ceil(values.sum / threshold).toInt
      }.max
    }

    def compensateExcludedWorkers(candidateSpecWorkerCnt: Int, minimum: Int): Int = {
      // Excluded workers still occupy platform instances but cannot accept new allocations.
      val compensatedSpecWorkerCnt =
        if (input.newExcludedWorkerCnt > 0) {
          math.min(maxWorkers.toLong, candidateSpecWorkerCnt.toLong + input.newExcludedWorkerCnt)
        } else {
          candidateSpecWorkerCnt.toLong
        }
      val floor = math.min(maxWorkers.toLong, minimum.toLong + input.excludedWorkerCnt)
      math.max(compensatedSpecWorkerCnt, floor).toInt
    }

    val upThresholdExceeded = dimensions.zip(ratios).exists { case (dimension, values) =>
      values.sum / registeredWorkerWithMetricCnt > dimension.up
    }
    val allBelowDownThresholds = dimensions.zip(ratios).forall { case (dimension, values) =>
      values.sum / registeredWorkerWithMetricCnt < dimension.down
    }

    def computeWithUpThreshold(): Int = {
      val requiredWorkerCnt = math.max(alignedMinimum, math.min(maxWorkers, estimate(up = true)))
      val stepBudget =
        math.max(3, math.max(upStep, (upPercent * registeredWorkerWithMetricCnt).toInt))
      val deltaUp =
        math.min(stepBudget, math.max(0, requiredWorkerCnt - registeredWorkerWithMetricCnt))
      val candidateSpecWorkerCnt =
        if (deltaUp > 0) registeredWorkerWithMetricCnt + deltaUp else oldSpecWorkerCnt
      math.max(alignedMinimum, candidateSpecWorkerCnt)
    }

    def computeWithDownThreshold(): Int = {
      val requiredWorkerCnt = math.max(minWorkers, math.min(maxWorkers, estimate(up = false)))
      // Apply the budget to the platform target while registration or draining is still in flight.
      val stepBudget = math.max(1, math.min(downStep, (downPercent * oldSpecWorkerCnt).toInt))
      val deltaDown = math.min(stepBudget, math.max(0, oldSpecWorkerCnt - requiredWorkerCnt))
      oldSpecWorkerCnt - deltaDown
    }

    val useUpThreshold = oldSpecWorkerCnt < maxWorkers &&
      (upThresholdExceeded || oldSpecWorkerCnt < alignedMinimum)
    val useDownThreshold = allBelowDownThresholds || oldSpecWorkerCnt > maxWorkers
    val (candidateSpecWorkerCnt, minimumWorkerCnt) =
      if (useUpThreshold) {
        (computeWithUpThreshold(), alignedMinimum)
      } else if (useDownThreshold) {
        (computeWithDownThreshold(), minWorkers)
      } else {
        (oldSpecWorkerCnt, alignedMinimum)
      }
    val newSpecWorkerCnt = compensateExcludedWorkers(candidateSpecWorkerCnt, minimumWorkerCnt)
    newSpecWorkerCnt
  }
}

private[master] object SpecWorkerCntCalculator {
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
        WorkerStats.MemoryPressureRatio,
        get(CelebornConf.SCALE_UP_MEMORY_PRESSURE_RATIO),
        get(CelebornConf.SCALE_DOWN_MEMORY_PRESSURE_RATIO)),
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
   * @param registeredWorkerCnt workers registered with Master, including Idle, automatically
   *                            decommissioning and excluded workers; not a platform replica count
   * @param specWorkerCnt platform spec worker count (for example, Kubernetes spec.replicas): the
   *                      desired total instance count, including pending and excluded workers
   * @param workerStats metrics used to estimate load, excluding Idle and automatically
   *                    decommissioning workers but including excluded workers and their existing load
   * @param excludedWorkerCnt total excluded workers in the metric inventory; these still occupy
   *                          platform instances but cannot accept new allocations
   * @param newExcludedWorkerCnt excluded workers in the metric inventory whose replacement has not
   *                             yet been accounted for in specWorkerCnt; the caller advances its
   *                             checkpoint after the new spec worker count is accepted
   */
  case class Input(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCnt: Int = 0,
      newExcludedWorkerCnt: Int = 0)
}
