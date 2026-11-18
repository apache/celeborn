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
  def calSpecWorkerCnt(input: Input): Int = cal(input).targetSpecWorkerCnt

  def cal(input: Input): Result = {
    val scaleConf = new ScaleConf(conf, configService)
    import scaleConf._

    validateInput(input)
    val oldSpecWorkerCnt = input.specWorkerCnt
    val ratios = dimensions.map { dimension =>
      dimension -> input.workerStats.map(dimension.value)
    }
    // The metric inventory includes excluded workers, so this is not the available worker count.
    val metrics = MetricRatios(ratios, input.workerStats.size)
    val load = classifyLoad(metrics)
    val specPos = classifySpec(oldSpecWorkerCnt, minWorkers, maxWorkers)

    val rule: ScaleRule = (load, specPos) match {
      case (Load.High, SpecPos.LtMin | SpecPos.EqMin | SpecPos.InRange) =>
        ScaleRule.ScaleUpForLoad
      case (_, SpecPos.LtMin) if alignMin =>
        ScaleRule.AlignToMin
      case (Load.Low, _) =>
        ScaleRule.ScaleDownForLoad
      case (_, SpecPos.GtMax) =>
        ScaleRule.ConvergeToMax
      case _ =>
        ScaleRule.KeepSpec
    }

    val candidate = rule match {
      case ScaleRule.ScaleUpForLoad | ScaleRule.AlignToMin =>
        calUpCandidate(oldSpecWorkerCnt, metrics, load, scaleConf)
      case ScaleRule.ScaleDownForLoad =>
        calDownCandidate(input, downDemand(metrics, scaleConf), scaleConf)
      case ScaleRule.ConvergeToMax =>
        calDownCandidate(input, downDemand(metrics, scaleConf), scaleConf)
      case ScaleRule.KeepSpec =>
        SpecCandidate(oldSpecWorkerCnt, alignedMin, ExcludedCompensation.UnaccountedOnly)
    }

    compensateExcluded(input, candidate, maxWorkers)
  }

  private def validateInput(input: Input): Unit = {
    require(input.registeredWorkerCnt >= 0, "Registered worker count must be non-negative")
    require(input.specWorkerCnt >= 0, "Platform spec worker count must be non-negative")
    require(
      input.workerStats.size <= input.registeredWorkerCnt,
      "Worker metrics must not exceed the registered worker count")
    require(
      input.excludedWorkerCnt >= 0 && input.excludedWorkerCnt <= input.workerStats.size,
      "Excluded worker count must be within the worker metric inventory")
    require(
      input.unaccountedExcludedWorkerCnt >= 0 &&
        input.unaccountedExcludedWorkerCnt <= input.excludedWorkerCnt,
      "Unaccounted excluded worker count must be between zero and total excluded worker count")
  }

  private def classifyLoad(metrics: MetricRatios): Load = {
    val ratios = metrics.values.iterator
    var allBelowDown = true

    while (ratios.hasNext) {
      val (dimension, values) = ratios.next()
      val avg = values.sum / metrics.registeredWorkerWithMetricCnt

      if (avg > dimension.up) {
        return Load.High
      }
      allBelowDown = allBelowDown && avg < dimension.down
    }

    if (allBelowDown) Load.Low else Load.Normal
  }

  private def classifySpec(specWorkerCnt: Int, minWorkers: Int, maxWorkers: Int): SpecPos = {
    if (specWorkerCnt > maxWorkers) SpecPos.GtMax
    else if (specWorkerCnt == maxWorkers) SpecPos.EqMax
    else if (specWorkerCnt < minWorkers) SpecPos.LtMin
    else if (specWorkerCnt == minWorkers) SpecPos.EqMin
    else SpecPos.InRange
  }

  private def calUpCandidate(
      oldSpecWorkerCnt: Int,
      metrics: MetricRatios,
      load: Load,
      scaleConf: ScaleConf): SpecCandidate = {
    import scaleConf._
    val registeredWorkerWithMetricCnt = metrics.registeredWorkerWithMetricCnt
    val requiredWorkerCnt =
      math.max(alignedMin, math.min(maxWorkers, estimate(metrics, up = true)))
    val stepBudget =
      math.max(3, math.max(upStep, (upPercent * registeredWorkerWithMetricCnt).toInt))
    val deltaUp =
      math.min(stepBudget, math.max(0, requiredWorkerCnt - registeredWorkerWithMetricCnt))

    if (deltaUp > 0) {
      SpecCandidate(
        math.max(alignedMin, registeredWorkerWithMetricCnt + deltaUp),
        alignedMin,
        ExcludedCompensation.AllExcluded)
    } else if (load == Load.High && requiredWorkerCnt > oldSpecWorkerCnt) {
      SpecCandidate(requiredWorkerCnt, alignedMin, ExcludedCompensation.AllExcluded)
    } else {
      SpecCandidate(
        math.max(alignedMin, oldSpecWorkerCnt),
        alignedMin,
        ExcludedCompensation.UnaccountedOnly)
    }
  }

  private def downDemand(metrics: MetricRatios, scaleConf: ScaleConf): Int = {
    import scaleConf._
    math.max(minWorkers, math.min(maxWorkers, estimate(metrics, up = false)))
  }

  private def calDownCandidate(
      input: Input,
      requiredWorkerCnt: Int,
      scaleConf: ScaleConf): SpecCandidate = {
    import scaleConf._
    val stepBudget =
      math.max(1, math.min(downStep, (downPercent * input.registeredWorkerCnt).toInt))
    val deltaDown =
      math.min(stepBudget, math.max(0, input.registeredWorkerCnt - requiredWorkerCnt))
    val candidateSpecWorkerCnt = math.max(requiredWorkerCnt, input.registeredWorkerCnt - deltaDown)
    SpecCandidate(
      math.min(input.specWorkerCnt, candidateSpecWorkerCnt),
      minWorkers,
      ExcludedCompensation.UnaccountedOnly)
  }

  private def estimate(metrics: MetricRatios, up: Boolean): Int = {
    metrics.values.map { case (dimension, values) =>
      val threshold = if (up) dimension.up else dimension.down
      math.ceil(values.sum / threshold).toInt
    }.max
  }

  private def compensateExcluded(
      input: Input,
      candidate: SpecCandidate,
      maxWorkers: Int): Result = {
    // Excluded workers still occupy platform instances but cannot accept new allocations.
    val carriedReplacementCnt = candidate.excludedCompensation match {
      case ExcludedCompensation.AllExcluded => 0
      case ExcludedCompensation.UnaccountedOnly =>
        input.excludedWorkerCnt - input.unaccountedExcludedWorkerCnt
    }
    val requestedReplacementCnt = input.excludedWorkerCnt - carriedReplacementCnt
    val replacementHeadroom = math.max(0, maxWorkers - candidate.specWorkerCnt)
    val addedReplacementCnt = math.min(requestedReplacementCnt, replacementHeadroom)
    val floor = math.min(
      maxWorkers.toLong,
      candidate.minWorkerCnt.toLong + input.excludedWorkerCnt).toInt
    val targetSpecWorkerCnt = math.max(candidate.specWorkerCnt + addedReplacementCnt, floor)
    val specWithoutReplacements =
      math.max(candidate.minWorkerCnt, candidate.specWorkerCnt - carriedReplacementCnt)
    val accountedExcludedWorkerCnt =
      math.min(input.excludedWorkerCnt, targetSpecWorkerCnt - specWithoutReplacements)
    Result(targetSpecWorkerCnt, accountedExcludedWorkerCnt)
  }
}

private[master] object SpecWorkerCntCalculator {
  private case class MetricRatios(
      values: Seq[(Dimension, Seq[Double])],
      registeredWorkerWithMetricCnt: Int)

  private case class Dimension(key: String, up: Double, down: Double) {
    require(down < up, s"Scale-down threshold must be below scale-up threshold for $key")

    def value(stats: WorkerStats): Double = {
      val ratio = stats.metrics.getOrElse(
        key,
        throw new IllegalArgumentException(s"Missing worker metric: $key"))
      require(
        !ratio.isInfinity && (ratio.isNaN || ratio >= 0),
        s"Worker metric $key must be finite and non-negative, or NaN")
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
    val alignMin: Boolean = get(CelebornConf.SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED)
    val alignedMin: Int = if (alignMin) minWorkers else 0
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
   * @param unaccountedExcludedWorkerCnt excluded workers whose replacements are still due, including
   *                                     those capped in earlier rounds; the caller updates its
   *                                     checkpoint after accepting the result's target
   */
  case class Input(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCnt: Int = 0,
      unaccountedExcludedWorkerCnt: Int = 0)

  case class Result(targetSpecWorkerCnt: Int, accountedExcludedWorkerCnt: Int)
}

sealed private trait Load

private object Load {
  case object High extends Load
  case object Low extends Load
  case object Normal extends Load
}

sealed private trait SpecPos
private object SpecPos {
  case object LtMin extends SpecPos
  case object EqMin extends SpecPos
  case object InRange extends SpecPos
  case object EqMax extends SpecPos
  case object GtMax extends SpecPos
}

sealed private trait ScaleRule
private object ScaleRule {
  case object ScaleUpForLoad extends ScaleRule
  case object ScaleDownForLoad extends ScaleRule
  case object AlignToMin extends ScaleRule
  case object ConvergeToMax extends ScaleRule
  case object KeepSpec extends ScaleRule
}

sealed private trait ExcludedCompensation

private object ExcludedCompensation {
  case object AllExcluded extends ExcludedCompensation
  case object UnaccountedOnly extends ExcludedCompensation
}

final private case class SpecCandidate(
    specWorkerCnt: Int,
    minWorkerCnt: Int,
    excludedCompensation: ExcludedCompensation)
