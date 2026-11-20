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

import java.math.{BigDecimal, RoundingMode}

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
    val metrics = MetricRatios(ratios, input.workerStats.size - input.exclWorkerCnt)
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

    rule match {
      case ScaleRule.ScaleUpForLoad | ScaleRule.AlignToMin =>
        makupExcl(
          input,
          calUpCandidate(oldSpecWorkerCnt, metrics, load, scaleConf),
          maxWorkers)
      case ScaleRule.ScaleDownForLoad =>
        calDownForLoad(input, downDemand(metrics, scaleConf), scaleConf)
      case ScaleRule.ConvergeToMax =>
        calConvergeToMax(input, scaleConf)
      case ScaleRule.KeepSpec =>
        makupExcl(
          input,
          SpecCandidate(oldSpecWorkerCnt, alignedMin, ExclMakup.UnmetOnly),
          maxWorkers)
    }
  }

  private def validateInput(input: Input): Unit = {
    require(input.registeredWorkerCnt >= 0, "Registered worker count must be non-negative")
    require(input.specWorkerCnt >= 0, "Platform spec worker count must be non-negative")
    require(
      input.workerStats.size <= input.registeredWorkerCnt,
      "Worker metrics must not exceed the registered worker count")
    require(
      input.exclWorkerCnt >= 0 && input.exclWorkerCnt <= input.workerStats.size,
      "Excluded worker count must be within the worker metric inventory")
    require(
      input.unaccountedExclWorkerCnt >= 0 &&
        input.unaccountedExclWorkerCnt <= input.exclWorkerCnt,
      "Unaccounted excluded worker count must be between zero and total excluded worker count")
  }

  private def classifyLoad(metrics: MetricRatios): Load = {
    val availWorkerCnt = BigDecimal.valueOf(metrics.availWorkerWithMetricCnt.toLong)
    val ratios = metrics.values.iterator
    var allBelowDown = true

    while (ratios.hasNext) {
      val (dimension, values) = ratios.next()
      sumRatios(values) match {
        case Some(sum) =>
          val up = BigDecimal.valueOf(dimension.up).multiply(availWorkerCnt)
          if (sum.compareTo(up) > 0) {
            return Load.High
          }
          allBelowDown = allBelowDown &&
            sum.compareTo(BigDecimal.valueOf(dimension.down).multiply(availWorkerCnt)) < 0
        case None =>
          allBelowDown = false
      }
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
    val availWorkerWithMetricCnt = metrics.availWorkerWithMetricCnt
    val requiredWorkerCnt =
      math.max(alignedMin, math.min(maxWorkers, estimate(metrics, up = true)))
    val stepBudget =
      math.max(3, math.max(upStep, (upPercent * availWorkerWithMetricCnt).toInt))
    val deltaUp =
      math.min(stepBudget, math.max(0, requiredWorkerCnt - availWorkerWithMetricCnt))

    if (deltaUp > 0) {
      SpecCandidate(
        math.max(alignedMin, availWorkerWithMetricCnt + deltaUp),
        alignedMin,
        ExclMakup.AllExcl)
    } else if (load == Load.High && requiredWorkerCnt > oldSpecWorkerCnt) {
      SpecCandidate(requiredWorkerCnt, alignedMin, ExclMakup.AllExcl)
    } else {
      SpecCandidate(
        math.max(alignedMin, oldSpecWorkerCnt),
        alignedMin,
        ExclMakup.UnmetOnly)
    }
  }

  private def downDemand(metrics: MetricRatios, scaleConf: ScaleConf): Int = {
    import scaleConf._
    math.max(minWorkers, math.min(maxWorkers, estimate(metrics, up = false)))
  }

  private def calDownForLoad(
      input: Input,
      requiredAvailWorkerCnt: Int,
      scaleConf: ScaleConf): Result = {
    import scaleConf._
    val desiredTotalSpec =
      math.min(maxWorkers.toLong, requiredAvailWorkerCnt.toLong + input.exclWorkerCnt).toInt
    val targetSpecWorkerCnt = calRetireTarget(input, desiredTotalSpec, scaleConf)
    val accountedExclWorkerCnt =
      math.min(
        input.exclWorkerCnt,
        math.max(0, targetSpecWorkerCnt - requiredAvailWorkerCnt))
    Result(targetSpecWorkerCnt, accountedExclWorkerCnt)
  }

  private def calConvergeToMax(input: Input, scaleConf: ScaleConf): Result = {
    import scaleConf._
    val targetSpecWorkerCnt = calRetireTarget(input, maxWorkers, scaleConf)
    val carriedReplacementCnt = input.exclWorkerCnt - input.unaccountedExclWorkerCnt
    val specWithoutReplacements =
      math.max(alignedMin, input.specWorkerCnt - carriedReplacementCnt)
    val accountedExclWorkerCnt =
      math.min(input.exclWorkerCnt, math.max(0, targetSpecWorkerCnt - specWithoutReplacements))
    Result(targetSpecWorkerCnt, accountedExclWorkerCnt)
  }

  private def calRetireTarget(
      input: Input,
      desiredTotalSpec: Int,
      scaleConf: ScaleConf): Int = {
    import scaleConf._
    val stepBudget =
      math.max(1, math.min(downStep, (downPercent * input.registeredWorkerCnt).toInt))
    val deltaDown =
      math.min(stepBudget, math.max(0, input.registeredWorkerCnt - desiredTotalSpec))
    val candidateSpecWorkerCnt = input.registeredWorkerCnt - deltaDown
    math.max(desiredTotalSpec, math.min(input.specWorkerCnt, candidateSpecWorkerCnt))
  }

  private def estimate(metrics: MetricRatios, up: Boolean): Int = {
    val maxInt = BigDecimal.valueOf(Int.MaxValue.toLong)
    metrics.values.map { case (dimension, values) =>
      val threshold = BigDecimal.valueOf(if (up) dimension.up else dimension.down)
      sumRatios(values).fold(0) { sum =>
        sum.divide(threshold, 0, RoundingMode.CEILING).min(maxInt).intValueExact()
      }
    }.max
  }

  private def sumRatios(values: Seq[Double]): Option[BigDecimal] = {
    var total = BigDecimal.ZERO
    var available = true
    values.foreach { value =>
      if (value.isNaN) {
        available = false
      } else if (available) {
        total = total.add(BigDecimal.valueOf(value))
      }
    }
    if (available) Some(total) else None
  }

  private def makupExcl(
      input: Input,
      candidate: SpecCandidate,
      maxWorkers: Int): Result = {
    // Excluded workers still occupy platform instances but cannot accept new allocations.
    val carriedReplacementCnt = candidate.exclMakup match {
      case ExclMakup.AllExcl => 0
      case ExclMakup.UnmetOnly =>
        input.exclWorkerCnt - input.unaccountedExclWorkerCnt
    }
    val requestedReplacementCnt = input.exclWorkerCnt - carriedReplacementCnt
    val replacementHeadroom = math.max(0, maxWorkers - candidate.specWorkerCnt)
    val addedReplacementCnt = math.min(requestedReplacementCnt, replacementHeadroom)
    val floor = math.min(
      maxWorkers.toLong,
      candidate.minWorkerCnt.toLong + input.exclWorkerCnt).toInt
    val targetSpecWorkerCnt = math.max(candidate.specWorkerCnt + addedReplacementCnt, floor)
    val specWithoutReplacements =
      math.max(candidate.minWorkerCnt, candidate.specWorkerCnt - carriedReplacementCnt)
    val accountedExclWorkerCnt =
      math.min(input.exclWorkerCnt, targetSpecWorkerCnt - specWithoutReplacements)
    Result(targetSpecWorkerCnt, accountedExclWorkerCnt)
  }
}

private[master] object SpecWorkerCntCalculator {
  private case class MetricRatios(
      values: Seq[(Dimension, Seq[Double])],
      availWorkerWithMetricCnt: Int)

  private case class Dimension(key: String, up: Double, down: Double) {
    require(down < up, s"Scale-down threshold must be below scale-up threshold for $key")

    def value(stats: WorkerStats): Double = {
      val ratio = stats.metrics.getOrElse(
        key,
        throw new IllegalArgumentException(s"Missing worker metric: $key"))
      if (key == WorkerStats.LastMinuteSystemLoadRatio && ratio < 0 && !ratio.isInfinity) {
        Double.NaN
      } else {
        require(
          !ratio.isInfinity && (ratio.isNaN || ratio >= 0),
          s"Worker metric $key must be finite and non-negative, or NaN")
        ratio
      }
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
   * @param exclWorkerCnt total excluded workers in the metric inventory; these still occupy
   *                          platform instances but cannot accept new allocations
   * @param unaccountedExclWorkerCnt excluded workers whose replacements are still due, including
   *                                     those capped in earlier rounds; the caller updates its
   *                                     checkpoint after accepting the result's target
   */
  case class Input(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      exclWorkerCnt: Int = 0,
      unaccountedExclWorkerCnt: Int = 0)

  case class Result(targetSpecWorkerCnt: Int, accountedExclWorkerCnt: Int)
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

sealed private trait ExclMakup

private object ExclMakup {
  case object AllExcl extends ExclMakup
  case object UnmetOnly extends ExclMakup
}

final private case class SpecCandidate(
    specWorkerCnt: Int,
    minWorkerCnt: Int,
    exclMakup: ExclMakup)
