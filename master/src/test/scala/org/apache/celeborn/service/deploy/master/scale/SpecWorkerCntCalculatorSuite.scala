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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.server.common.service.config.{ConfigService, FsConfigServiceImpl, SystemConfig}
import org.apache.celeborn.service.deploy.master.scale.SpecWorkerCntCalculator.{Input, Result}

class SpecWorkerCntCalculatorSuite extends AnyFunSuite {
  private val ratioKeys = Seq(
    WorkerStats.MemoryPressureRatio,
    WorkerStats.LastMinuteSystemLoadRatio,
    WorkerStats.DiskUsedRatio)
  private val low = WorkerStats(ratioKeys.map(_ -> 0.1).toMap)
  private val neutral = WorkerStats(low.metrics ++ ratioKeys.map(_ -> 0.4))

  private def baseConf(): CelebornConf =
    new CelebornConf(false).set(SCALE_WORKER_NUMBER_MIN.key, "1")

  private def snapshot(count: Int, stats: WorkerStats = low): Input =
    Input(count, count, Seq.fill(count)(stats))

  private def newCalculator(conf: CelebornConf = baseConf()): SpecWorkerCntCalculator =
    new SpecWorkerCntCalculator(conf)

  private def configSource(conf: CelebornConf): ConfigService = {
    val source = mock[ConfigService]
    when(source.getSystemConfigFromCache).thenReturn(new SystemConfig(conf))
    source
  }

  test("each ratio independently triggers scale-up and the largest demand wins") {
    ratioKeys.foreach { key =>
      val stats = WorkerStats(low.metrics + (key -> 0.9))
      assert(newCalculator().calSpecWorkerCnt(snapshot(10, stats)) == 15, key)
    }
    val conf = baseConf().set(SCALE_UP_POLICY_STEP_NUMBER.key, "100")
    val stats = WorkerStats(low.metrics ++ Map(
      WorkerStats.MemoryPressureRatio -> 0.6,
      WorkerStats.LastMinuteSystemLoadRatio -> 1.2,
      WorkerStats.DiskUsedRatio -> 0.8))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, stats)) == 24)
  }

  test("memory pressure above one is retained when estimating demand") {
    val conf = baseConf().set(SCALE_UP_POLICY_STEP_NUMBER.key, "100")
    val stats = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 1.25))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, stats)) == 25)
  }

  test("scale-up and scale-down use their own thresholds for estimation") {
    val conf = baseConf()
      .set("celeborn.scale.up.memoryPressure.ratio", "0.8")
      .set("celeborn.scale.down.memoryPressure.ratio", "0.2")
      .set(SCALE_UP_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    val highMemory = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, highMemory)) == 12)
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10)) == 5)
  }

  test("all dimensions must be strictly below their scale-down thresholds") {
    assert(newCalculator().calSpecWorkerCnt(snapshot(10)) == 9)
    ratioKeys.foreach { key =>
      Seq(0.3, 0.4).foreach { value =>
        val stats = WorkerStats(low.metrics + (key -> value))
        assert(newCalculator().calSpecWorkerCnt(snapshot(10, stats)) == 10, s"$key=$value")
      }
    }
  }

  test("scale-up budget uses the maximum of three, the fixed step and the floored percentage") {
    val stats = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 10.0))
    val cases = Seq(
      (10, 1, 0.01, 13),
      (10, 7, 0.01, 17),
      (100, 1, 0.29, 128))
    cases.foreach { case (count, step, percent, expected) =>
      val conf = baseConf()
        .set(SCALE_UP_POLICY_STEP_NUMBER.key, step.toString)
        .set(SCALE_UP_POLICY_PERCENT.key, percent.toString)
      assert(newCalculator(conf).calSpecWorkerCnt(snapshot(count, stats)) == expected)
    }
  }

  test("high load can restore the spec to demand already covered by reporting workers") {
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MAX.key, "20")
      .set(SCALE_UP_POLICY_STEP_NUMBER.key, "3")
    val calculator = newCalculator(conf)
    val high = WorkerStats(neutral.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    Seq((20, 20), (19, 20), (15, 18)).foreach { case (reportingWorkerCnt, target) =>
      val input = snapshot(reportingWorkerCnt, high).copy(specWorkerCnt = 15)
      assert(calculator.calSpecWorkerCnt(input) == target, s"reporting=$reportingWorkerCnt")
    }
    assert(calculator.calSpecWorkerCnt(snapshot(19, high).copy(specWorkerCnt = 20)) == 20)
  }

  test("minimum alignment with neutral load keeps the minimum target") {
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "10")
      .set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
      .set(SCALE_WORKER_NUMBER_MAX.key, "20")
    val input = snapshot(20, neutral).copy(specWorkerCnt = 5)
    assert(newCalculator(conf).calSpecWorkerCnt(input) == 10)
  }

  test("scale-down budget uses the minimum and removes at least one when needed") {
    val cases = Seq(
      (10, 5, 0.01, 9),
      (100, 5, 0.29, 95),
      (100, 100, 0.29, 72))
    cases.foreach { case (count, step, percent, expected) =>
      val conf = baseConf()
        .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, step.toString)
        .set(SCALE_DOWN_POLICY_PERCENT.key, percent.toString)
      assert(newCalculator(conf).calSpecWorkerCnt(snapshot(count)) == expected)
    }
  }

  test("budgets never overshoot the estimated difference") {
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.55))
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, high)) == 11)
    val conf = baseConf()
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    val stats = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.26))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, stats)) == 9)
  }

  test("permuting worker reports at the scale-up threshold keeps the same spec") {
    val calculator = newCalculator()
    Seq(0.8, 0.3, 0.4).permutations.foreach { memoryRatios =>
      val stats = memoryRatios.map { ratio =>
        WorkerStats(neutral.metrics + (WorkerStats.MemoryPressureRatio -> ratio))
      }
      assert(calculator.cal(Input(3, 20, stats)) == Result(20, 0), s"memory ratios=$memoryRatios")
    }
  }

  test("permuting worker reports at integer demand gives the same rounded demand") {
    val calculator = newCalculator()
    val cases = Seq(
      (Seq(0.1, 0.2, 2.2), neutral, 5),
      (Seq(0.1, 0.2, 0.3), low, 2))
    cases.foreach { case (values, otherRatios, expected) =>
      values.permutations.foreach { memoryRatios =>
        val stats = memoryRatios.map { ratio =>
          WorkerStats(otherRatios.metrics + (WorkerStats.MemoryPressureRatio -> ratio))
        }
        assert(
          calculator.calSpecWorkerCnt(Input(3, 20, stats)) == expected,
          s"memory ratios=$memoryRatios")
      }
    }
  }

  test("demand rounds up only across integer boundaries") {
    val high = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.7))
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, high)) == 14)
    val conf = baseConf()
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(3)) == 1)
    val nearBoundary = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.27))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, nearBoundary)) == 9)
  }

  test("scale-up only triggers when the average is strictly above its configured threshold") {
    val conf = baseConf()
      .set(SCALE_UP_SYSTEM_LOAD_RATIO.key, "0.1")
      .set(SCALE_DOWN_SYSTEM_LOAD_RATIO.key, "0.05")
    val calculator = newCalculator(conf)
    val atThreshold = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.1))
    val aboveThreshold = WorkerStats(
      low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> Math.nextUp(0.1)))
    assert(calculator.calSpecWorkerCnt(snapshot(3, atThreshold)) == 3)
    assert(calculator.calSpecWorkerCnt(snapshot(3, aboveThreshold)) == 4)
  }

  test("missing and invalid ratio metrics reject the decision instead of becoming zero") {
    ratioKeys.foreach { key =>
      val missing = WorkerStats(low.metrics - key)
      intercept[IllegalArgumentException] {
        newCalculator().calSpecWorkerCnt(snapshot(10, missing))
      }
      val infinities = Seq(Double.PositiveInfinity, Double.NegativeInfinity)
      val invalid =
        if (key == WorkerStats.LastMinuteSystemLoadRatio) infinities
        else infinities :+ (-0.1)
      invalid.foreach { value =>
        intercept[IllegalArgumentException] {
          newCalculator().calSpecWorkerCnt(snapshot(
            10,
            WorkerStats(low.metrics + (key -> value))))
        }
      }
    }
  }

  test("unavailable ratios allow observed scale-up and prevent load-driven scale-down") {
    ratioKeys.foreach { unavailableKey =>
      val input = snapshot(20).copy(workerStats = Seq.fill(19)(low) :+
        WorkerStats(low.metrics + (unavailableKey -> Double.NaN)))
      assert(newCalculator().calSpecWorkerCnt(input) == 20, unavailableKey)
      ratioKeys.filterNot(_ == unavailableKey).foreach { overloadedKey =>
        val overloaded = input.copy(workerStats = input.workerStats.map { stats =>
          WorkerStats(stats.metrics + (overloadedKey -> 0.9))
        })
        assert(
          newCalculator().calSpecWorkerCnt(overloaded) == 25,
          s"unavailable=$unavailableKey, overloaded=$overloadedKey")
      }
    }
  }

  test("unavailable ratios preserve explicit minimum and maximum constraints") {
    val unavailable = WorkerStats(ratioKeys.map(_ -> Double.NaN).toMap)
    assert(newCalculator().calSpecWorkerCnt(snapshot(20, unavailable)) == 20)
    val aligned = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "30")
      .set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    assert(newCalculator(aligned).calSpecWorkerCnt(snapshot(20, unavailable)) == 30)
    val limited = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    assert(newCalculator(limited).calSpecWorkerCnt(snapshot(10, unavailable)) == 9)
  }

  test(
    "legacy negative system load allows scale-up from memory and prevents load-driven scale-down") {
    val calculator = newCalculator()
    val legacy = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> -0.25))
    val input = snapshot(20).copy(workerStats = Seq.fill(19)(low) :+ legacy)
    val hot = input.copy(workerStats = input.workerStats.map { stats =>
      WorkerStats(stats.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    })
    assert(calculator.cal(hot) == Result(25, 0))
    assert(calculator.cal(input) == Result(20, 0))
  }

  test("remaining disk bytes do not change the ratio calculation") {
    val stats = WorkerStats(low.metrics + (WorkerStats.DiskRemainingSize -> 0.0))
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, stats)) == 9)
  }

  test("the default minimum matches the reference and active alignment is optional") {
    val defaults = new SpecWorkerCntCalculator(new CelebornConf(false))
    assert(defaults.calSpecWorkerCnt(snapshot(10)) == 10)
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MIN.key, "30")
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, neutral)) == 10)
    conf.set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    // Minimum alignment is applied after the step, so it can increase ten directly to thirty.
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, neutral)) == 30)
    assert(newCalculator(conf).calSpecWorkerCnt(Input(0, 0, Seq.empty)) == 30)
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(35)) == 32)
  }

  test("minimum alignment is preserved across rounds while workers are still registering") {
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "10")
      .set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    val calculator = newCalculator(conf)
    for (ratio <- Seq(0.1, 0.4, 0.6); excluded <- Seq(0, 2)) {
      val stats = WorkerStats(ratioKeys.map(_ -> ratio).toMap)
      var input = Input(5, 10, Seq.fill(5)(stats), excludedWorkerCnt = excluded)
      (1 to 3).foreach { round =>
        withClue(s"ratio=$ratio, excluded=$excluded, round=$round: ") {
          val newSpecWorkerCnt = calculator.calSpecWorkerCnt(input)
          assert(newSpecWorkerCnt == 10 + excluded)
          input = input.copy(specWorkerCnt = newSpecWorkerCnt)
        }
      }
    }
  }

  test("lowering the maximum forces gradual convergence even with high load") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    Seq(low, neutral, high).foreach { stats =>
      assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, stats)) == 9)
    }
    assert(newCalculator(conf).calSpecWorkerCnt(Input(10, 10, Seq.empty)) == 9)
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "10")
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(100)) == 95)
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "1500")
    val oversized = snapshot(1600, high)
    val calculator = newCalculator(conf)
    assert(calculator.cal(oversized) == Result(1595, 0))
    assert(calculator.cal(
      oversized.copy(excludedWorkerCnt = 1, unaccountedExcludedWorkerCnt = 1)) == Result(1595, 0))
  }

  test("a capped replacement remains due after the maximum is raised") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "11")
    val calculator = newCalculator(conf)
    val input = snapshot(10, neutral).copy(excludedWorkerCnt = 2, unaccountedExcludedWorkerCnt = 2)

    def afterAccepting(result: Result): Input = input.copy(
      specWorkerCnt = result.targetSpecWorkerCnt,
      unaccountedExcludedWorkerCnt = input.excludedWorkerCnt - result.accountedExcludedWorkerCnt)

    val first = calculator.cal(input)
    assert(first == Result(11, 1))
    assert(calculator.cal(afterAccepting(first)) == first)

    conf.set(SCALE_WORKER_NUMBER_MAX.key, "20")
    val second = calculator.cal(afterAccepting(first))
    assert(second == Result(12, 2))
    assert(calculator.cal(afterAccepting(second)) == second)
  }

  test("accounts for the replacements covered by the minimum floor") {
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "10")
      .set(SCALE_WORKER_NUMBER_MAX.key, "11")
    val input = snapshot(9, low).copy(excludedWorkerCnt = 2, unaccountedExcludedWorkerCnt = 2)
    val calculator = newCalculator(conf)
    assert(calculator.cal(input) == Result(11, 1))
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "20")
    assert(calculator.cal(input) == Result(12, 2))
  }

  test("a metric rebuild can reduce the count of accounted excluded replacements") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "13")
    val high = WorkerStats(ratioKeys.map(_ -> 0.66).toMap)
    val input = snapshot(10, high).copy(specWorkerCnt = 12, excludedWorkerCnt = 2)
    assert(newCalculator(conf).cal(input) == Result(13, 0))
  }

  test("the maximum prevents scale-up at the limit without clamping every final result") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "12")
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, high)) == 12)
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(12, high)) == 12)
    val excluded = Input(12, 12, snapshot(12, neutral).workerStats, 2, 2)
    assert(newCalculator(conf).calSpecWorkerCnt(excluded) == 12)
  }

  test("exceeding max without a low-load signal converges to max") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "100")
    val calculator = newCalculator(conf)
    val high = WorkerStats(neutral.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    Seq(neutral, high).foreach { stats =>
      val atMax = snapshot(20, stats).copy(specWorkerCnt = 100)
      assert(calculator.cal(atMax) == Result(100, 0))
      val oversized = atMax.copy(specWorkerCnt = 101)
      val target = calculator.cal(oversized)
      assert(target == Result(100, 0))
      assert(calculator.cal(oversized.copy(specWorkerCnt = target.targetSpecWorkerCnt)) == target)
    }
  }

  test("low load can reduce an oversized target below max") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "100")
    val input = snapshot(20).copy(specWorkerCnt = 101)
    assert(newCalculator(conf).cal(input) == Result(18, 0))
  }

  test("convergence to max respects the registered-worker retirement budget") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "100")
    val calculator = newCalculator(conf)
    var specWorkerCnt = 112
    Seq((112, 107), (107, 102), (102, 100), (100, 100)).foreach {
      case (registeredWorkerCnt, expected) =>
        val input = Input(registeredWorkerCnt, specWorkerCnt, Seq.empty)
        val target = calculator.calSpecWorkerCnt(input)
        assert(target == expected, s"registered=$registeredWorkerCnt, spec=$specWorkerCnt")
        specWorkerCnt = target
    }
  }

  test("oversized targets cancel pending replicas without waiting for registrations") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    val cases = Seq(
      (Input(0, 10, Seq.empty), 8),
      (snapshot(4, high).copy(specWorkerCnt = 10), 8),
      (snapshot(8, high).copy(specWorkerCnt = 10), 8),
      (snapshot(8, neutral).copy(specWorkerCnt = 10), 8))
    val calculator = newCalculator(conf)
    cases.foreach { case (initial, expected) =>
      var input = initial
      (1 to 3).foreach { round =>
        withClue(s"registered=${input.registeredWorkerCnt}, round=$round: ") {
          val newSpecWorkerCnt = calculator.calSpecWorkerCnt(input)
          assert(newSpecWorkerCnt == expected)
          input = input.copy(specWorkerCnt = newSpecWorkerCnt)
        }
      }
    }
  }

  test("scale-down budgets retire registered workers while allowing pending cancellation") {
    val cases = Seq(
      (20, 5, 0.1, 50),
      (0, 5, 0.1, 50),
      (50, 5, 0.1, 50),
      (80, 5, 0.1, 75),
      (100, 5, 0.1, 95),
      (80, 100, 0.1, 72),
      (80, 100, 0.29, 57))
    cases.foreach { case (registeredWorkerCnt, step, percent, expected) =>
      val conf = baseConf()
        .set(SCALE_WORKER_NUMBER_MIN.key, "50")
        .set(SCALE_WORKER_NUMBER_MAX.key, "50")
        .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, step.toString)
        .set(SCALE_DOWN_POLICY_PERCENT.key, percent.toString)
      val input = snapshot(registeredWorkerCnt).copy(specWorkerCnt = 100)
      assert(
        newCalculator(conf).calSpecWorkerCnt(input) == expected,
        s"registered=$registeredWorkerCnt, step=$step, percent=$percent")
    }
  }

  test("scale-down does not increase a target while excess workers remain registered") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    val calculator = newCalculator(conf)
    var input = Input(10, 8, Seq.fill(8)(low))
    (1 to 3).foreach { _ =>
      val newSpecWorkerCnt = calculator.calSpecWorkerCnt(input)
      assert(newSpecWorkerCnt == 8)
      input = input.copy(specWorkerCnt = newSpecWorkerCnt)
    }
  }

  test("the retirement budget advances after registered workers finish draining") {
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "50")
      .set(SCALE_WORKER_NUMBER_MAX.key, "50")
    val calculator = newCalculator(conf)
    assert(calculator.calSpecWorkerCnt(snapshot(100)) == 95)
    val draining = Input(100, 100, Seq.fill(95)(low))
    assert(calculator.calSpecWorkerCnt(draining) == 95)
    assert(calculator.calSpecWorkerCnt(snapshot(95)) == 90)
  }

  test("pending capacity does not consume the registered-worker retirement budget") {
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    val input = snapshot(4, high).copy(specWorkerCnt = 100)
    val cases = Seq((3, 1.0), (100, 0.1), (100, 0.29), (100, 1.0))
    cases.foreach { case (step, percent) =>
      val conf = baseConf()
        .set(SCALE_WORKER_NUMBER_MAX.key, "8")
        .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, step.toString)
        .set(SCALE_DOWN_POLICY_PERCENT.key, percent.toString)
      assert(
        newCalculator(conf).calSpecWorkerCnt(input) == 8,
        s"step=$step, percent=$percent")
    }
  }

  test("pending cancellation respects the maximum with existing and new exclusions") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    val input = snapshot(8, neutral).copy(specWorkerCnt = 10, excludedWorkerCnt = 2)
    val calculator = newCalculator(conf)
    Seq(0, 2).foreach { unaccounted =>
      assert(calculator.cal(input.copy(unaccountedExcludedWorkerCnt = unaccounted)) == Result(8, 0))
    }
  }

  test("new exclusions compensate once and remain part of the metric inventory") {
    val initial =
      snapshot(10, neutral).copy(excludedWorkerCnt = 2, unaccountedExcludedWorkerCnt = 2)
    val calculator = newCalculator()
    val target = calculator.calSpecWorkerCnt(initial)
    assert(target == 12)
    assert(calculator.calSpecWorkerCnt(initial) == 12)
    val pending = initial.copy(specWorkerCnt = target, unaccountedExcludedWorkerCnt = 0)
    assert(calculator.calSpecWorkerCnt(pending) == 12)
    val accepted = snapshot(12, neutral).copy(excludedWorkerCnt = 2)
    assert(calculator.calSpecWorkerCnt(accepted) == 12)
    val cancelled = accepted.copy(excludedWorkerCnt = 0)
    assert(calculator.calSpecWorkerCnt(cancelled) == 12)
    assert(calculator.calSpecWorkerCnt(snapshot(12)) == 11)
  }

  test("metric-derived targets retain exclusion replacements while registration lags") {
    val stats = WorkerStats(ratioKeys.map(_ -> 0.6).toMap)
    val cases = Seq(
      (1, false, 2000, 14),
      (13, true, 2000, 15),
      (1, false, 13, 13))
    cases.foreach { case (minWorkers, align, maxWorkers, expected) =>
      val conf = baseConf()
        .set(SCALE_WORKER_NUMBER_MIN.key, minWorkers.toString)
        .set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, align.toString)
        .set(SCALE_WORKER_NUMBER_MAX.key, maxWorkers.toString)
      val calculator = newCalculator(conf)
      var input = snapshot(10, stats).copy(excludedWorkerCnt = 2, unaccountedExcludedWorkerCnt = 2)
      (1 to 3).foreach { round =>
        withClue(s"min=$minWorkers, align=$align, max=$maxWorkers, round=$round: ") {
          val newSpecWorkerCnt = calculator.calSpecWorkerCnt(input)
          assert(newSpecWorkerCnt == expected)
          input = input.copy(specWorkerCnt = newSpecWorkerCnt, unaccountedExcludedWorkerCnt = 0)
        }
      }
      assert(calculator.calSpecWorkerCnt(input.copy(workerStats =
        Seq.fill(10)(neutral))) == expected)
    }
  }

  test("low-load rebuilds retain exclusion replacements while registration lags") {
    val conf = new CelebornConf(false)
    val calculator = newCalculator(conf)
    val stats = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.299))
    val initial = snapshot(100, stats).copy(excludedWorkerCnt = 2, unaccountedExcludedWorkerCnt = 2)
    assert(calculator.cal(initial.copy(unaccountedExcludedWorkerCnt = 1)) == Result(101, 2))

    var input = initial
    def accept(expected: Result): Unit = {
      val result = calculator.cal(input)
      assert(
        result == expected,
        s"input spec=${input.specWorkerCnt}, " +
          s"unaccounted exclusions=${input.unaccountedExcludedWorkerCnt}")
      input = input.copy(
        specWorkerCnt = result.targetSpecWorkerCnt,
        unaccountedExcludedWorkerCnt = input.excludedWorkerCnt - result.accountedExcludedWorkerCnt)
    }

    (1 to 3).foreach(_ => accept(Result(102, 2)))
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "101")
    (1 to 3).foreach(_ => accept(Result(101, 1)))
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "2000")
    accept(Result(102, 2))
  }

  test("restoring the spec to demand recomputes accounted excluded replacements") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "10")
    val calculator = newCalculator(conf)
    val stats = WorkerStats(ratioKeys.map(_ -> 0.6).toMap)
    (0 to 2).foreach { unaccountedExcludedWorkerCnt =>
      val input = snapshot(10, stats).copy(
        specWorkerCnt = 8,
        excludedWorkerCnt = 2,
        unaccountedExcludedWorkerCnt = unaccountedExcludedWorkerCnt)
      val result = calculator.cal(input)
      assert(result == Result(10, 0))
      val accepted = input.copy(
        specWorkerCnt = result.targetSpecWorkerCnt,
        unaccountedExcludedWorkerCnt = input.excludedWorkerCnt - result.accountedExcludedWorkerCnt)
      assert(calculator.cal(accepted) == result)
    }
  }

  test("excluded workers retain their existing pressure in demand and trigger calculations") {
    val busy = WorkerStats(neutral.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 3.0))
    val input = Input(10, 10, Seq.fill(2)(busy) ++ Seq.fill(8)(neutral), 2, 0)
    assert(newCalculator().calSpecWorkerCnt(input) == 17)
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, neutral).copy(excludedWorkerCnt =
      10)) == 10)
  }

  test("the exclusion floor follows the selected decision branch") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MIN.key, "5")
    val neutralInput = snapshot(5, neutral).copy(excludedWorkerCnt = 2)
    assert(newCalculator(conf).calSpecWorkerCnt(neutralInput) == 5)
    conf.set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    assert(newCalculator(conf).calSpecWorkerCnt(neutralInput) == 7)
    // The low-load branch uses the configured minimum plus total exclusions.
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(5).copy(excludedWorkerCnt = 2)) == 7)
    // Active minimum alignment also includes existing exclusions.
    conf.set(SCALE_WORKER_NUMBER_MIN.key, "12")
    assert(newCalculator(conf).calSpecWorkerCnt(snapshot(10, neutral).copy(excludedWorkerCnt =
      2)) == 14)
  }

  test("count results can reduce pending capacity while recovering drained workers") {
    val high = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.6))
    assert(
      newCalculator().calSpecWorkerCnt(snapshot(10, high).copy(specWorkerCnt = 15)) == 12)
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, high).copy(
      specWorkerCnt = 16,
      excludedWorkerCnt = 2)) == 14)
    val input = Input(80, 100, Seq.fill(77)(WorkerStats(ratioKeys.map(_ -> 0.65).toMap)))
    assert(newCalculator().calSpecWorkerCnt(input) == 84)
    val recovering = Input(10, 10, Seq.fill(5)(WorkerStats(ratioKeys.map(_ -> 0.7).toMap)))
    assert(newCalculator().calSpecWorkerCnt(recovering) == 7)
    val stats = WorkerStats(Map(
      WorkerStats.MemoryPressureRatio -> 0.25,
      WorkerStats.LastMinuteSystemLoadRatio -> 0.75,
      WorkerStats.DiskUsedRatio -> 0.25))
    assert(newCalculator().calSpecWorkerCnt(Input(10, 15, Seq.fill(8)(stats), 2, 1)) == 14)
  }

  test("empty metrics and automatically draining workers retain the reference count semantics") {
    assert(newCalculator().calSpecWorkerCnt(Input(10, 10, Seq.empty)) == 10)
    assert(newCalculator().calSpecWorkerCnt(Input(10, 10, Seq.fill(6)(low))) == 9)
    assert(newCalculator().calSpecWorkerCnt(Input(0, 0, Seq.empty)) == 0)
  }

  test("large finite values saturate the estimate and count additions do not overflow") {
    val high = WorkerStats(low.metrics +
      (WorkerStats.LastMinuteSystemLoadRatio -> Double.MaxValue))
    assert(newCalculator().calSpecWorkerCnt(snapshot(10, high)) == 15)
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "2")
      .set(SCALE_WORKER_NUMBER_MAX.key, Int.MaxValue.toString)
    val input = Input(Int.MaxValue, Int.MaxValue, Seq(neutral), 1, 1)
    assert(newCalculator(conf).calSpecWorkerCnt(input) == Int.MaxValue)
  }

  test("every calculator setting can be overridden by system dynamic configuration") {
    val highMemory = WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9))
    val highLoad = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.9))
    val highDisk = WorkerStats(low.metrics + (WorkerStats.DiskUsedRatio -> 0.9))
    val largeLoad = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 10.0))
    val conf = baseConf().set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
    val source = configSource(conf)
    val systemConfig = source.getSystemConfigFromCache
    val calculator = new SpecWorkerCntCalculator(conf, Some(source))
    val cases = Seq(
      (SCALE_WORKER_NUMBER_MIN, "12", snapshot(20), 18),
      (SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED, "true", snapshot(0, neutral), 1),
      (SCALE_WORKER_NUMBER_MAX, "12", snapshot(10, highMemory), 12),
      (SCALE_UP_MEMORY_PRESSURE_RATIO, "0.8", snapshot(10, highMemory), 12),
      (SCALE_DOWN_MEMORY_PRESSURE_RATIO, "0.05", snapshot(10), 10),
      (SCALE_UP_SYSTEM_LOAD_RATIO, "0.8", snapshot(10, highLoad), 12),
      (SCALE_DOWN_SYSTEM_LOAD_RATIO, "0.05", snapshot(10), 10),
      (SCALE_UP_DISK_RATIO, "0.8", snapshot(10, highDisk), 12),
      (SCALE_DOWN_DISK_RATIO, "0.05", snapshot(10), 10),
      (SCALE_UP_POLICY_STEP_NUMBER, "7", snapshot(10, highMemory), 17),
      (SCALE_DOWN_POLICY_STEP_NUMBER, "1", snapshot(100), 99),
      (SCALE_UP_POLICY_PERCENT, "0.2", snapshot(100, largeLoad), 120),
      (SCALE_DOWN_POLICY_PERCENT, "0.2", snapshot(100), 80))
    cases.foreach { case (entry, value, input, expected) =>
      assert(entry.isDynamic, entry.key)
      systemConfig.setConfigs(Map(entry.key -> value).asJava)
      assert(calculator.calSpecWorkerCnt(input) == expected, entry.key)
    }
  }

  test("an existing calculator observes FS refresh and falls back when overrides are removed") {
    val file = Files.createTempFile("worker-num-calculator-config", ".yaml")
    val conf = baseConf()
      .set(DYNAMIC_CONFIG_STORE_FS_PATH.key, file.toString)
      .set(DYNAMIC_CONFIG_REFRESH_INTERVAL.key, "1d")
      .set(SCALE_UP_MEMORY_PRESSURE_RATIO.key, "0.6")
      .set(SCALE_UP_POLICY_STEP_NUMBER.key, "2")
    def writeConfig(overrides: Map[String, String]): Unit = {
      val yaml =
        if (overrides.isEmpty) {
          "- level: SYSTEM\n  config: {}\n"
        } else {
          "- level: SYSTEM\n  config:\n" + overrides.toSeq.sortBy(_._1).map {
            case (key, value) => s"    $key: '$value'\n"
          }.mkString
        }
      Files.write(file, yaml.getBytes(StandardCharsets.UTF_8))
    }
    var source: FsConfigServiceImpl = null
    try {
      writeConfig(Map(SCALE_UP_MEMORY_PRESSURE_RATIO.key -> "0.8"))
      source = new FsConfigServiceImpl(conf)
      val calculator = new SpecWorkerCntCalculator(conf, Some(source))
      val input = snapshot(10, WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9)))
      assert(calculator.calSpecWorkerCnt(input) == 12)
      writeConfig(Map(
        SCALE_UP_MEMORY_PRESSURE_RATIO.key -> "0.5",
        SCALE_UP_POLICY_STEP_NUMBER.key -> "7"))
      source.refreshCache()
      assert(calculator.calSpecWorkerCnt(input) == 17)
      writeConfig(Map.empty)
      source.refreshCache()
      assert(calculator.calSpecWorkerCnt(input) == 13)
    } finally {
      if (source != null) source.shutdown()
      Files.deleteIfExists(file)
    }
  }

  test("one configuration snapshot is used for each evaluation") {
    val conf = baseConf()
    val first = new SystemConfig(conf)
    val second = new SystemConfig(conf)
    second.setConfigs(Map(SCALE_UP_MEMORY_PRESSURE_RATIO.key -> "0.8").asJava)
    val source = mock[ConfigService]
    when(source.getSystemConfigFromCache).thenReturn(first, second)
    val calculator = new SpecWorkerCntCalculator(conf, Some(source))
    val input = snapshot(10, WorkerStats(low.metrics + (WorkerStats.MemoryPressureRatio -> 0.9)))
    assert(calculator.calSpecWorkerCnt(input) == 15)
    assert(calculator.calSpecWorkerCnt(input) == 12)
  }

  test("invalid dynamic settings fail the round and corrected settings apply next round") {
    val conf = baseConf()
    val source = configSource(conf)
    val systemConfig = source.getSystemConfigFromCache
    val calculator = new SpecWorkerCntCalculator(conf, Some(source))
    val invalid = Seq(
      Map(SCALE_WORKER_NUMBER_MIN.key -> "0"),
      Map(SCALE_WORKER_NUMBER_MIN.key -> "2001"),
      Map(SCALE_UP_POLICY_STEP_NUMBER.key -> "not-a-number"),
      Map(SCALE_UP_POLICY_PERCENT.key -> "NaN"),
      Map(SCALE_UP_POLICY_PERCENT.key -> "1.1"),
      Map(SCALE_UP_MEMORY_PRESSURE_RATIO.key -> "1.1"),
      Map(SCALE_UP_SYSTEM_LOAD_RATIO.key -> "Infinity"),
      Map(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key -> "not-a-boolean"),
      Map(
        SCALE_UP_MEMORY_PRESSURE_RATIO.key -> "0.3",
        SCALE_DOWN_MEMORY_PRESSURE_RATIO.key -> "0.3"),
      Map(SCALE_UP_SYSTEM_LOAD_RATIO.key -> "0.3", SCALE_DOWN_SYSTEM_LOAD_RATIO.key -> "0.3"),
      Map(SCALE_UP_DISK_RATIO.key -> "0.3", SCALE_DOWN_DISK_RATIO.key -> "0.3"))
    invalid.foreach { overrides =>
      systemConfig.setConfigs(overrides.asJava)
      intercept[IllegalArgumentException] {
        calculator.calSpecWorkerCnt(snapshot(10))
      }
      systemConfig.setConfigs(Map.empty[String, String].asJava)
      assert(calculator.calSpecWorkerCnt(snapshot(10)) == 9)
    }
  }

  test("invalid input counts fail before a decision is made") {
    val input = snapshot(10)
    Seq(
      input.copy(registeredWorkerCnt = -1),
      input.copy(specWorkerCnt = -1),
      input.copy(excludedWorkerCnt = -1),
      input.copy(unaccountedExcludedWorkerCnt = 1),
      input.copy(excludedWorkerCnt = 11),
      input.copy(registeredWorkerCnt = 9),
      input.copy(unaccountedExcludedWorkerCnt = -1)).foreach { invalid =>
      intercept[IllegalArgumentException] {
        newCalculator().calSpecWorkerCnt(invalid)
      }
    }
  }
}
