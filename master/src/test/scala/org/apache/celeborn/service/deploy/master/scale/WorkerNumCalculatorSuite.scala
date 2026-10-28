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
import org.apache.celeborn.service.deploy.master.scale.WorkerNumCalculator.Input

class WorkerNumCalculatorSuite extends AnyFunSuite {
  private val ratioKeys = Seq(
    WorkerStats.NettyMemoryUsedRatio,
    WorkerStats.LastMinuteSystemLoadRatio,
    WorkerStats.DiskUsedRatio)
  private val low = WorkerStats(ratioKeys.map(_ -> 0.1).toMap)
  private val neutral = WorkerStats(low.metrics ++ ratioKeys.map(_ -> 0.4))

  private def baseConf(): CelebornConf =
    new CelebornConf(false).set(SCALE_WORKER_NUMBER_MIN.key, "1")

  private def snapshot(count: Int, stats: WorkerStats = low): Input =
    Input(count, count, Seq.fill(count)(stats))

  private def newCalculator(conf: CelebornConf = baseConf()): WorkerNumCalculator =
    new WorkerNumCalculator(conf)

  private def configSource(conf: CelebornConf): ConfigService = {
    val source = mock[ConfigService]
    when(source.getSystemConfigFromCache).thenReturn(new SystemConfig(conf))
    source
  }

  test("each ratio independently triggers scale-up and the largest demand wins") {
    ratioKeys.foreach { key =>
      val stats = WorkerStats(low.metrics + (key -> 0.9))
      assert(newCalculator().cal(snapshot(10, stats)) == 15, key)
    }
    val conf = baseConf().set(SCALE_UP_POLICY_STEP_NUMBER.key, "100")
    val stats = WorkerStats(low.metrics ++ Map(
      WorkerStats.NettyMemoryUsedRatio -> 0.6,
      WorkerStats.LastMinuteSystemLoadRatio -> 1.2,
      WorkerStats.DiskUsedRatio -> 0.8))
    assert(newCalculator(conf).cal(snapshot(10, stats)) == 24)
  }

  test("scale-up and scale-down use their own thresholds for estimation") {
    val conf = baseConf()
      .set(SCALE_UP_MEMORY_RATIO.key, "0.8")
      .set(SCALE_DOWN_MEMORY_RATIO.key, "0.2")
      .set(SCALE_UP_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    val highMemory = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9))
    assert(newCalculator(conf).cal(snapshot(10, highMemory)) == 12)
    assert(newCalculator(conf).cal(snapshot(10)) == 5)
  }

  test("all dimensions must be strictly below their scale-down thresholds") {
    assert(newCalculator().cal(snapshot(10)) == 9)
    ratioKeys.foreach { key =>
      Seq(0.3, 0.4).foreach { value =>
        val stats = WorkerStats(low.metrics + (key -> value))
        assert(newCalculator().cal(snapshot(10, stats)) == 10, s"$key=$value")
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
      assert(newCalculator(conf).cal(snapshot(count, stats)) == expected)
    }
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
      assert(newCalculator(conf).cal(snapshot(count)) == expected)
    }
  }

  test("budgets never overshoot the estimated difference") {
    val high = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.55))
    assert(newCalculator().cal(snapshot(10, high)) == 11)
    val conf = baseConf()
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    val stats = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.26))
    assert(newCalculator(conf).cal(snapshot(10, stats)) == 9)
  }

  test("ratio sums and averages retain double rounding at integer boundaries") {
    val high = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.7))
    // Ten additions of 0.7 produce 7.000000000000001, whose estimate is fifteen workers.
    assert(newCalculator().cal(snapshot(10, high)) == 15)
    val conf = baseConf()
      .set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
      .set(SCALE_DOWN_POLICY_PERCENT.key, "1")
    // Three additions of 0.1 produce 0.30000000000000004; ceil(sum / 0.3) is two.
    assert(newCalculator(conf).cal(snapshot(3)) == 2)
    val nearBoundary = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.27))
    assert(newCalculator(conf).cal(snapshot(10, nearBoundary)) == 10)
    val atThreshold = baseConf()
      .set(SCALE_UP_SYSTEM_LOAD_RATIO.key, "0.1")
      .set(SCALE_DOWN_SYSTEM_LOAD_RATIO.key, "0.05")
    val stats = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.1))
    assert(newCalculator(atThreshold).cal(snapshot(3, stats)) == 4)
  }

  test("missing and invalid ratio metrics reject the decision instead of becoming zero") {
    ratioKeys.foreach { key =>
      val missing = WorkerStats(low.metrics - key)
      intercept[IllegalArgumentException] {
        newCalculator().cal(snapshot(10, missing))
      }
      Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity, -0.1).foreach { value =>
        intercept[IllegalArgumentException] {
          newCalculator().cal(snapshot(10, WorkerStats(low.metrics + (key -> value))))
        }
      }
    }
  }

  test("remaining disk bytes do not change the ratio calculation") {
    val stats = WorkerStats(low.metrics + (WorkerStats.DiskRemainingSize -> 0.0))
    assert(newCalculator().cal(snapshot(10, stats)) == 9)
  }

  test("the default minimum matches the reference and active alignment is optional") {
    val defaults = new WorkerNumCalculator(new CelebornConf(false))
    assert(defaults.cal(snapshot(10)) == 10)
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MIN.key, "30")
    assert(newCalculator(conf).cal(snapshot(10, neutral)) == 10)
    conf.set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    // Minimum alignment is applied after the step, so it can increase ten directly to thirty.
    assert(newCalculator(conf).cal(snapshot(10, neutral)) == 30)
    assert(newCalculator(conf).cal(Input(0, 0, Seq.empty)) == 30)
    assert(newCalculator(conf).cal(snapshot(35)) == 32)
  }

  test("lowering the maximum forces gradual convergence even with high load") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "8")
    val high = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9))
    Seq(low, neutral, high).foreach { stats =>
      assert(newCalculator(conf).cal(snapshot(10, stats)) == 9)
    }
    assert(newCalculator(conf).cal(Input(10, 10, Seq.empty)) == 9)
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "10")
    assert(newCalculator(conf).cal(snapshot(100)) == 95)
    conf.set(SCALE_WORKER_NUMBER_MAX.key, "1500")
    val oversized = snapshot(1600, high)
    assert(newCalculator(conf).cal(oversized) == 1595)
    assert(newCalculator(conf).cal(
      oversized.copy(excludedWorkerCount = 2, newlyExcludedWorkerCount = 2)) == 1500)
  }

  test("the maximum prevents scale-up at the limit without clamping every final result") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MAX.key, "12")
    val high = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9))
    assert(newCalculator(conf).cal(snapshot(10, high)) == 12)
    assert(newCalculator(conf).cal(snapshot(12, high)) == 12)
    val excluded = Input(12, 12, snapshot(12, neutral).workerStats, 2, 2)
    assert(newCalculator(conf).cal(excluded) == 12)
  }

  test("new exclusions compensate once and remain part of the metric inventory") {
    val initial = snapshot(10, neutral).copy(excludedWorkerCount = 2, newlyExcludedWorkerCount = 2)
    val workerNumCalculator = newCalculator()
    val target = workerNumCalculator.cal(initial)
    assert(target == 12)
    assert(workerNumCalculator.cal(initial) == 12)
    val pending = initial.copy(expectedWorkerCount = target, newlyExcludedWorkerCount = 0)
    assert(workerNumCalculator.cal(pending) == 12)
    val accepted = snapshot(12, neutral).copy(excludedWorkerCount = 2)
    assert(workerNumCalculator.cal(accepted) == 12)
    val cancelled = accepted.copy(excludedWorkerCount = 0)
    assert(workerNumCalculator.cal(cancelled) == 12)
    assert(workerNumCalculator.cal(snapshot(12)) == 11)
  }

  test("excluded workers retain their existing pressure in demand and trigger calculations") {
    val busy = WorkerStats(neutral.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 3.0))
    val input = Input(10, 10, Seq.fill(2)(busy) ++ Seq.fill(8)(neutral), 2, 0)
    assert(newCalculator().cal(input) == 15)
    assert(newCalculator().cal(snapshot(10, neutral).copy(excludedWorkerCount = 10)) == 10)
  }

  test("the exclusion floor follows the selected decision branch") {
    val conf = baseConf().set(SCALE_WORKER_NUMBER_MIN.key, "5")
    val neutralInput = snapshot(5, neutral).copy(excludedWorkerCount = 2)
    assert(newCalculator(conf).cal(neutralInput) == 5)
    conf.set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key, "true")
    assert(newCalculator(conf).cal(neutralInput) == 5)
    // The low-load branch uses the configured minimum plus total exclusions.
    assert(newCalculator(conf).cal(snapshot(5).copy(excludedWorkerCount = 2)) == 7)
    // Active minimum alignment also includes existing exclusions.
    conf.set(SCALE_WORKER_NUMBER_MIN.key, "12")
    assert(newCalculator(conf).cal(snapshot(10, neutral).copy(excludedWorkerCount = 2)) == 14)
  }

  test("count results can reduce pending capacity while recovering drained workers") {
    val high = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.6))
    assert(newCalculator().cal(snapshot(10, high).copy(expectedWorkerCount = 15)) == 12)
    val input = Input(80, 100, Seq.fill(77)(WorkerStats(ratioKeys.map(_ -> 0.65).toMap)))
    assert(newCalculator().cal(input) == 84)
    val recovering = Input(10, 10, Seq.fill(5)(WorkerStats(ratioKeys.map(_ -> 0.7).toMap)))
    assert(newCalculator().cal(recovering) == 7)
    val stats = WorkerStats(Map(
      WorkerStats.NettyMemoryUsedRatio -> 0.25,
      WorkerStats.LastMinuteSystemLoadRatio -> 0.75,
      WorkerStats.DiskUsedRatio -> 0.25))
    assert(newCalculator().cal(Input(10, 15, Seq.fill(8)(stats), 2, 1)) == 13)
  }

  test("empty metrics and automatically draining workers retain the reference count semantics") {
    assert(newCalculator().cal(Input(10, 10, Seq.empty)) == 10)
    assert(newCalculator().cal(Input(10, 10, Seq.fill(6)(low))) == 9)
    assert(newCalculator().cal(Input(0, 0, Seq.empty)) == 0)
  }

  test("large finite values saturate the estimate and count additions do not overflow") {
    val high = WorkerStats(low.metrics +
      (WorkerStats.LastMinuteSystemLoadRatio -> Double.MaxValue))
    assert(newCalculator().cal(snapshot(10, high)) == 15)
    val conf = baseConf()
      .set(SCALE_WORKER_NUMBER_MIN.key, "2")
      .set(SCALE_WORKER_NUMBER_MAX.key, Int.MaxValue.toString)
    val input = Input(Int.MaxValue, Int.MaxValue, Seq(neutral), 1, 1)
    assert(newCalculator(conf).cal(input) == Int.MaxValue)
  }

  test("every calculator setting can be overridden by system dynamic configuration") {
    val highMemory = WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9))
    val highLoad = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 0.9))
    val highDisk = WorkerStats(low.metrics + (WorkerStats.DiskUsedRatio -> 0.9))
    val largeLoad = WorkerStats(low.metrics + (WorkerStats.LastMinuteSystemLoadRatio -> 10.0))
    val conf = baseConf().set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "100")
    val source = configSource(conf)
    val systemConfig = source.getSystemConfigFromCache
    val workerNumCalculator = new WorkerNumCalculator(conf, Some(source))
    val cases = Seq(
      (SCALE_WORKER_NUMBER_MIN, "12", snapshot(20), 18),
      (SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED, "true", snapshot(0, neutral), 1),
      (SCALE_WORKER_NUMBER_MAX, "12", snapshot(10, highMemory), 12),
      (SCALE_UP_MEMORY_RATIO, "0.8", snapshot(10, highMemory), 12),
      (SCALE_DOWN_MEMORY_RATIO, "0.05", snapshot(10), 10),
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
      assert(workerNumCalculator.cal(input) == expected, entry.key)
    }
  }

  test("an existing calculator observes FS refresh and falls back when overrides are removed") {
    val file = Files.createTempFile("worker-num-calculator-config", ".yaml")
    val conf = baseConf()
      .set(DYNAMIC_CONFIG_STORE_FS_PATH.key, file.toString)
      .set(DYNAMIC_CONFIG_REFRESH_INTERVAL.key, "1d")
      .set(SCALE_UP_MEMORY_RATIO.key, "0.6")
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
      writeConfig(Map(SCALE_UP_MEMORY_RATIO.key -> "0.8"))
      source = new FsConfigServiceImpl(conf)
      val workerNumCalculator = new WorkerNumCalculator(conf, Some(source))
      val input = snapshot(10, WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9)))
      assert(workerNumCalculator.cal(input) == 12)
      writeConfig(Map(SCALE_UP_MEMORY_RATIO.key -> "0.5", SCALE_UP_POLICY_STEP_NUMBER.key -> "7"))
      source.refreshCache()
      assert(workerNumCalculator.cal(input) == 17)
      writeConfig(Map.empty)
      source.refreshCache()
      assert(workerNumCalculator.cal(input) == 13)
    } finally {
      if (source != null) source.shutdown()
      Files.deleteIfExists(file)
    }
  }

  test("one configuration snapshot is used for each evaluation") {
    val conf = baseConf()
    val first = new SystemConfig(conf)
    val second = new SystemConfig(conf)
    second.setConfigs(Map(SCALE_UP_MEMORY_RATIO.key -> "0.8").asJava)
    val source = mock[ConfigService]
    when(source.getSystemConfigFromCache).thenReturn(first, second)
    val workerNumCalculator = new WorkerNumCalculator(conf, Some(source))
    val input = snapshot(10, WorkerStats(low.metrics + (WorkerStats.NettyMemoryUsedRatio -> 0.9)))
    assert(workerNumCalculator.cal(input) == 15)
    assert(workerNumCalculator.cal(input) == 12)
    verify(source, times(2)).getSystemConfigFromCache
  }

  test("invalid dynamic settings fail the round and corrected settings apply next round") {
    val conf = baseConf()
    val source = configSource(conf)
    val systemConfig = source.getSystemConfigFromCache
    val workerNumCalculator = new WorkerNumCalculator(conf, Some(source))
    val invalid = Seq(
      Map(SCALE_WORKER_NUMBER_MIN.key -> "0"),
      Map(SCALE_WORKER_NUMBER_MIN.key -> "2001"),
      Map(SCALE_UP_POLICY_STEP_NUMBER.key -> "not-a-number"),
      Map(SCALE_UP_POLICY_PERCENT.key -> "NaN"),
      Map(SCALE_UP_POLICY_PERCENT.key -> "1.1"),
      Map(SCALE_UP_MEMORY_RATIO.key -> "1.1"),
      Map(SCALE_UP_SYSTEM_LOAD_RATIO.key -> "Infinity"),
      Map(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED.key -> "not-a-boolean"),
      Map(SCALE_UP_MEMORY_RATIO.key -> "0.3", SCALE_DOWN_MEMORY_RATIO.key -> "0.3"),
      Map(SCALE_UP_SYSTEM_LOAD_RATIO.key -> "0.3", SCALE_DOWN_SYSTEM_LOAD_RATIO.key -> "0.3"),
      Map(SCALE_UP_DISK_RATIO.key -> "0.3", SCALE_DOWN_DISK_RATIO.key -> "0.3"))
    invalid.foreach { overrides =>
      systemConfig.setConfigs(overrides.asJava)
      intercept[IllegalArgumentException] {
        workerNumCalculator.cal(snapshot(10))
      }
      systemConfig.setConfigs(Map.empty[String, String].asJava)
      assert(workerNumCalculator.cal(snapshot(10)) == 9)
    }
  }

  test("invalid input counts fail before a decision is made") {
    val input = snapshot(10)
    Seq(
      input.copy(currentWorkerCount = -1),
      input.copy(expectedWorkerCount = -1),
      input.copy(excludedWorkerCount = -1),
      input.copy(newlyExcludedWorkerCount = 1),
      input.copy(excludedWorkerCount = 11),
      input.copy(currentWorkerCount = 9),
      input.copy(newlyExcludedWorkerCount = -1)).foreach { invalid =>
      intercept[IllegalArgumentException] {
        newCalculator().cal(invalid)
      }
    }
  }
}
