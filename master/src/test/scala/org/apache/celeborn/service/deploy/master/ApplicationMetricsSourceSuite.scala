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

package org.apache.celeborn.service.deploy.master

import java.util.{Collections, HashMap => JHashMap}
import java.util.concurrent.{CountDownLatch, Delayed, Executors, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class ApplicationMetricsSourceSuite extends CelebornFunSuite {

  // Track every source created by a test so its metricsCleaner daemon thread can be shut down
  // in afterEach, instead of leaking a scheduled thread per source across the suite.
  private val createdSources = ArrayBuffer[ApplicationMetricsSource]()

  private def newSource(conf: CelebornConf): ApplicationMetricsSource = {
    val source = new ApplicationMetricsSource(conf)
    createdSources += source
    source
  }

  override def afterEach(): Unit = {
    createdSources.foreach(_.destroy())
    createdSources.clear()
    super.afterEach()
  }

  /** Exposes per-app counter state so tests can assert nothing is left behind. */
  private class InspectableSource(conf: CelebornConf) extends ApplicationMetricsSource(conf) {
    def trackedCounterApps(labels: Map[String, String], name: String = CounterName): Set[String] =
      Option(namedCountersWithDetails.get(metricNameWithCustomizedLabels(name, labels)))
        .map(_.perAppValues.keySet().asScala.toSet)
        .getOrElse(Set.empty)

    def removeIdleCounters(idleBeforeMs: Long): Unit = removeIdleAppCounters(idleBeforeMs)
  }

  private def newInspectableSource(conf: CelebornConf): InspectableSource = {
    val source = new InspectableSource(conf)
    createdSources += source
    source
  }

  /**
   * A source whose first isAppRemoved call (made inside compute, before a new series is
   * published) blocks until the test releases it, so cleanup can be run deterministically in
   * the window between the removal check and the map insertion.
   */
  private class PausingSource(conf: CelebornConf) extends InspectableSource(conf) {
    val paused = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    private val firstCall = new AtomicBoolean(true)

    override protected def isAppRemoved(appId: String): Boolean = {
      val removed = super.isAppRemoved(appId)
      if (firstCall.compareAndSet(true, false)) {
        paused.countDown()
        assert(release.await(10, TimeUnit.SECONDS))
      }
      removed
    }
  }

  /** Runs the first heartbeat of an absent series, firing `cleanup` mid-insertion. */
  private def raceFirstInsertion(
      source: PausingSource,
      labels: Map[String, String],
      heartbeatBody: () => Unit = null)(cleanup: => Unit): Unit = {
    val body =
      if (heartbeatBody != null) heartbeatBody else () => update(source, gaugeMetrics(7), labels)
    val heartbeat = new Thread(() => body())
    heartbeat.start()
    assert(source.paused.await(10, TimeUnit.SECONDS))
    cleanup
    source.release.countDown()
    heartbeat.join(10000)
    assert(!heartbeat.isAlive)
  }

  private def enabledConf(): CelebornConf = {
    val c = new CelebornConf()
    c.set(CelebornConf.MASTER_CLIENT_METRICS_ENABLED, true)
    c
  }

  private def scheduledTaskCount(source: ApplicationMetricsSource): Int =
    source.metricsCleaner.asInstanceOf[ScheduledThreadPoolExecutor].getQueue.size()

  private def gaugeMetrics(value: Long): JHashMap[String, java.lang.Long] = {
    val map = new JHashMap[String, java.lang.Long]()
    map.put("ClientActiveShuffleCount", value)
    map
  }

  private def update(
      source: ApplicationMetricsSource,
      metrics: JHashMap[String, java.lang.Long],
      labels: Map[String, String] = Map.empty,
      appId: String = "app-1"): Unit =
    source.updateApplicationMetrics(
      appId,
      labels.asJava,
      metrics,
      Collections.emptyMap[String, java.lang.Long]())

  private val CounterName = "ClientReviveCount"

  private def updateCounter(
      source: ApplicationMetricsSource,
      value: Long,
      labels: Map[String, String],
      appId: String = "app-1"): Unit = {
    val counters = new JHashMap[String, java.lang.Long]()
    counters.put(CounterName, value)
    source.updateApplicationMetrics(
      appId,
      labels.asJava,
      Collections.emptyMap[String, java.lang.Long](),
      counters)
  }

  private def counterValue(
      source: ApplicationMetricsSource,
      labels: Map[String, String],
      name: String = CounterName): Option[Long] =
    source.counters()
      .find(c => c.name == name && hasLabels(c.labels, labels))
      .map(_.counter.getCount)

  private def gaugeValue(
      source: ApplicationMetricsSource,
      labels: Map[String, String],
      name: String = "ClientActiveShuffleCount"): Option[Long] =
    source.gauges()
      .find(g => g.name == name && hasLabels(g.labels, labels))
      .map(_.gauge.getValue.asInstanceOf[Number].longValue())

  private def hasLabels(
      actual: Map[String, String],
      expected: Map[String, String]): Boolean =
    expected.forall { case (key, value) => actual.get(key).contains(value) }

  test("masterClientMetrics disabled: removed app cleaner is not scheduled") {
    val source = newSource(new CelebornConf())

    assert(scheduledTaskCount(source) == 0)
  }

  test("removed app cleaner uses configured retention as schedule interval") {
    val conf = enabledConf()
    conf.set(CelebornConf.MASTER_CLIENT_METRICS_REMOVED_APP_RETENTION.key, "2s")
    val retentionMs = conf.masterClientMetricsRemovedAppRetentionMs
    assert(retentionMs == 2000L)
    val source = newSource(conf)

    val scheduledTasks = source.metricsCleaner
      .asInstanceOf[ScheduledThreadPoolExecutor]
      .getQueue
    assert(scheduledTasks.size() == 1)
    val delayMs = scheduledTasks.peek().asInstanceOf[Delayed].getDelay(TimeUnit.MILLISECONDS)
    assert(delayMs > 0)
    assert(delayMs <= retentionMs)
  }

  test("client labels are used as metric labels") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("team" -> "data-eng"))

    val metrics = source.getMetrics
    assert(metrics.contains("""team="data-eng""""))
  }

  test("gauge is updated to the latest reported value") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(1), labels)
    update(source, gaugeMetrics(42), labels)

    assert(gaugeValue(source, labels).contains(42L))
  }

  test("gauge for a label set aggregates values across apps by sum") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    assert(gaugeValue(source, labels).contains(10L))
  }

  test("masterClientMetrics disabled: replicated client metrics are ignored") {
    val source = newSource(new CelebornConf())

    update(source, gaugeMetrics(3), Map("team" -> "data-eng"))

    assert(source.gauges().isEmpty)
  }

  test("removing one app updates gauge sum while another app still contributes") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    assert(gaugeValue(source, labels).contains(7L))
  }

  test("removing the last contributing app deregisters tracked metrics") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    assert(gaugeValue(source, labels).contains(7L))

    source.removeApplicationMetrics("app-2")

    assert(gaugeValue(source, labels).isEmpty)
  }

  test("late heartbeats for removed apps are ignored") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    source.removeApplicationMetrics("app-1")
    update(source, gaugeMetrics(9), labels, "app-1")

    assert(gaugeValue(source, labels).isEmpty)
  }

  test("labels with invalid Prometheus key names are rejected") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("invalid-key" -> "ok"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("123start" -> "ok"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("valid_key" -> "ok"))
    assert(source.gauges().nonEmpty)
  }

  test("labels with unsafe values (quotes, backslashes, newlines) are rejected") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("team" -> """val"ue"""))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "val\\ue"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "val\nue"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "safe_value"))
    assert(source.gauges().nonEmpty)
  }

  test("gauge values from multiple apps are summed, not last-writer-wins") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    assert(gaugeValue(source, labels).contains(10L))

    update(source, gaugeMetrics(5), labels, "app-1")
    assert(gaugeValue(source, labels).contains(12L))
  }

  test("clearApplicationMetrics drops all series but keeps removed apps ignored") {
    val source = newSource(enabledConf())
    val engLabels = Map("team" -> "data-eng")
    val mlLabels = Map("team" -> "ml")
    update(source, gaugeMetrics(3), engLabels, "app-1")
    update(source, gaugeMetrics(4), engLabels, "app-2")
    update(source, gaugeMetrics(5), mlLabels, "app-3")
    source.removeApplicationMetrics("app-3")
    assert(source.gauges().size == 1)

    source.clearApplicationMetrics()

    assert(source.gauges().isEmpty)
    assert(!source.gaugeExists("ClientActiveShuffleCount", engLabels))

    // Live apps repopulate from their next heartbeat, starting from a clean sum.
    update(source, gaugeMetrics(6), engLabels, "app-1")
    assert(gaugeValue(source, engLabels).contains(6L))

    // An app removed before the clear stays ignored.
    update(source, gaugeMetrics(5), mlLabels, "app-3")
    assert(gaugeValue(source, mlLabels).isEmpty)
  }

  test("removal racing the first insertion of an absent series leaves no gauge") {
    val source = new PausingSource(enabledConf())
    createdSources += source
    val labels = Map("team" -> "data-eng")

    raceFirstInsertion(source, labels) {
      source.removeApplicationMetrics("app-1")
    }

    assert(gaugeValue(source, labels).isEmpty)
    assert(source.gauges().isEmpty)
  }

  test("counter from a new app counts its full cumulative value") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 5, labels)

    assert(counterValue(source, labels).contains(5L))
    assert(source.getMetrics.contains("metrics_ClientReviveCount_Count"))
  }

  test("duplicated counter heartbeats are not double counted") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 5, labels)
    updateCounter(source, 5, labels)
    updateCounter(source, 8, labels)
    updateCounter(source, 8, labels)

    assert(counterValue(source, labels).contains(8L))
  }

  test("late counter heartbeat with a lower value is ignored") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 8, labels)
    updateCounter(source, 5, labels)
    assert(counterValue(source, labels).contains(8L))

    // The stale report must not lower the stored value, or the next report would recount 5..8.
    updateCounter(source, 10, labels)
    assert(counterValue(source, labels).contains(10L))
  }

  test("counters aggregate across apps and never decrease when an app is removed") {
    val source = newInspectableSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 3, labels, "app-1")
    updateCounter(source, 7, labels, "app-2")
    assert(counterValue(source, labels).contains(10L))

    source.removeApplicationMetrics("app-1")
    assert(counterValue(source, labels).contains(10L))
    assert(source.trackedCounterApps(labels) == Set("app-2"))

    source.removeApplicationMetrics("app-2")
    // The series outlives its last app so the final increments are still scraped.
    assert(counterValue(source, labels).contains(10L))
    assert(source.trackedCounterApps(labels).isEmpty)
  }

  test("late counter heartbeats for removed apps are ignored") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 3, labels)
    source.removeApplicationMetrics("app-1")
    updateCounter(source, 9, labels)

    assert(counterValue(source, labels).contains(3L))
  }

  test("idle counters are dropped only after their idle period") {
    val source = newInspectableSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 3, labels)
    source.removeIdleCounters(System.currentTimeMillis() + 1)
    assert(counterValue(source, labels).contains(3L), "a counter with live apps is never idle")

    val beforeRemoval = System.currentTimeMillis() - 1
    source.removeApplicationMetrics("app-1")
    source.removeIdleCounters(beforeRemoval)
    assert(counterValue(source, labels).contains(3L))

    source.removeIdleCounters(System.currentTimeMillis() + 1)
    assert(counterValue(source, labels).isEmpty)
  }

  test("an app reporting again before idle cleanup keeps the counter") {
    val source = newInspectableSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    updateCounter(source, 3, labels, "app-1")
    source.removeApplicationMetrics("app-1")
    updateCounter(source, 2, labels, "app-2")

    source.removeIdleCounters(System.currentTimeMillis() + 1)
    assert(counterValue(source, labels).contains(5L))
  }

  test("clearApplicationMetrics drops counters and apps repopulate from their next report") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")
    updateCounter(source, 3, labels)

    source.clearApplicationMetrics()
    assert(counterValue(source, labels).isEmpty)

    updateCounter(source, 6, labels)
    assert(counterValue(source, labels).contains(6L))
    updateCounter(source, 9, labels)
    assert(counterValue(source, labels).contains(9L))
  }

  test("removal racing the first counter insertion leaves no per-app state") {
    val source = new PausingSource(enabledConf())
    createdSources += source
    val labels = Map("team" -> "data-eng")

    raceFirstInsertion(source, labels, () => updateCounter(source, 7, labels)) {
      source.removeApplicationMetrics("app-1")
    }

    assert(source.trackedCounterApps(labels).isEmpty)
    updateCounter(source, 9, labels)
    assert(counterValue(source, labels).forall(_ <= 7L))
  }

  test("concurrent, duplicated and reordered counter heartbeats count each increment once") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")
    val apps = 8
    val reportsPerApp = 200
    val pool = Executors.newFixedThreadPool(apps * 2)
    try {
      val start = new CountDownLatch(1)
      // Two senders per app replay the same cumulative series in opposite orders.
      val futures = (0 until apps).flatMap { app =>
        Seq(1 to reportsPerApp, reportsPerApp to 1 by -1).map { series =>
          pool.submit(new Runnable {
            override def run(): Unit = {
              start.await()
              series.foreach(v => updateCounter(source, v, labels, s"app-$app"))
            }
          })
        }
      }
      start.countDown()
      futures.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }

    assert(counterValue(source, labels).contains(apps.toLong * reportsPerApp))
  }
}
