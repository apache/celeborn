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

import java.util.{HashMap => JHashMap}
import java.util.concurrent.{CountDownLatch, Delayed, ScheduledThreadPoolExecutor, TimeUnit}
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

  /**
   * A source whose first isAppRemoved call (made inside compute, before a new series is
   * published) blocks until the test releases it, so cleanup can be run deterministically in
   * the window between the removal check and the map insertion.
   */
  private class PausingSource(conf: CelebornConf) extends ApplicationMetricsSource(conf) {
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
      labels: Map[String, String])(cleanup: => Unit): Unit = {
    val heartbeat = new Thread(() => update(source, gaugeMetrics(7), labels))
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
    source.updateApplicationMetrics(appId, labels.asJava, metrics)

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
}
