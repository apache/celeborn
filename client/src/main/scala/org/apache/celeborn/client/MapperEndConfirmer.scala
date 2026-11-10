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

package org.apache.celeborn.client

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.ControlMessages.MapperEnd
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._

private[client] case class PendingMapperEnd(request: MapperEnd, enqueuedNanos: Long) {
  def mapId: Int = request.mapId
  def attemptId: Int = request.attemptId
  def taskAttemptId: Long = request.taskAttemptId
}

/**
 * Holds MapperEnds whose task attempt the engine has not accepted yet. A pending MapperEnd is
 * confirmed (handed to `confirm`) once `resolveRegisteredTaskIds` reports the same task attempt
 * for its map index, i.e. once Spark's MapOutputTracker has registered that attempt's output.
 * At most one attempt is confirmed per map index; the other pending attempts of that map are
 * dropped.
 *
 * Until a map is confirmed it is not ended in the LifecycleManager, so a retry of a task that
 * reported MapperEnd but was then failed by the engine (for example, on a preempted executor)
 * can keep pushing and reviving.
 *
 * @param resolveRegisteredTaskIds celeborn shuffle id -> registered task attempt id per map
 *                                 index (-1 where none), or None if the engine doesn't know the
 *                                 shuffle
 * @param endedAttempt             attempt id already confirmed for (shuffleId, mapId), or -1
 * @param isStageEndOrInProcess    whether StageEnd has started for the shuffle
 * @param confirm                  applies a confirmed attempt to the LifecycleManager
 */
private[client] class MapperEndConfirmer(
    intervalMs: Long,
    resolveRegisteredTaskIds: Int => Option[Array[Long]],
    endedAttempt: (Int, Int) => Int,
    isStageEndOrInProcess: Int => Boolean,
    confirm: (Int, PendingMapperEnd) => Unit) extends Logging {

  // shuffleId -> mapId -> taskAttemptId -> pending MapperEnd
  private val pending = JavaUtils.newConcurrentHashMap[
    Int,
    ConcurrentHashMap[Int, ConcurrentHashMap[Long, PendingMapperEnd]]]()

  private val scheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("celeborn-mapper-end-confirmer")

  def start(): Unit = {
    scheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = tick()
      },
      intervalMs,
      intervalMs,
      TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    scheduler.shutdownNow()
  }

  def add(shuffleId: Int, mapperEnd: PendingMapperEnd): Unit = {
    pending
      .computeIfAbsent(
        shuffleId,
        (_: Int) =>
          JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Long, PendingMapperEnd]]())
      .computeIfAbsent(
        mapperEnd.mapId,
        (_: Int) => JavaUtils.newConcurrentHashMap[Long, PendingMapperEnd]())
      .put(mapperEnd.taskAttemptId, mapperEnd)
  }

  def hasPending(shuffleId: Int): Boolean = {
    val byMap = pending.get(shuffleId)
    byMap != null && !byMap.isEmpty
  }

  def pendingCount: Int = pending.values().asScala.map(_.size()).sum

  /**
   * Confirms every pending map of the shuffle whose registered task attempt is pending.
   * Safe to call concurrently: each map is claimed with a remove(key, value) before it is
   * confirmed, so it is confirmed at most once.
   */
  def check(shuffleId: Int): Unit = {
    val byMap = pending.get(shuffleId)
    if (byMap == null || byMap.isEmpty) {
      return
    }
    if (isStageEndOrInProcess(shuffleId)) {
      pending.remove(shuffleId)
      return
    }
    val registered = resolveRegisteredTaskIds(shuffleId).orNull
    if (registered == null) {
      return
    }
    byMap.asScala.foreach { case (mapId, attempts) =>
      val ended = endedAttempt(shuffleId, mapId)
      if (ended >= 0) {
        // Confirmed meanwhile (e.g. through the legacy path); duplicates of that attempt from
        // RPC retries are not rejections.
        if (byMap.remove(mapId, attempts)) {
          logRejected(shuffleId, attempts.values().asScala.filter(_.attemptId != ended), None)
        }
      } else if (mapId < registered.length && registered(mapId) >= 0) {
        val matched = attempts.get(registered(mapId))
        if (matched != null && byMap.remove(mapId, attempts)) {
          confirmPending(shuffleId, matched)
          logRejected(
            shuffleId,
            attempts.values().asScala.filter(_.taskAttemptId != matched.taskAttemptId),
            Some(matched))
        }
      }
    }
  }

  /**
   * Called right before StageEnd is forced (e.g. on unregisterShuffle). Confirms what can be
   * confirmed, then falls back to first-wins for maps the engine has no status for. Returns true
   * if the engine accepted an attempt for some map that never reported MapperEnd, in which case
   * the shuffle data can't be trusted.
   */
  def drainWithFallback(shuffleId: Int): Boolean = {
    check(shuffleId)
    val byMap = pending.remove(shuffleId)
    if (byMap == null || byMap.isEmpty) {
      return false
    }
    val registered = resolveRegisteredTaskIds(shuffleId).orNull
    var acceptedButUnseen = false
    byMap.asScala.foreach { case (mapId, attempts) =>
      if (endedAttempt(shuffleId, mapId) < 0 && !attempts.isEmpty) {
        val registeredTaskId =
          if (registered != null && mapId < registered.length) registered(mapId) else -1L
        if (registeredTaskId >= 0) {
          logError(s"Spark accepted task attempt $registeredTaskId for shuffle $shuffleId map " +
            s"$mapId, but only task attempts ${attempts.keySet().asScala.mkString(",")} " +
            s"reported MapperEnd.")
          acceptedButUnseen = true
        } else {
          val first = attempts.values().asScala.minBy(_.enqueuedNanos)
          logWarning(s"No Spark map status for shuffle $shuffleId map $mapId, falling back to " +
            s"the first finished attempt ${Utils.makeMapKey(shuffleId, mapId, first.attemptId)}.")
          confirmPending(shuffleId, first)
        }
      }
    }
    acceptedButUnseen
  }

  def remove(shuffleId: Int): Unit = {
    pending.remove(shuffleId)
  }

  private def tick(): Unit = {
    pending.keySet().asScala.foreach { shuffleId =>
      try {
        check(shuffleId)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to confirm pending MapperEnds for shuffle $shuffleId.", e)
      }
    }
  }

  private def confirmPending(shuffleId: Int, mapperEnd: PendingMapperEnd): Unit = {
    logDebug(s"Confirmed MapperEnd " +
      s"${Utils.makeMapKey(shuffleId, mapperEnd.mapId, mapperEnd.attemptId)} " +
      s"(task attempt ${mapperEnd.taskAttemptId}) after " +
      s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - mapperEnd.enqueuedNanos)}ms.")
    confirm(shuffleId, mapperEnd)
  }

  private def logRejected(
      shuffleId: Int,
      rejected: Iterable[PendingMapperEnd],
      confirmed: Option[PendingMapperEnd]): Unit = {
    rejected.foreach { r =>
      logWarning(s"Dropping MapperEnd ${Utils.makeMapKey(shuffleId, r.mapId, r.attemptId)} " +
        s"(task attempt ${r.taskAttemptId}): Spark accepted " +
        confirmed.map(c => s"attempt ${c.attemptId} (task attempt ${c.taskAttemptId})")
          .getOrElse("another attempt") + " for this map.")
    }
  }
}
