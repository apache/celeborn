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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.ControlMessages.MapperEnd
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}

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
 * @param resolveRegisteredTaskIds returns the registered task attempt IDs in the order of the
 *                                 requested map indexes, or None if the shuffle is unknown
 * @param endedAttempt             attempt id already confirmed for (shuffleId, mapId), or -1
 * @param isStageEndOrInProcess    whether StageEnd has started for the shuffle
 * @param confirm                  applies a confirmed attempt to the LifecycleManager
 */
private[client] class MapperEndConfirmer(
    intervalMs: Long,
    resolveRegisteredTaskIds: (Int, Array[Int]) => Option[Array[Long]],
    endedAttempt: (Int, Int) => Int,
    isStageEndOrInProcess: Int => Boolean,
    confirm: (Int, PendingMapperEnd) => Unit) extends Logging {

  private type PendingMaps = mutable.Map[Int, mutable.Map[Long, PendingMapperEnd]]
  // Each shuffle's map is also its lock. Keep confirmation and its commit-state update together
  // so readers racing the poller can wait for the applied state.
  private val pending = JavaUtils.newConcurrentHashMap[Int, PendingMaps]()

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
    val byMap = pending.computeIfAbsent(shuffleId, _ => mutable.HashMap.empty)
    byMap.synchronized {
      if ((pending.get(shuffleId) eq byMap) && !isStageEndOrInProcess(shuffleId)) {
        byMap.getOrElseUpdate(mapperEnd.mapId, mutable.HashMap.empty)
          .put(mapperEnd.taskAttemptId, mapperEnd)
      }
    }
  }

  def hasPending(shuffleId: Int): Boolean = {
    val byMap = pending.get(shuffleId)
    byMap != null && byMap.synchronized { byMap.nonEmpty }
  }

  def pendingCount: Int = pending.values().asScala.map { byMap =>
    byMap.synchronized { byMap.valuesIterator.map(_.size).sum }
  }.sum

  /** Confirm matching attempts and wait for any concurrent check of this shuffle to finish. */
  def check(shuffleId: Int): Unit = {
    val byMap = pending.get(shuffleId)
    if (byMap != null) {
      byMap.synchronized {
        if (pending.get(shuffleId) eq byMap) {
          if (isStageEndOrInProcess(shuffleId)) {
            pending.remove(shuffleId, byMap)
          } else {
            resolvePending(shuffleId, byMap, allowFallback = false)
          }
        }
      }
    }
  }

  /**
   * Before forcing StageEnd, confirm matching attempts and fall back for maps with no Spark
   * status. Return true if Spark accepted an attempt that did not report MapperEnd.
   */
  def drainWithFallback(shuffleId: Int): Boolean = {
    val byMap = pending.get(shuffleId)
    if (byMap == null) {
      return false
    }
    byMap.synchronized {
      if (!(pending.get(shuffleId) eq byMap)) {
        false
      } else {
        val dataLost = !isStageEndOrInProcess(shuffleId) &&
          resolvePending(shuffleId, byMap, allowFallback = true)
        pending.remove(shuffleId, byMap)
        dataLost
      }
    }
  }

  // Caller holds byMap's lock. Only ask the engine about maps with pending attempts.
  private def resolvePending(
      shuffleId: Int,
      byMap: PendingMaps,
      allowFallback: Boolean): Boolean = {
    if (byMap.isEmpty) {
      return false
    }
    val mapIndexes = byMap.keysIterator.toArray
    val registered = resolveRegisteredTaskIds(shuffleId, mapIndexes).orNull
    var acceptedButUnseen = false
    mapIndexes.indices.foreach { index =>
      val mapId = mapIndexes(index)
      val attempts = byMap(mapId)
      val ended = endedAttempt(shuffleId, mapId)
      if (ended >= 0) {
        byMap.remove(mapId)
        // Retries of the same MapperEnd are duplicates, not rejections.
        logRejected(shuffleId, attempts.values.filter(_.attemptId != ended), None)
      } else {
        val taskId = if (registered == null) -1L else registered(index)
        attempts.get(taskId) match {
          case Some(matched) if taskId >= 0 =>
            confirmPending(shuffleId, matched)
            byMap.remove(mapId)
            logRejected(
              shuffleId,
              attempts.values.filter(_.taskAttemptId != taskId),
              Some(matched))
          case _ if allowFallback && taskId >= 0 =>
            logError(s"Spark accepted task attempt $taskId for shuffle $shuffleId map $mapId, " +
              s"but only task attempts ${attempts.keysIterator.mkString(",")} reported MapperEnd.")
            acceptedButUnseen = true
          case _ if allowFallback =>
            val first = attempts.valuesIterator.minBy(_.enqueuedNanos)
            logWarning(s"No Spark map status for shuffle $shuffleId map $mapId, falling back to " +
              s"the first finished attempt ${Utils.makeMapKey(shuffleId, mapId, first.attemptId)}.")
            confirmPending(shuffleId, first)
            byMap.remove(mapId)
          case _ =>
        }
      }
    }
    acceptedButUnseen
  }

  def remove(shuffleId: Int): Unit = {
    val byMap = pending.get(shuffleId)
    if (byMap != null) {
      byMap.synchronized {
        pending.remove(shuffleId, byMap)
      }
    }
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
