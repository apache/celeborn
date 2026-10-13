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

package org.apache.spark.shuffle.celeborn.events

import org.apache.spark.scheduler.SparkListenerEvent

import org.apache.celeborn.common.protocol.message.{PushWorkerStats, ReadMetrics, WorkerReadCost, WriteMetrics}

/**
 * Root for Celeborn UI events posted to the Spark listener bus. Defined in the
 *  spark-3 module (rather than spark-3-ui) so that [[org.apache.spark.shuffle.celeborn.SparkShuffleManager]]
 *  and [[org.apache.spark.shuffle.celeborn.CelebornShuffleFallbackPolicyRunner]] can post them
 *  without inverting the module dependency. Subclasses are case classes so they serialize into
 *  the Spark event log and can be replayed by the HistoryServer plugin.
 */
sealed trait CelebornEvent extends SparkListenerEvent

/** Posted once from the driver plugin at initialization with build/version info. */
case class CelebornBuildInfoEvent(info: Map[String, String]) extends CelebornEvent

/**
 * Posted from SparkShuffleManager.registerShuffle after a successful Celeborn slot assignment,
 *  carrying the shuffle -> worker topology. workers is a java.util.List (not Scala Seq) so the
 *  Java SparkShuffleManager can construct it without Scala collection interop.
 */
case class CelebornShuffleAssignmentEvent(
    appShuffleId: Int,
    celebornShuffleId: Int,
    workers: java.util.List[String],
    numPartitions: Int,
    timestamp: Long) extends CelebornEvent

/**
 * Posted from CelebornShuffleFallbackPolicyRunner when a shuffle falls back to
 *  SortShuffleManager, carrying a snapshot of per-policy fallback counts. Counts use
 *  java.util.Map so Java callers don't need Scala collection interop.
 */
case class CelebornFallbackEvent(
    fallbackCounts: java.util.Map[String, java.lang.Long],
    timestamp: Long) extends CelebornEvent

/**
 * Posted from LifecycleManager (via a driver-side callback registered by SparkShuffleManager)
 *  the first time a partition split / block-send-failure revive / stage retry is triggered.
 *  Uses AtomicBoolean dedup in LifecycleManager so only the first trigger per type is posted
 *  (mirrors Uniffle's postReassignTriggeredEvent). stageRetry may be left false (placeholder)
 *  if the fetch-failure rerun path is not wired.
 */
case class CelebornReassignEvent(
    partitionSplit: Boolean,
    reviveTriggered: Boolean,
    stageRetry: Boolean,
    timestamp: Long) extends CelebornEvent

/**
 * Posted from LifecycleManager.handleMapperEnd (via registerMapperEndMetricsCallback) when the
 *  executor populated write metrics. Carries the per-task write-path timing breakdown and the
 *  per-worker push stats; the listener accumulates them per-shuffle for the UI's Shuffle Write
 *  Times and Shuffle Servers sections.
 */
case class CelebornWriteMetricsEvent(
    shuffleId: Int,
    writeMetrics: WriteMetrics,
    pushWorkerStats: java.util.List[PushWorkerStats],
    timestamp: Long) extends CelebornEvent

/**
 * Posted from LifecycleManager's ReportShuffleReadMetrics handler (via registerReadMetricsCallback)
 *  when the executor reported read metrics. Carries the per-task read-path timing breakdown and
 *  per-worker read cost; the listener accumulates them per-shuffle for the UI's Shuffle Read Times
 *  and Shuffle Servers (read side) sections.
 */
case class CelebornReadMetricsEvent(
    shuffleId: Int,
    readMetrics: ReadMetrics,
    workerReadCosts: java.util.List[WorkerReadCost],
    timestamp: Long) extends CelebornEvent
