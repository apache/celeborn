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

package org.apache.spark.shuffle.celeborn

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVIndex, KVStore}

private[celeborn] case class AggregatedTaskInfoUIData(
    shuffleWriteBytes: Long,
    shuffleWriteTimeMs: Long,
    shuffleReadBytes: Long,
    shuffleFetchWaitTimeMs: Long,
    taskDurationMs: Long,
    // Bytes pushed before compression; 0 for records written by older versions.
    uncompressedShuffleBytes: Long = 0L,
    // Bytes pushed after compression; 0 for records written by older versions.
    compressedShuffleBytes: Long = 0L) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[AggregatedTaskInfoUIData].getName
}

private[celeborn] class CelebornPropertiesUIData(
    val info: Seq[(String, String)]) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornPropertiesUIData].getName
}

private[celeborn] class CelebornBuildInfoUIData(
    val info: Seq[(String, String)]) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornBuildInfoUIData].getName
}

private[celeborn] case class ShuffleAssignmentUIData(
    shuffleId: Int,
    appShuffleId: Int,
    workers: Int,
    slots: Int,
    minWorkerSlots: Int,
    maxWorkerSlots: Int) {

  @JsonIgnore
  @KVIndex
  def id: Int = shuffleId
}

/** Shuffles and slots of each worker, over all shuffles of the application. */
private[celeborn] case class WorkerSlotsUIData(
    shuffles: Long,
    workerShuffles: Map[String, Int],
    workerSlots: Map[String, Int]) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[WorkerSlotsUIData].getName
}

/**
 * Marker persisted during History Server replay when the application's recorded
 * `spark.plugins` or `spark.plugins.defaultList` contains [[CelebornPlugin]].
 * `setupUI` runs on a fresh plugin
 * instance (FsHistoryProvider loads plugins again after replay), so the opt-in
 * state has to be passed through the KVStore rather than in memory.
 */
private[celeborn] class CelebornExtensionEnabledUIData {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornExtensionEnabledUIData].getName
}

private[celeborn] class CelebornStatusStore(store: KVStore) {

  def aggregatedTaskInfo(): AggregatedTaskInfoUIData = {
    val kClass = classOf[AggregatedTaskInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => AggregatedTaskInfoUIData(0L, 0L, 0L, 0L, 0L, 0L)
    }
  }

  def celebornProperties(): CelebornPropertiesUIData = {
    val kClass = classOf[CelebornPropertiesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornPropertiesUIData(Seq.empty)
    }
  }

  def buildInfo(): CelebornBuildInfoUIData = {
    val kClass = classOf[CelebornBuildInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornBuildInfoUIData(Seq.empty)
    }
  }

  def shuffleAssignments(): Seq[ShuffleAssignmentUIData] = {
    Utils.tryWithResource(store.view(classOf[ShuffleAssignmentUIData]).closeableIterator()) {
      iter => iter.asScala.toList
    }
  }

  def workerSlots(): WorkerSlotsUIData = {
    val kClass = classOf[WorkerSlotsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => WorkerSlotsUIData(0L, Map.empty, Map.empty)
    }
  }

  def extensionEnabled(): Boolean = {
    val kClass = classOf[CelebornExtensionEnabledUIData]
    try {
      store.read(kClass, kClass.getName)
      true
    } catch {
      case _: NoSuchElementException => false
    }
  }
}
