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

package org.apache.celeborn.service.deploy.worker.storage

import scala.collection.mutable

/**
 * One queue per flusher thread. A writer's FlushNotifier identifies all its queued flushes.
 * Promotion and removal share a lock, so a task is returned exactly once and each writer's
 * submission order is preserved. The consumer releases the lock before doing any I/O.
 */
private[storage] class FlushTaskQueue(committingBatchSize: Int, normalBatchSize: Int) {
  require(committingBatchSize > 0 && normalBatchSize > 0)

  private class WriterQueue(val notifier: FlushNotifier) {
    val tasks = mutable.Queue.empty[FlushTask]
  }

  private val pending = mutable.LinkedHashMap.empty[FlushTask, WriterQueue]
  private val writers = mutable.HashMap.empty[FlushNotifier, WriterQueue]
  private val committing = mutable.LinkedHashSet.empty[WriterQueue]
  private var committingTasks = 0
  private var priorityCount = 0
  private var normalRemaining = 0

  def add(task: FlushTask): Unit = synchronized {
    val writer = writers.getOrElseUpdate(task.notifier, new WriterQueue(task.notifier))
    writer.tasks.enqueue(task)
    pending.put(task, writer)
    if (committing.contains(writer)) committingTasks += 1
    notifyAll()
  }

  // Called after the final flush is submitted, including when the final buffer was empty.
  def markCommitting(notifier: FlushNotifier): Unit = synchronized {
    writers.get(notifier).foreach { writer =>
      if (committing.add(writer)) committingTasks += writer.tasks.size
    }
    notifyAll()
  }

  @throws[InterruptedException]
  def take(): FlushTask = synchronized {
    while (pending.isEmpty) {
      priorityCount = 0
      normalRemaining = 0
      wait()
    }

    val task =
      if (committing.isEmpty) {
        priorityCount = 0
        normalRemaining = 0
        pending.head._1
      } else if (normalRemaining > 0) {
        normalRemaining -= 1
        pending.head._1
      } else {
        priorityCount += 1
        if (priorityCount == committingBatchSize) {
          priorityCount = 0
          normalRemaining = normalBatchSize
        }
        committing.head.tasks.front
      }

    val writer = pending.remove(task).get
    writer.tasks.dequeue()
    if (committing.contains(writer)) committingTasks -= 1
    if (writer.tasks.isEmpty) {
      committing.remove(writer)
      writers.remove(writer.notifier)
    }
    task
  }

  def size: Int = synchronized { pending.size }

  def committingWriterCount: Int = synchronized { committing.size }

  def committingTaskCount: Int = synchronized { committingTasks }
}
