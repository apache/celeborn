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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.WorkerStats

class WorkerStatsStoreSuite extends AnyFunSuite {
  private val worker = new WorkerInfo("host", 1, 2, 3, 4)
  private def stats(ratio: Double): WorkerStats =
    WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> ratio))

  test("keeps the most recently received snapshot for each worker") {
    val store = new WorkerStatsStore
    val other = new WorkerInfo("other", 1, 2, 3, 4)
    store.update(worker, stats(0.5))
    store.update(worker, stats(0.25))
    store.update(other, stats(0.75))
    assert(store.get(worker).contains(stats(0.25)))
    assert(store.get(other).contains(stats(0.75)))
  }

  test("removal clears the stored snapshot and allows subsequent reporting") {
    val store = new WorkerStatsStore
    store.update(worker, stats(0.5))
    store.remove(worker)
    assert(store.get(worker).isEmpty)
    store.update(worker, stats(0.25))
    assert(store.get(worker).contains(stats(0.25)))
  }
}
