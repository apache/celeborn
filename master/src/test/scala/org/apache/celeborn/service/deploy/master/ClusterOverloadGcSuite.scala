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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo}

class ClusterOverloadGcSuite extends CelebornFunSuite {

  private def makeConf(enabled: Boolean, threshold: Double): CelebornConf = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.MASTER_CLUSTER_OVERLOAD_GC_ENABLED, enabled)
    conf.set(CelebornConf.MASTER_CLUSTER_OVERLOAD_GC_THRESHOLD, threshold)
    conf
  }

  private def makeWorker(host: String, totalSpace: Long, usableSpace: Long): WorkerInfo = {
    val worker = new WorkerInfo(host, 10001, 10002, 10003, 10004)
    val disk = new DiskInfo("/mnt/data", usableSpace, 0L, 0L, 0L)
    disk.totalSpace = totalSpace
    worker.updateThenGetDiskInfos(Map("/mnt/data" -> disk).asJava)
    worker
  }

  private def workersMap(workers: WorkerInfo*): java.util.Map[String, WorkerInfo] = {
    val m = new util.HashMap[String, WorkerInfo]()
    workers.foreach(w => m.put(w.toUniqueId, w))
    m
  }

  private def availableWorkers(workers: WorkerInfo*): java.util.Set[WorkerInfo] = {
    val s = ConcurrentHashMap.newKeySet[WorkerInfo]()
    workers.foreach(s.add)
    s
  }

  test("returns false when feature is disabled") {
    val conf = makeConf(enabled = false, threshold = 0.9)
    val w = makeWorker("host1", totalSpace = 1000L, usableSpace = 10L)
    assert(!Master.isClusterOverloaded(conf, workersMap(w), availableWorkers(w)))
  }

  test("returns false when cluster is below the threshold") {
    // 50% used — below 90% threshold
    val conf = makeConf(enabled = true, threshold = 0.9)
    val w = makeWorker("host1", totalSpace = 1000L, usableSpace = 500L)
    assert(!Master.isClusterOverloaded(conf, workersMap(w), availableWorkers(w)))
  }

  test("returns true when cluster is exactly at the threshold") {
    // 90% used, 10% free — exactly at the 90% threshold
    val conf = makeConf(enabled = true, threshold = 0.9)
    val w = makeWorker("host1", totalSpace = 1000L, usableSpace = 100L)
    assert(Master.isClusterOverloaded(conf, workersMap(w), availableWorkers(w)))
  }

  test("returns true when cluster exceeds the threshold") {
    // 95% used, 5% free — above 90% threshold
    val conf = makeConf(enabled = true, threshold = 0.9)
    val w = makeWorker("host1", totalSpace = 1000L, usableSpace = 50L)
    assert(Master.isClusterOverloaded(conf, workersMap(w), availableWorkers(w)))
  }

  test("returns false when totalCapacity is zero (no workers)") {
    val conf = makeConf(enabled = true, threshold = 0.9)
    assert(!Master.isClusterOverloaded(conf, workersMap(), availableWorkers()))
  }

  test("available workers can be a subset of all workers") {
    // Two workers; only one is available for free-capacity accounting.
    // total = 1000+1000 = 2000, free (available only) = 100 → 95% used → overloaded.
    val conf = makeConf(enabled = true, threshold = 0.9)
    val w1 = makeWorker("host1", totalSpace = 1000L, usableSpace = 100L)
    val w2 = makeWorker("host2", totalSpace = 1000L, usableSpace = 900L)
    // Only w1 is available (e.g. w2 is excluded/shutdown)
    assert(Master.isClusterOverloaded(conf, workersMap(w1, w2), availableWorkers(w1)))
  }

  test("not overloaded when enough free space across multiple workers") {
    // Each worker has 50% free → aggregate 50% used, well below 90%
    val conf = makeConf(enabled = true, threshold = 0.9)
    val w1 = makeWorker("host1", totalSpace = 1000L, usableSpace = 500L)
    val w2 = makeWorker("host2", totalSpace = 1000L, usableSpace = 500L)
    assert(!Master.isClusterOverloaded(conf, workersMap(w1, w2), availableWorkers(w1, w2)))
  }

  test("threshold of 1.0 is never exceeded when there is any free space") {
    val conf = makeConf(enabled = true, threshold = 1.0)
    val w = makeWorker("host1", totalSpace = 1000L, usableSpace = 1L)
    assert(!Master.isClusterOverloaded(conf, workersMap(w), availableWorkers(w)))
  }
}
