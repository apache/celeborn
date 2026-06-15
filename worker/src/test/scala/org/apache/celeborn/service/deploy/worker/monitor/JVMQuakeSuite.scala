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

package org.apache.celeborn.service.deploy.worker.monitor

import java.io.File
import java.util.concurrent.TimeUnit

import org.junit.Assert.assertTrue

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.util.JavaUtils

class JVMQuakeSuite extends CelebornFunSuite {

  test("Convert JVMStat timer ticks to nanoseconds") {
    assert(JVMQuake.ticksToNanos(1L, 1000000000L) === 1L)
    assert(JVMQuake.ticksToNanos(1000L, 1000L) === 1000000000L)
    assert(JVMQuake.ticksToNanos(1500L, 1000L) === 1500000000L)
    assert(JVMQuake.ticksToNanos(-1000L, 1000L) === -1000000000L)

    intercept[IllegalArgumentException] {
      JVMQuake.ticksToNanos(1L, 0L)
    }
  }

  test("[CELEBORN-1092] Introduce JVM monitoring in Celeborn Worker using JVMQuake") {
    val quake = new JVMQuake(new CelebornConf().set(WORKER_JVM_QUAKE_ENABLED.key, "true")
      .set(WORKER_JVM_QUAKE_RUNTIME_WEIGHT.key, "1")
      .set(WORKER_JVM_QUAKE_DUMP_THRESHOLD.key, "1s")
      .set(WORKER_JVM_QUAKE_KILL_THRESHOLD.key, "2s"))

    // Drive the GC "deficit" bucket deterministically rather than inducing real GC pressure:
    // feed a GC-time delta above the 1s dump threshold with no offsetting execution time, so the
    // heap dump is triggered exactly once. The previous version spun until real GC happened to
    // trip the threshold, which could (and did) hang indefinitely when the runner had enough
    // headroom that GC pauses never dominated runtime.
    assert(!quake.heapDumped)
    quake.checkAndDump(TimeUnit.SECONDS.toNanos(2), 0L)

    assertTrue(quake.heapDumped)
    val heapDump = new File(s"${quake.getHeapDumpSavePath}/${quake.dumpFile}")
    assert(heapDump.exists())
    JavaUtils.deleteRecursively(heapDump)
    JavaUtils.deleteRecursively(new File(quake.getHeapDumpLinkPath))
  }

  test("start() schedules monitoring and stop() tears it down without dumping") {
    val quake = new JVMQuake(new CelebornConf().set(WORKER_JVM_QUAKE_ENABLED.key, "true"))
    quake.start()
    quake.stop()
    assert(!quake.heapDumped)
  }
}
