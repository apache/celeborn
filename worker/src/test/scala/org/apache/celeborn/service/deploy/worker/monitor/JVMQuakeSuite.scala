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

import scala.collection.mutable.ArrayBuffer

import org.junit.Assert.assertTrue

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.util.JavaUtils

class JVMQuakeSuite extends CelebornFunSuite {

  private val allocation = new ArrayBuffer[Array[Byte]]()

  override def afterEach(): Unit = {
    allocation.clear()
    System.gc()
  }

  test("[CELEBORN-1092] Introduce JVM monitoring in Celeborn Worker using JVMQuake") {
    val quake = new JVMQuake(new CelebornConf().set(WORKER_JVM_QUAKE_ENABLED.key, "true")
      .set(WORKER_JVM_QUAKE_RUNTIME_WEIGHT.key, "1")
      .set(WORKER_JVM_QUAKE_DUMP_THRESHOLD.key, "1s")
      .set(WORKER_JVM_QUAKE_KILL_THRESHOLD.key, "2s"))
    quake.start()
    allocateMemory(quake)
    quake.stop()

    assertTrue(quake.heapDumped)
    val heapDump = new File(s"${quake.getHeapDumpSavePath}/${quake.dumpFile}")
    assert(heapDump.exists())
    JavaUtils.deleteRecursively(heapDump)
    JavaUtils.deleteRecursively(new File(quake.getHeapDumpLinkPath))
  }

  def allocateMemory(quake: JVMQuake): Unit = {
    val capacity = 1024 * 100
    while (allocation.size * capacity < Runtime.getRuntime.maxMemory / 4) {
      val bytes = new Array[Byte](capacity)
      allocation.append(bytes)
    }
    while (quake.shouldHeapDump) {
      for (index <- allocation.indices) {
        val bytes = new Array[Byte](capacity)
        allocation(index) = bytes
      }
    }
  }
}
