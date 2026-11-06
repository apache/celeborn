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

import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.protocol.message.ControlMessages.MapperEnd

class MapperEndConfirmerSuite extends CelebornFunSuite {

  private val shuffleId = 1
  private val numMappers = 3

  /** Minimal stand-in for the LifecycleManager side: first confirmed attempt per map wins. */
  private class Fixture {
    val registered = new ConcurrentHashMap[Int, Array[Long]]()
    val attempts = new ConcurrentHashMap[Int, Array[Int]]()
    val stageEnded = ConcurrentHashMap.newKeySet[Int]()
    val confirmed = new ArrayBuffer[(Int, PendingMapperEnd)]()
    val confirmCount = new AtomicInteger()

    val confirmer = new MapperEndConfirmer(
      intervalMs = 10,
      resolveRegisteredTaskIds = id => Option(registered.get(id)),
      endedAttempt = (id, mapId) => {
        val a = attempts.get(id)
        if (a == null) -1 else a(mapId)
      },
      isStageEndOrInProcess = id => stageEnded.contains(id),
      confirm = (id, p) => {
        confirmCount.incrementAndGet()
        confirmed.synchronized { confirmed += ((id, p)) }
        val a = attempts.computeIfAbsent(id, (_: Int) => Array.fill(numMappers)(-1))
        a.synchronized { if (a(p.mapId) < 0) a(p.mapId) = p.attemptId }
      })

    def add(mapId: Int, attemptId: Int, taskAttemptId: Long): Unit = {
      val request = MapperEnd(
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        -1,
        Collections.emptyMap(),
        0,
        Array.emptyIntArray,
        Array.emptyLongArray,
        SerdeVersion.V1,
        taskAttemptId = taskAttemptId)
      confirmer.add(shuffleId, PendingMapperEnd(request, System.nanoTime()))
    }

    def ended(mapId: Int): Int = {
      val a = attempts.get(shuffleId)
      if (a == null) -1 else a(mapId)
    }
  }

  test("does not confirm an attempt Spark has not registered") {
    val f = new Fixture
    f.add(mapId = 0, attemptId = 0, taskAttemptId = 100)
    f.confirmer.check(shuffleId)
    assert(f.confirmCount.get() == 0)

    f.registered.put(shuffleId, Array(-1L, -1L, -1L))
    f.confirmer.check(shuffleId)
    assert(f.confirmCount.get() == 0)
    assert(f.ended(0) == -1)
    assert(f.confirmer.hasPending(shuffleId))
  }

  test("confirms the registered attempt and drops the others for that map") {
    val f = new Fixture
    // attempt 0 reported MapperEnd but Spark failed it (e.g. preempted executor)
    f.add(mapId = 0, attemptId = 0, taskAttemptId = 100)
    f.add(mapId = 1, attemptId = 0, taskAttemptId = 101)
    f.registered.put(shuffleId, Array(-1L, 101L, -1L))
    f.confirmer.check(shuffleId)
    assert(f.ended(0) == -1)
    assert(f.ended(1) == 0)

    // retry of map 0 finishes and Spark registers it
    f.add(mapId = 0, attemptId = 1, taskAttemptId = 200)
    f.registered.put(shuffleId, Array(200L, 101L, -1L))
    f.confirmer.check(shuffleId)
    assert(f.ended(0) == 1)
    assert(f.confirmCount.get() == 2)
    assert(!f.confirmer.hasPending(shuffleId))
  }

  test("pending entries of an already ended map are dropped without confirming") {
    val f = new Fixture
    f.attempts.put(shuffleId, Array(5, -1, -1))
    f.add(mapId = 0, attemptId = 6, taskAttemptId = 300)
    f.registered.put(shuffleId, Array(300L, -1L, -1L))
    f.confirmer.check(shuffleId)
    assert(f.confirmCount.get() == 0)
    assert(f.ended(0) == 5)
    assert(!f.confirmer.hasPending(shuffleId))
  }

  test("stage end clears pending entries without confirming") {
    val f = new Fixture
    f.add(mapId = 0, attemptId = 0, taskAttemptId = 100)
    f.registered.put(shuffleId, Array(100L, -1L, -1L))
    f.stageEnded.add(shuffleId)
    f.confirmer.check(shuffleId)
    assert(f.confirmCount.get() == 0)
    assert(!f.confirmer.hasPending(shuffleId))
  }

  test("concurrent checks confirm each map exactly once") {
    val f = new Fixture
    (0 until numMappers).foreach(m => f.add(m, attemptId = 0, taskAttemptId = 100L + m))
    f.registered.put(shuffleId, Array(100L, 101L, 102L))

    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    try {
      (0 until threads).foreach { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            start.await()
            f.confirmer.check(shuffleId)
          }
        })
      }
      start.countDown()
      pool.shutdown()
      assert(pool.awaitTermination(10, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }
    assert(f.confirmCount.get() == numMappers)
    assert(f.confirmed.map(_._2.mapId).sorted == (0 until numMappers))
  }

  test("background thread confirms once Spark registers the attempt") {
    val f = new Fixture
    f.confirmer.start()
    try {
      f.add(mapId = 2, attemptId = 0, taskAttemptId = 42)
      f.registered.put(shuffleId, Array(-1L, -1L, 42L))
      val deadline = System.currentTimeMillis() + 5000
      while (f.ended(2) < 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(10)
      }
      assert(f.ended(2) == 0)
    } finally {
      f.confirmer.stop()
    }
  }

  test("drainWithFallback uses first-wins for maps without a Spark status") {
    val f = new Fixture
    f.add(mapId = 0, attemptId = 3, taskAttemptId = 103)
    f.add(mapId = 0, attemptId = 4, taskAttemptId = 104)
    f.add(mapId = 1, attemptId = 0, taskAttemptId = 110)
    f.registered.put(shuffleId, Array(-1L, 110L, -1L))
    assert(!f.confirmer.drainWithFallback(shuffleId))
    assert(f.ended(0) == 3)
    assert(f.ended(1) == 0)
    assert(!f.confirmer.hasPending(shuffleId))
  }

  test("drainWithFallback reports an accepted attempt that never reported MapperEnd") {
    val f = new Fixture
    f.add(mapId = 0, attemptId = 0, taskAttemptId = 100)
    f.registered.put(shuffleId, Array(999L, -1L, -1L))
    assert(f.confirmer.drainWithFallback(shuffleId))
    assert(f.ended(0) == -1)
    assert(f.confirmCount.get() == 0)
  }

  test("remove forgets the shuffle") {
    val f = new Fixture
    f.add(mapId = 0, attemptId = 0, taskAttemptId = 100)
    f.confirmer.remove(shuffleId)
    assert(!f.confirmer.hasPending(shuffleId))
    assert(f.confirmer.pendingCount == 0)
  }
}
