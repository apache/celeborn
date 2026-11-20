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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

class FlushTaskQueueSuite extends AnyFunSuite {
  private def task(notifier: FlushNotifier): FlushTask =
    new FlushTask(null, notifier, false, null) {
      override def flush(copyBytes: Array[Byte]): Unit = {}
    }

  test("preserve FIFO order before any writer is committing") {
    val queue = new FlushTaskQueue(8, 4)
    val writers = Array.fill(2)(new FlushNotifier)
    val tasks = (0 until 10).map(i => task(writers(i % 2)))
    tasks.foreach(queue.add)
    assert(tasks.map(_ => queue.take()) == tasks)
    assert(queue.size == 0)
  }

  test("promote every pending task of a committing writer in file order") {
    val queue = new FlushTaskQueue(8, 4)
    val writer = new FlushNotifier
    val normal = task(new FlushNotifier)
    val priority = (0 until 3).map(_ => task(writer))
    queue.add(normal)
    priority.foreach(queue.add)
    queue.markCommitting(writer)
    queue.markCommitting(writer)
    assert(queue.committingWriterCount == 1)
    assert(queue.committingTaskCount == 3)
    assert(priority.map(_ => queue.take()) == priority)
    assert(queue.committingWriterCount == 0)
    assert(queue.committingTaskCount == 0)
    assert(queue.take() == normal)
    assert(queue.size == 0)
  }

  test("interleave FIFO batches while a committing writer still has pending tasks") {
    val queue = new FlushTaskQueue(2, 1)
    val writer = new FlushNotifier
    val normal = (0 until 3).map(_ => task(new FlushNotifier))
    val priority = (0 until 6).map(_ => task(writer))
    normal.foreach(queue.add)
    priority.foreach(queue.add)
    queue.markCommitting(writer)
    val expected = Seq(
      priority(0),
      priority(1),
      normal(0),
      priority(2),
      priority(3),
      normal(1),
      priority(4),
      priority(5),
      normal(2))
    assert(expected.map(_ => queue.take()) == expected)
    assert(queue.size == 0)
  }

  test("promotion after a partial drain preserves file order and empty writers retain no state") {
    val queue = new FlushTaskQueue(8, 4)
    val writer = new FlushNotifier
    val first = task(writer)
    queue.add(first)
    assert(queue.take() == first)
    queue.markCommitting(writer)
    assert(queue.committingWriterCount == 0)
    val normal = task(new FlushNotifier)
    val last = task(writer)
    queue.add(normal)
    queue.add(last)
    queue.markCommitting(writer)
    assert(queue.take() == last)
    assert(queue.take() == normal)
    assert(queue.committingTaskCount == 0)
  }

  test("concurrent producers and promotion neither lose nor duplicate flushes") {
    val queue = new FlushTaskQueue(8, 4)
    val writers = Array.fill(4)(new FlushNotifier)
    val tasks = writers.map(writer => (0 until 500).map(_ => task(writer)))
    val start = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(5)
    try {
      val reader = executor.submit(new Callable[Seq[FlushTask]] {
        override def call(): Seq[FlushTask] = {
          start.await()
          (0 until 2000).map(_ => queue.take())
        }
      })
      val producers = writers.indices.map { i =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            start.await()
            tasks(i).foreach(queue.add)
            queue.markCommitting(writers(i))
          }
        })
      }
      start.countDown()
      producers.foreach(_.get(10, TimeUnit.SECONDS))
      val received = reader.get(10, TimeUnit.SECONDS)
      assert(received.distinct.size == 2000)
      writers.indices.foreach { i =>
        assert(received.filter(_.notifier eq writers(i)) == tasks(i))
      }
      assert(queue.size == 0)
      assert(queue.committingTaskCount == 0)
      assert(queue.committingWriterCount == 0)
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  test("an idle consumer wakes for a new task and remains interruptible") {
    val queue = new FlushTaskQueue(8, 4)
    val executor = Executors.newSingleThreadExecutor()
    try {
      val received = executor.submit(new Callable[FlushTask] {
        override def call(): FlushTask = queue.take()
      })
      val expected = task(new FlushNotifier)
      queue.add(expected)
      assert(received.get(5, TimeUnit.SECONDS) == expected)
      executor.submit(new Callable[FlushTask] {
        override def call(): FlushTask = queue.take()
      })
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(5, TimeUnit.SECONDS))
    }
  }
}
