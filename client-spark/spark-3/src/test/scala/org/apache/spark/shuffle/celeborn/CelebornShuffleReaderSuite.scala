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

import java.io.IOException
import java.nio.file.Files
import java.util.HashSet
import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.apache.spark.{Dependency, ShuffleDependency, TaskContext}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{DummyShuffleClient, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.util.ThreadUtils

class CelebornShuffleReaderSuite extends AnyFunSuite {

  /**
   * Due to spark limitations, spark local mode can not test speculation tasks ,
   * test the method `checkAndReportFetchFailureForUpdateFileGroupFailure`
   */
  test("CELEBORN-1838 test check report fetch failure exceptions ") {
    val dependency = Mockito.mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handler = new CelebornShuffleHandle[Int, Int, Int](
      "APP",
      "HOST1",
      1,
      UserIdentifier.apply("a", "b"),
      0,
      true,
      1,
      dependency)
    val context = Mockito.mock(classOf[TaskContext])
    val metricReporter = Mockito.mock(classOf[ShuffleReadMetricsReporter])
    val conf = new CelebornConf()

    val tmpFile = Files.createTempFile("test", ".tmp").toFile
    mockStatic(classOf[ShuffleClient]).when(() =>
      ShuffleClient.get(any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new DummyShuffleClient(conf, tmpFile))

    val shuffleReader =
      new CelebornShuffleReader[Int, Int](handler, 0, 0, 0, 0, context, conf, metricReporter, null)

    val exception1: Throwable = new CelebornIOException("test1", new InterruptedException("test1"))
    val exception2: Throwable = new CelebornIOException("test2", new TimeoutException("test2"))
    val exception3: Throwable = new CelebornIOException("test3")
    val exception4: Throwable = new CelebornIOException("test4")

    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception1)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception2)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception3)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 1)
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception4)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 2)

  }

  test("create batch open stream clients in parallel per worker") {
    val worker0 = newLocation(0, "worker-0", 19098)
    val worker1 = newLocation(0, "worker-1", 19098)
    val worker0Client = Mockito.mock(classOf[TransportClient])
    val worker1Client = Mockito.mock(classOf[TransportClient])
    val streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool("test-create-client", 2, 60)
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)

    try {
      val clientsFuture = scala.concurrent.Future {
        CelebornShuffleReader.createClientsInParallel(
          Seq(
            worker0.hostAndFetchPort -> Seq(worker0),
            worker1.hostAndFetchPort -> Seq(worker1)),
          streamCreatorPool,
          location => {
            started.countDown()
            assert(started.await(5, TimeUnit.SECONDS))
            assert(release.await(5, TimeUnit.SECONDS))
            if (location eq worker0) worker0Client else worker1Client
          },
          (_, _, ex) => fail("Unexpected client creation failure", ex))
      }(scala.concurrent.ExecutionContext.global)

      assert(started.await(5, TimeUnit.SECONDS))
      release.countDown()
      val clients =
        scala.concurrent.Await.result(
          clientsFuture,
          scala.concurrent.duration.Duration(5, "seconds"))

      assert(clients(worker0.hostAndFetchPort) eq worker0Client)
      assert(clients(worker1.hostAndFetchPort) eq worker1Client)
    } finally {
      streamCreatorPool.shutdownNow()
    }
  }

  test("skip failed batch open stream client creation while keeping healthy workers") {
    val failedWorker = newLocation(0, "worker-0", 19098)
    val healthyWorker = newLocation(0, "worker-1", 19098)
    val healthyClient = Mockito.mock(classOf[TransportClient])
    val streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool("test-create-client", 2, 60)
    var failedHostPort: String = null

    try {
      val clients = CelebornShuffleReader.createClientsInParallel(
        Seq(
          failedWorker.hostAndFetchPort -> Seq(failedWorker),
          healthyWorker.hostAndFetchPort -> Seq(healthyWorker)),
        streamCreatorPool,
        location => {
          if (location eq failedWorker) throw new IOException("boom")
          healthyClient
        },
        (hostPort, _, _) => failedHostPort = hostPort)

      assert(failedHostPort === failedWorker.hostAndFetchPort)
      assert(!clients.contains(failedWorker.hostAndFetchPort))
      assert(clients(healthyWorker.hostAndFetchPort) eq healthyClient)
    } finally {
      streamCreatorPool.shutdownNow()
    }
  }

  test("attempt batch open stream client creation once per worker") {
    val locations = (0 until 100).map(id => newLocation(id, "worker-0", 19098))
    val streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool("test-create-client", 1, 60)
    val attempts = new AtomicInteger()
    val failures = new AtomicInteger()

    try {
      val clients = CelebornShuffleReader.createClientsInParallel(
        Seq(locations.head.hostAndFetchPort -> locations),
        streamCreatorPool,
        _ => {
          attempts.incrementAndGet()
          throw new IOException("boom")
        },
        (_, _, _) => failures.incrementAndGet())

      assert(clients.isEmpty)
      assert(attempts.get() === 1)
      assert(failures.get() === 1)
    } finally {
      streamCreatorPool.shutdownNow()
    }
  }

  test("attempt sequential batch open stream client creation once per worker endpoint") {
    val firstLocation = newLocation(0, "worker-0", 19098)
    val duplicateEndpoint = newLocation(1, "worker-0", 19098)
    val differentEndpoint = newLocation(2, "worker-0", 19099)
    val attemptedClientHostPorts = new HashSet[String]()
    val attempts = new AtomicInteger()
    val failures = new AtomicInteger()

    Seq(firstLocation, duplicateEndpoint, differentEndpoint).foreach { location =>
      val client = CelebornShuffleReader.tryCreateClientOncePerEndpoint(
        location,
        attemptedClientHostPorts,
        _ => {
          attempts.incrementAndGet()
          throw new IOException("boom")
        },
        _ => failures.incrementAndGet())
      assert(client.isEmpty)
    }

    assert(attempts.get() === 2)
    assert(failures.get() === 2)
  }

  test("propagate wrapped client creation interruption without reporting a worker failure") {
    val location = newLocation(0, "worker-0", 19098)
    val failureReported = new AtomicBoolean(false)
    val interruptedException = new InterruptedException("test")

    try {
      val exception = intercept[InterruptedException] {
        CelebornShuffleReader.tryCreateClient(
          location,
          _ => throw new IOException("wrapped", interruptedException),
          _ => failureReported.set(true))
      }

      assert(exception eq interruptedException)
      assert(Thread.currentThread().isInterrupted)
      assert(!failureReported.get())
    } finally {
      Thread.interrupted()
    }
  }

  test("propagate interruption across parallel client creation future boundary") {
    val location = newLocation(0, "worker-0", 19098)
    val interruptedException = new InterruptedException("test")
    val failureReported = new AtomicBoolean(false)
    val streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool("test-create-client", 1, 60)

    try {
      val exception = intercept[InterruptedException] {
        CelebornShuffleReader.createClientsInParallel(
          Seq(location.hostAndFetchPort -> Seq(location)),
          streamCreatorPool,
          _ => throw new IOException("wrapped", interruptedException),
          (_, _, _) => failureReported.set(true))
      }

      assert(exception eq interruptedException)
      assert(Thread.currentThread().isInterrupted)
      assert(!failureReported.get())
    } finally {
      Thread.interrupted()
      streamCreatorPool.shutdownNow()
    }
  }

  test("skip client creation when the thread is already interrupted") {
    val location = newLocation(0, "worker-0", 19098)
    val clientCreationAttempted = new AtomicBoolean(false)
    val failureReported = new AtomicBoolean(false)

    try {
      Thread.currentThread().interrupt()
      intercept[InterruptedException] {
        CelebornShuffleReader.tryCreateClient(
          location,
          _ => {
            clientCreationAttempted.set(true)
            Mockito.mock(classOf[TransportClient])
          },
          _ => failureReported.set(true))
      }

      assert(Thread.currentThread().isInterrupted)
      assert(!clientCreationAttempted.get())
      assert(!failureReported.get())
    } finally {
      Thread.interrupted()
    }
  }

  test("cancel batch open stream client creation when waiting thread is interrupted") {
    val blockedLocation = newLocation(0, "worker-0", 19098)
    val client = Mockito.mock(classOf[TransportClient])
    val streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool("test-create-client", 1, 60)
    val clientStarted = new CountDownLatch(1)
    val releaseClient = new CountDownLatch(1)
    val clientInterrupted = new CountDownLatch(1)
    val failureReported = new AtomicBoolean(false)
    val callerInterrupted = new AtomicBoolean(false)
    val callerFailure = new AtomicReference[Throwable]()

    val caller = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          CelebornShuffleReader.createClientsInParallel(
            Seq(blockedLocation.hostAndFetchPort -> Seq(blockedLocation)),
            streamCreatorPool,
            _ => {
              clientStarted.countDown()
              try {
                releaseClient.await()
                client
              } catch {
                case ex: InterruptedException =>
                  clientInterrupted.countDown()
                  throw ex
              }
            },
            (_, _, _) => failureReported.set(true))
          callerFailure.set(new AssertionError("Expected waiting thread to be interrupted"))
        } catch {
          case _: InterruptedException =>
            callerInterrupted.set(Thread.currentThread().isInterrupted)
          case ex: Throwable =>
            callerFailure.set(ex)
        }
      }
    })

    try {
      caller.start()
      assert(clientStarted.await(5, TimeUnit.SECONDS))
      caller.interrupt()
      caller.join(TimeUnit.SECONDS.toMillis(5))
      assert(!caller.isAlive)
      assert(clientInterrupted.await(5, TimeUnit.SECONDS))
      streamCreatorPool.shutdown()
      assert(streamCreatorPool.awaitTermination(5, TimeUnit.SECONDS))
      assert(callerFailure.get() == null)
      assert(callerInterrupted.get())
      assert(!failureReported.get())
    } finally {
      releaseClient.countDown()
      streamCreatorPool.shutdownNow()
      caller.interrupt()
      caller.join(TimeUnit.SECONDS.toMillis(5))
    }
  }

  private def newLocation(id: Int, host: String, fetchPort: Int): PartitionLocation =
    new PartitionLocation(id, 0, host, 0, 0, fetchPort, 0, PartitionLocation.Mode.PRIMARY)
}
