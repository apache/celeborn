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
import java.lang.{Long => JLong}
import java.nio.file.Files
import java.util
import java.util.HashSet
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.JavaConverters._

import org.apache.spark.{Dependency, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.celeborn.ExceptionMakerHelper
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.mockito.Answers
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{DummyShuffleClient, ShuffleClient, ShuffleClientImpl}
import org.apache.celeborn.client.read.CelebornInputStream
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.{TransportClient, TransportClientFactory}
import org.apache.celeborn.common.protocol.{PartitionLocation, PbGetShuffleId, PbGetShuffleIdResponse, PbGetStageEnd, PbGetStageEndResponse, PbReportShuffleFetchFailure, PbReportShuffleFetchFailureResponse}
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetReducerFileGroup, GetReducerFileGroupResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
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
    val clientFactory = mockStatic(classOf[ShuffleClient])
    val shuffleReader =
      try {
        clientFactory.when(() =>
          ShuffleClient.get(any(), any(), any(), any(), any(), any(), any())).thenReturn(
          new DummyShuffleClient(conf, tmpFile))
        new CelebornShuffleReader[Int, Int](
          handler,
          0,
          0,
          0,
          0,
          context,
          conf,
          metricReporter,
          null)
      } finally {
        clientFactory.close()
      }

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

  test("open the stream on the peer of a fetch-excluded worker") {
    val primary = newLocation(0, "worker-0", 19098)
    val replica =
      new PartitionLocation(0, 0, "worker-1", 0, 0, 19098, 0, PartitionLocation.Mode.REPLICA)
    primary.setPeer(replica)
    replica.setPeer(primary)
    val excludedWorkers = new ConcurrentHashMap[String, JLong]()
    val shouldSkip = (location: PartitionLocation) =>
      CelebornInputStream.isExcluded(excludedWorkers, TimeUnit.MINUTES.toMillis(10), location)

    // Nothing excluded: the primary, or the replica for a task attempt that prefers it.
    assert(CelebornShuffleReader.chooseReadLocation(primary, false, shouldSkip) eq primary)
    assert(CelebornShuffleReader.chooseReadLocation(primary, true, shouldSkip) eq replica)

    excludedWorkers.put(primary.hostAndFetchPort, System.currentTimeMillis())
    assert(CelebornShuffleReader.chooseReadLocation(primary, false, shouldSkip) eq replica)
    assert(CelebornShuffleReader.chooseReadLocation(primary, true, shouldSkip) eq replica)

    excludedWorkers.clear()
    excludedWorkers.put(replica.hostAndFetchPort, System.currentTimeMillis())
    assert(CelebornShuffleReader.chooseReadLocation(primary, false, shouldSkip) eq primary)
    assert(CelebornShuffleReader.chooseReadLocation(primary, true, shouldSkip) eq primary)

    // Both excluded: keep the one excluded earlier, as CelebornInputStream does.
    excludedWorkers.put(primary.hostAndFetchPort, System.currentTimeMillis() - 1000)
    assert(CelebornShuffleReader.chooseReadLocation(primary, false, shouldSkip) eq primary)
    assert(CelebornShuffleReader.chooseReadLocation(primary, true, shouldSkip) eq primary)

    // Without a peer there is nothing to switch to.
    val single = newLocation(1, "worker-0", 19098)
    assert(CelebornShuffleReader.chooseReadLocation(single, false, shouldSkip) eq single)
  }

  test("select read locations for a partition") {
    val primary = newLocation(0, "worker-0", 19098)
    val replica =
      new PartitionLocation(0, 0, "worker-1", 0, 0, 19098, 0, PartitionLocation.Mode.REPLICA)
    primary.setPeer(replica)
    replica.setPeer(primary)
    val originLocations: util.Set[PartitionLocation] = new HashSet[PartitionLocation]()
    originLocations.add(primary)

    def select(
        client: ShuffleClient,
        pushReplicateEnabled: Boolean = true,
        splitSkewPartitionWithoutMapRange: Boolean = false,
        preferReplicaRead: Boolean = false): util.Set[PartitionLocation] =
      CelebornShuffleReader.selectReadLocations(
        originLocations,
        pushReplicateEnabled,
        splitSkewPartitionWithoutMapRange,
        preferReplicaRead,
        client)

    // Nothing excluded: the per-location lookup is skipped altogether.
    val noneExcluded = mock(classOf[ShuffleClient])
    assert(select(noneExcluded) eq originLocations)
    verify(noneExcluded, never()).shouldSkipFetchLocation(any[PartitionLocation]())
    assert(select(noneExcluded, preferReplicaRead = true).asScala.toSet == Set(replica))

    val primaryExcluded = mock(classOf[ShuffleClient])
    when(primaryExcluded.hasFetchExcludedWorkers).thenReturn(true)
    when(primaryExcluded.shouldSkipFetchLocation(primary)).thenReturn(true)
    assert(select(primaryExcluded).asScala.toSet == Set(replica))
    assert(select(primaryExcluded, pushReplicateEnabled = false) eq originLocations)
    // Skew partitions read without a map range keep their locations (CELEBORN-2032).
    assert(select(primaryExcluded, splitSkewPartitionWithoutMapRange = true) eq originLocations)
    assert(select(
      primaryExcluded,
      splitSkewPartitionWithoutMapRange = true,
      preferReplicaRead = true) eq originLocations)
  }

  test("batch open stream connects to the peer of a fetch-excluded worker") {
    val shuffleId = 3
    val partitionId = 0
    val primary =
      new PartitionLocation(
        partitionId,
        0,
        "worker-0",
        0,
        0,
        19098,
        0,
        PartitionLocation.Mode.PRIMARY)
    val replica =
      new PartitionLocation(
        partitionId,
        0,
        "worker-1",
        0,
        0,
        19098,
        0,
        PartitionLocation.Mode.REPLICA)
    primary.setPeer(replica)
    replica.setPeer(primary)
    val fileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    fileGroups.put(partitionId, util.Collections.singleton(primary))
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_FETCH_EXCLUDE_WORKER_ON_FAILURE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_FETCH_MAX_RETRIES_FOR_EACH_REPLICA.key, "1")
      .set("celeborn.data.io.retryWait", "0ms")

    // Every worker connection fails, so the read ends with an error; what matters is which worker
    // the batch open stream connects to first.
    val connectedHosts = new CopyOnWriteArrayList[String]()
    val workerFactory = mock(classOf[TransportClientFactory])
    when(workerFactory.createClient(anyString(), anyInt())).thenAnswer(new Answer[TransportClient] {
      override def answer(invocation: InvocationOnMock): TransportClient = {
        connectedHosts.add(invocation.getArgument[String](0))
        throw new IOException("Connection refused")
      }
    })
    val endpoint = mock(
      classOf[RpcEndpointRef],
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): AnyRef = {
          if (invocation.getMethod.getName != "askSync") {
            Answers.RETURNS_DEFAULTS.answer(invocation)
          } else {
            invocation.getArgument[AnyRef](0) match {
              case _: GetReducerFileGroup =>
                GetReducerFileGroupResponse(StatusCode.SUCCESS, fileGroups, Array(0))
              case _: PbGetShuffleId =>
                PbGetShuffleIdResponse.newBuilder().setShuffleId(shuffleId).setSuccess(true).build()
              case _: PbReportShuffleFetchFailure =>
                PbReportShuffleFetchFailureResponse.newBuilder().setSuccess(true).build()
              case _: PbGetStageEnd =>
                PbGetStageEndResponse.newBuilder().setStageEnd(true).build()
              case other => throw new AssertionError(s"Unexpected RPC: $other")
            }
          }
        }
      })
    val client = new ShuffleClientImpl("excluded-peer-test", conf, UserIdentifier("test", "test")) {
      override def getDataClientFactory(): TransportClientFactory = workerFactory
    }
    client.setupLifecycleManagerRef(endpoint)
    client.excludeFailedFetchLocation(
      primary.hostAndFetchPort,
      new CelebornIOException("Connecting to worker-0:19098 timed out (10000 ms)"))

    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(new JavaSerializer(new SparkConf(false)))
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.aggregator).thenReturn(None)
    val handle = new CelebornShuffleHandle[Int, Int, Int](
      "excluded-peer-test",
      "localhost",
      0,
      UserIdentifier("test", "test"),
      shuffleId,
      false,
      1,
      dependency)
    val context = mock(classOf[TaskContext])
    when(context.taskMetrics()).thenReturn(TaskMetrics.empty)
    try {
      val factory = mockStatic(classOf[ShuffleClient])
      val reader =
        try {
          factory.when(() => ShuffleClient.get(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(client)
          new CelebornShuffleReader[Int, Int](
            handle,
            partitionId,
            partitionId + 1,
            0,
            Int.MaxValue,
            context,
            conf,
            mock(classOf[ShuffleReadMetricsReporter]),
            new ExecutorShuffleIdTracker())
        } finally {
          factory.close()
        }

      intercept[Exception](reader.read().foreach(_ => ()))
      assert(!connectedHosts.isEmpty)
      assert(connectedHosts.get(0) === replica.getHost)
    } finally {
      client.shutdown()
    }
  }

  test("FetchFailureException message includes workerHost") {
    val workerHost = "celeborn-worker-1"
    val e = ExceptionMakerHelper.SHUFFLE_FETCH_FAILURE_EXCEPTION_MAKER
      .makeFetchFailureException(1, 2, 3, workerHost, new IOException("test"))
    assert(e.getMessage.contains(workerHost))
    assert(e.getMessage.contains(ExceptionMakerHelper.FETCH_FAILURE_ERROR_MSG))
  }

  private def newLocation(id: Int, host: String, fetchPort: Int): PartitionLocation =
    new PartitionLocation(id, 0, host, 0, 0, fetchPort, 0, PartitionLocation.Mode.PRIMARY)
}
