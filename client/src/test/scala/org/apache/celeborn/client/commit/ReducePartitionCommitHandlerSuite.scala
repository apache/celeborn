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

package org.apache.celeborn.client.commit

import java.nio.ByteBuffer
import java.util.concurrent.{ScheduledExecutorService, ThreadPoolExecutor}

import scala.collection.JavaConverters._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, eq => eqTo}
import org.mockito.Mockito.{mock, never, spy, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.{LifecycleManager, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.network.client.RpcResponseCallback
import org.apache.celeborn.common.network.protocol.{SerdeVersion, TransportMessage}
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.netty.{NettyRpcEnv, RemoteNettyRpcCallContext}
import org.apache.celeborn.common.util.JavaUtils

class ReducePartitionCommitHandlerSuite extends CelebornFunSuite {
  private val shuffleId = 1

  test("mark data lost when an allocated worker becomes unknown") {
    // Heartbeats may report a different WorkerInfo instance for the same worker identity.
    val handler = newHandler(worker("allocated"))
    assert(handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost when an unrelated worker becomes unknown") {
    val handler = newHandler(worker("unrelated"))
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost when unknown-worker detection is disabled") {
    val handler = newHandler(worker("allocated"), detectionEnabled = false)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost on an unknown worker when replication is enabled") {
    val handler = newHandler(worker("allocated"), replicationEnabled = true)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost for an allocated worker with another failure status") {
    val handler = newHandler(worker("allocated"), status = StatusCode.WORKER_UNRESPONSIVE)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost for a shuffle without allocated workers") {
    val handler = newHandler(worker("allocated"))
    assert(!handler.isStageDataLost(shuffleId + 1))
  }

  test("reuse a live broadcast instead of rebuilding the response on an rpc cache miss") {
    val cache = new BroadcastCache
    val handler = spy[ReducePartitionCommitHandler](newBroadcastHandler(cache))
    val remote = newRemoteContext()

    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)
    val handle = cache.currentHandle

    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)

    // The second miss must neither rebuild the response nor create another broadcast.
    verify(handler, times(1)).getMapperAttempts(shuffleId)
    verify(cache.lifecycleManager, times(1)).broadcastGetReducerFileGroupResponse(
      eqTo(shuffleId),
      any(classOf[GetReducerFileGroupResponse]))

    serializedMessagesOf(remote, 2).foreach {
      case response: GetReducerFileGroupResponse =>
        assert(response.status == StatusCode.SUCCESS)
        assert(response.broadcast.sameElements(handle))
        assert(response.fileGroup.isEmpty)
        assert(response.attempts.isEmpty)
      case other => fail(s"expected a broadcast response, got ${other.getClass.getName}")
    }
  }

  test("rebuild the response on an rpc cache miss after the broadcast was invalidated") {
    val cache = new BroadcastCache
    val handler = spy[ReducePartitionCommitHandler](newBroadcastHandler(cache))
    val remote = newRemoteContext()

    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)
    val firstHandle = cache.currentHandle

    cache.invalidate()
    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)

    // Nothing to reuse, so the response is built a second time and broadcast anew.
    verify(handler, times(2)).getMapperAttempts(shuffleId)
    verify(cache.lifecycleManager, times(2)).broadcastGetReducerFileGroupResponse(
      eqTo(shuffleId),
      any(classOf[GetReducerFileGroupResponse]))

    val secondHandle = cache.currentHandle
    assert(!secondHandle.sameElements(firstHandle))
    val replies =
      serializedMessagesOf(remote, 2).map(_.asInstanceOf[GetReducerFileGroupResponse])
    assert(replies.head.broadcast.sameElements(firstHandle))
    assert(replies.last.broadcast.sameElements(secondHandle))
  }

  test("reply with the whole response when the broadcast cannot be created") {
    val cache = new BroadcastCache(canCreate = false)
    val handler = newBroadcastHandler(cache)
    val remote = newRemoteContext()

    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)

    verify(cache.lifecycleManager, times(1)).broadcastGetReducerFileGroupResponse(
      eqTo(shuffleId),
      any(classOf[GetReducerFileGroupResponse]))
    assertWholeResponse(serializedMessagesOf(remote, 1).head)
  }

  test("reply with the whole response when broadcasting is disabled") {
    val cache = new BroadcastCache
    val handler = newBroadcastHandler(cache, broadcastEnabled = false)
    val remote = newRemoteContext()

    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)
    handler.handleGetReducerFileGroup(remote.context, shuffleId, SerdeVersion.V1)

    verify(cache.lifecycleManager, never()).cachedBroadcastGetReducerFileGroupResponse(anyInt())
    verify(cache.lifecycleManager, never()).broadcastGetReducerFileGroupResponse(
      anyInt(),
      any(classOf[GetReducerFileGroupResponse]))
    serializedMessagesOf(remote, 2).foreach(assertWholeResponse)
  }

  /** Asserts that a serialized message carries this shuffle's file group, not a broadcast. */
  private def assertWholeResponse(message: Any): Unit = {
    assert(message.isInstanceOf[TransportMessage], s"expected a transport message, got $message")
    val response = ControlMessages
      .fromTransportMessage(message.asInstanceOf[TransportMessage])
      .asInstanceOf[GetReducerFileGroupResponse]
    assert(response.status == StatusCode.SUCCESS)
    assert(response.attempts.sameElements(Array(-1, -1)))
    assert(response.broadcast.isEmpty)
  }

  /**
   * Stands in for SparkUtils' broadcast cache: creating a broadcast stores a new handle and
   * returns it, a lookup returns whatever is stored, and invalidate() drops it.
   */
  private class BroadcastCache(canCreate: Boolean = true) {
    private var handle: Array[Byte] = _
    private var created = 0

    val lifecycleManager: LifecycleManager = {
      val manager = mock(classOf[LifecycleManager])
      when(manager.broadcastGetReducerFileGroupResponse(
        anyInt(),
        any(classOf[GetReducerFileGroupResponse]))).thenAnswer(
        new Answer[Option[Array[Byte]]] {
          override def answer(invocation: InvocationOnMock): Option[Array[Byte]] = {
            if (!canCreate) {
              None
            } else {
              created += 1
              handle = s"broadcast-handle-$created".getBytes()
              Some(handle)
            }
          }
        })
      when(manager.cachedBroadcastGetReducerFileGroupResponse(anyInt())).thenAnswer(
        new Answer[Option[Array[Byte]]] {
          override def answer(invocation: InvocationOnMock): Option[Array[Byte]] = Option(handle)
        })
      manager
    }

    def invalidate(): Unit = handle = null

    def currentHandle: Array[Byte] = handle
  }

  private def newBroadcastHandler(
      cache: BroadcastCache,
      broadcastEnabled: Boolean = true): ReducePartitionCommitHandler = {
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_SHUFFLE_GET_REDUCER_FILE_GROUP_BROADCAST_ENABLED, broadcastEnabled)
      .set(CelebornConf.CLIENT_SHUFFLE_GET_REDUCER_FILE_GROUP_BROADCAST_MINI_SIZE, 0L)
      .set(CelebornConf.CLIENT_RPC_CACHE_EXPIRE_TIME, 0L)
    val handler = new ReducePartitionCommitHandler(
      "test-app",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      mock(classOf[ThreadPoolExecutor]),
      mock(classOf[ScheduledExecutorService]),
      cache.lifecycleManager)
    handler.registerShuffle(shuffleId, 2, isSegmentGranularityVisible = false, 2)
    handler.setStageEnd(shuffleId)
    handler
  }

  /** A remote rpc context with the serialized reply wired back to its callback. */
  private class RemoteContext {
    val serialized: ByteBuffer = ByteBuffer.wrap("serialized-reply".getBytes())
    val callback: RpcResponseCallback = mock(classOf[RpcResponseCallback])
    val nettyEnv: NettyRpcEnv = {
      val env = mock(classOf[NettyRpcEnv])
      when(env.serialize(any())).thenReturn(serialized)
      env
    }
    val context: RemoteNettyRpcCallContext = {
      val ctx = mock(classOf[RemoteNettyRpcCallContext])
      when(ctx.nettyEnv).thenReturn(nettyEnv)
      when(ctx.callback).thenReturn(callback)
      ctx
    }
  }

  private def newRemoteContext(): RemoteContext = new RemoteContext

  /** The messages handed to the rpc env for serialization, once each reply was sent back. */
  private def serializedMessagesOf(remote: RemoteContext, count: Int): Seq[Any] = {
    verify(remote.callback, times(count)).onSuccess(remote.serialized)
    val messages = ArgumentCaptor.forClass(classOf[Any])
    verify(remote.nettyEnv, times(count)).serialize(messages.capture())
    messages.getAllValues.asScala.toList
  }

  private def newHandler(
      excludedWorker: WorkerInfo,
      detectionEnabled: Boolean = true,
      replicationEnabled: Boolean = false,
      status: StatusCode = StatusCode.WORKER_UNKNOWN): ReducePartitionCommitHandler = {
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_SHUFFLE_DATA_LOST_ON_UNKNOWN_WORKER_ENABLED, detectionEnabled)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED, replicationEnabled)
    val allocatedWorker = worker("allocated")
    val workers = JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
    workers.put(allocatedWorker.toUniqueId, new ShufflePartitionLocationInfo(allocatedWorker))
    val allocatedWorkers = new ShuffleAllocatedWorkers()
    allocatedWorkers.put(shuffleId, workers)
    val statusTracker = new WorkerStatusTracker(conf, null)
    statusTracker.excludedWorkers.put(excludedWorker, (status, System.currentTimeMillis()))

    new ReducePartitionCommitHandler(
      "test-app",
      conf,
      allocatedWorkers,
      new CommittedPartitionInfo(),
      statusTracker,
      mock(classOf[ThreadPoolExecutor]),
      mock(classOf[ScheduledExecutorService]),
      mock(classOf[LifecycleManager]))
  }

  private def worker(host: String): WorkerInfo =
    new WorkerInfo(host, 1001, 1002, 1003, 1004)
}
