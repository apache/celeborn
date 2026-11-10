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

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import scala.collection.mutable

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages.ReadReducerPartitionEnd
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.JavaUtils

class LifecycleManagerIntegrityFailureSuite extends CelebornFunSuite {
  private val validationError = "AQE Partition 0 failed validation check"

  // Exercise real RPC dispatch and lifecycle decisions without starting a master or RPC threads.
  private def fixture(): (LifecycleManager, CommitManager, AtomicInteger) = {
    val manager = mock(classOf[LifecycleManager], CALLS_REAL_METHODS)
    val commit = mock(classOf[CommitManager])
    setField(manager, "conf", new CelebornConf())
    setField(
      manager,
      "shuffleIdMapping",
      JavaUtils.newConcurrentHashMap[
        Int,
        mutable.LinkedHashMap[String, (Int, Boolean)]]())
    setField(manager, "failedIntegrityShuffles", JavaUtils.newConcurrentHashMap[Int, String]())
    setField(manager, "shuffleIdGenerator", new AtomicInteger())
    setField(manager, "appShuffleDeterminateMap", JavaUtils.newConcurrentHashMap[Int, Boolean]())
    setField(
      manager,
      "celebornShuffleIdToAppShuffleIdMap",
      JavaUtils.newConcurrentHashMap[Int, Int]())
    setField(manager, "unregisterShuffleCallback", None)
    setField(manager, "validateCelebornShuffleIdForClean", None)
    manager.registerReportTaskShuffleFetchFailurePreCheck(new java.util.function.Function[
      java.lang.Long,
      Boolean] {
      override def apply(id: java.lang.Long): Boolean = true
    })
    setField(manager, "appShuffleTrackerCallback", None)
    setField(manager, "commitManager", commit)
    setField(manager, "shufflePartitionType", JavaUtils.newConcurrentHashMap[Int, PartitionType]())
    when(manager.getPartitionType(anyInt())).thenReturn(PartitionType.REDUCE)
    doNothing().when(manager).unregisterShuffle(anyInt())
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((true, ""))
    when(commit.areAllMapperAttemptsFinished(anyInt())).thenReturn(true)
    val callbacks = new AtomicInteger()
    manager.registerShuffleTrackerCallback(new Consumer[Integer] {
      override def accept(id: Integer): Unit = { callbacks.incrementAndGet() }
    })
    manager.registerAppShuffleDeterminate(10, determinate = false)
    (manager, commit, callbacks)
  }

  private def setField(manager: LifecycleManager, name: String, value: AnyRef): Unit = {
    val field = classOf[LifecycleManager].getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name)).get
    field.setAccessible(true)
    field.set(manager, value)
  }

  private def reply(manager: LifecycleManager, request: Any): AnyRef = {
    val context = mock(classOf[RpcCallContext])
    manager.receiveAndReply(context)(request)
    val response = ArgumentCaptor.forClass(classOf[Object])
    verify(context).reply(response.capture())
    response.getValue
  }

  private def shuffleId(
      manager: LifecycleManager,
      attempt: Int,
      appId: Int = 10): PbGetShuffleIdResponse = {
    reply(
      manager,
      PbGetShuffleId.newBuilder()
        .setAppShuffleId(appId)
        .setAppShuffleIdentifier(s"$appId-0-$attempt")
        .setIsShuffleWriter(true)
        .build()).asInstanceOf[PbGetShuffleIdResponse]
  }

  private def fetchFailure(manager: LifecycleManager, id: Int, appId: Int = 10): Boolean = {
    reply(
      manager,
      PbReportShuffleFetchFailure.newBuilder()
        .setAppShuffleId(appId).setShuffleId(id).build())
      .asInstanceOf[PbReportShuffleFetchFailureResponse].getSuccess
  }

  private def finish(
      manager: LifecycleManager,
      id: Int,
      end: Int): PbReadReducerPartitionEndResponse = {
    reply(manager, ReadReducerPartitionEnd(id, 0, if (end == 0) 2 else 0, end, 1, 100L))
      .asInstanceOf[PbReadReducerPartitionEndResponse]
  }

  Seq(1, 0).foreach { end =>
    test(
      s"partial integrity failure cannot trigger stage rerun or accept corrected reports, end=$end") {
      val (manager, commit, callbacks) = fixture()
      val id = shuffleId(manager, 0).getShuffleId
      when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
        .thenReturn((false, validationError))
      assert(finish(
        manager,
        id,
        end).getStatus == StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue)
      assert(!fetchFailure(manager, id))
      assert(callbacks.get() == 0)
      assert(!shuffleId(manager, 1).getSuccess)
      when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
        .thenReturn((true, ""))
      assert(finish(manager, id, end).getErrorMsg == validationError)
      verify(commit, times(1)).finishPartition(
        anyInt(),
        anyInt(),
        anyInt(),
        anyInt(),
        any[CommitMetadata])
    }
  }

  test("a physical generation allocated before the integrity failure cannot bypass it") {
    val (manager, commit, _) = fixture()
    val first = shuffleId(manager, 0).getShuffleId
    assert(fetchFailure(manager, first))
    val second = shuffleId(manager, 1).getShuffleId
    assert(second != first)
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((false, validationError))
    finish(manager, first, 1)
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((true, ""))
    assert(finish(manager, second, 1).getErrorMsg == validationError)
    assert(!fetchFailure(manager, second))
    assert(!shuffleId(manager, 2).getSuccess)
  }

  test("ordinary fetch failure and full-partition integrity failure retain stage rerun") {
    val (manager, commit, callbacks) = fixture()
    val first = shuffleId(manager, 0).getShuffleId
    assert(fetchFailure(manager, first))
    val second = shuffleId(manager, 1).getShuffleId
    assert(second != first)
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((false, "full-partition mismatch"))
    finish(manager, second, Integer.MAX_VALUE)
    assert(fetchFailure(manager, second))
    assert(shuffleId(manager, 2).getSuccess)
    assert(callbacks.get() == 2)
  }

  test("integrity failure does not block unrelated logical shuffles") {
    val (manager, commit, _) = fixture()
    val bad = shuffleId(manager, 0).getShuffleId
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((false, validationError))
    finish(manager, bad, 1)
    val unrelated = shuffleId(manager, 0, appId = 20)
    assert(unrelated.getSuccess)
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((true, ""))
    assert(finish(manager, unrelated.getShuffleId, 1).getStatus == StatusCode.SUCCESS.getValue)
    assert(fetchFailure(manager, unrelated.getShuffleId, appId = 20))
  }

  test("logical shuffle unregister removes its integrity failure") {
    val (manager, commit, _) = fixture()
    val first = shuffleId(manager, 0).getShuffleId
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((false, validationError))
    finish(manager, first, 1)
    manager.unregisterAppShuffle(10, hasMapping = true)
    assert(shuffleId(manager, 1).getSuccess)
    when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
      .thenReturn((true, ""))
    assert(finish(manager, first, 1).getStatus == StatusCode.SUCCESS.getValue)
  }
}
