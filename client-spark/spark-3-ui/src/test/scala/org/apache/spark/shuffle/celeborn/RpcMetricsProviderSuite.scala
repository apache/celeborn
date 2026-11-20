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

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleManager
import org.junit.{After, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.{mock, when}

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.rpc.RpcMetricsSnapshot

@RunWith(classOf[JUnit4])
class RpcMetricsProviderSuite {

  @After
  def clearSparkEnv(): Unit = {
    SparkEnv.set(null)
  }

  @Test
  def returnsEmptyWithoutLifecycleManager(): Unit = {
    SparkEnv.set(null)
    assertTrue(RpcMetricsProvider.snapshot().isEmpty)

    val env = mock(classOf[SparkEnv])
    when(env.shuffleManager).thenReturn(mock(classOf[ShuffleManager]))
    SparkEnv.set(env)
    assertTrue(RpcMetricsProvider.snapshot().isEmpty)
  }

  @Test
  def returnsSnapshotFromCelebornShuffleManager(): Unit = {
    val expected = metrics("expected")
    SparkEnv.set(environment(expected))
    assertEquals(Some(expected), RpcMetricsProvider.snapshot())
  }

  @Test
  def recoversAfterSnapshotFailure(): Unit = {
    val env = mock(classOf[SparkEnv])
    val shuffleManager = mock(classOf[SparkShuffleManager])
    val lifecycleManager = mock(classOf[LifecycleManager])
    when(env.shuffleManager).thenReturn(shuffleManager)
    when(shuffleManager.getLifecycleManager()).thenReturn(lifecycleManager)
    when(lifecycleManager.rpcMetricsSnapshot()).thenThrow(new RuntimeException("test failure"))

    SparkEnv.set(env)
    assertTrue(RpcMetricsProvider.snapshot().isEmpty)

    val expected = metrics("recovered")
    SparkEnv.set(environment(expected))
    assertEquals(Some(expected), RpcMetricsProvider.snapshot())
  }

  private def environment(snapshot: RpcMetricsSnapshot): SparkEnv = {
    val env = mock(classOf[SparkEnv])
    val shuffleManager = mock(classOf[SparkShuffleManager])
    val lifecycleManager = mock(classOf[LifecycleManager])
    when(env.shuffleManager).thenReturn(shuffleManager)
    when(shuffleManager.getLifecycleManager()).thenReturn(lifecycleManager)
    when(lifecycleManager.rpcMetricsSnapshot()).thenReturn(Some(snapshot))
    env
  }

  private def metrics(endpointName: String): RpcMetricsSnapshot =
    RpcMetricsSnapshot(endpointName, 0, 0, Seq.empty, 0L)
}
