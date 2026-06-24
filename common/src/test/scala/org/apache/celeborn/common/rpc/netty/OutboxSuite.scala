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

package org.apache.celeborn.common.rpc.netty

import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.mockito.Mockito.{mock, when}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEnvStoppedException}

class OutboxSuite extends CelebornFunSuite {

  private def failureMessage(
      failure: AtomicReference[Throwable],
      failed: CountDownLatch): RpcOutboxMessage =
    RpcOutboxMessage(
      ByteBuffer.allocate(0),
      e => {
        failure.set(e)
        failed.countDown()
      },
      (_, _) => ())

  test("send after terminal stop uses the original cause") {
    val outbox = new Outbox(mock(classOf[NettyRpcEnv]), RpcAddress("localhost", 12345))
    val cause = new RpcEnvStoppedException()
    val failure = new AtomicReference[Throwable]()
    val failed = new CountDownLatch(1)

    outbox.stop(cause)
    outbox.send(failureMessage(failure, failed))

    assert(failed.await(10, TimeUnit.SECONDS))
    assert(failure.get() eq cause)
  }

  test("send after transient stop remains retryable") {
    val outbox = new Outbox(mock(classOf[NettyRpcEnv]), RpcAddress("localhost", 12345))
    val failure = new AtomicReference[Throwable]()
    val failed = new CountDownLatch(1)

    outbox.stop()
    outbox.send(failureMessage(failure, failed))

    assert(failed.await(10, TimeUnit.SECONDS))
    assert(failure.get().isInstanceOf[CelebornException])
    assert(failure.get().getMessage === "Message is dropped because Outbox is stopped")
  }

  test("default stop after RPC environment shutdown uses the terminal cause") {
    val nettyEnv = mock(classOf[NettyRpcEnv])
    val outbox = new Outbox(nettyEnv, RpcAddress("localhost", 12345))
    val failure = new AtomicReference[Throwable]()
    val failed = new CountDownLatch(1)
    when(nettyEnv.isStopped).thenReturn(true)

    outbox.stop()
    outbox.send(failureMessage(failure, failed))

    assert(failed.await(10, TimeUnit.SECONDS))
    assert(failure.get().isInstanceOf[RpcEnvStoppedException])
  }
}
