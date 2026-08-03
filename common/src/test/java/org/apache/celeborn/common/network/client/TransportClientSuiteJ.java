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

package org.apache.celeborn.common.network.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.util.TransportConf;

public class TransportClientSuiteJ {

  private Channel channel;
  private EventLoop eventLoop;
  private TransportResponseHandler handler;
  private TransportClient client;

  @Before
  public void setUp() {
    TransportConf conf = new TransportConf("shuffle", new CelebornConf());
    channel = mock(Channel.class);
    eventLoop = mock(EventLoop.class);
    when(channel.eventLoop()).thenReturn(eventLoop);
    handler = new TransportResponseHandler(conf, channel);
    client = new TransportClient(channel, handler);
  }

  @Test
  public void isActiveFalseWhenEventLoopIsShuttingDown() {
    // SPARK-58292: a client whose netty event loop has terminated must not be treated as active,
    // even though the TCP channel still reports open/active (nothing can close it -- closing runs
    // on the now-dead loop).
    when(channel.isOpen()).thenReturn(true);
    when(channel.isActive()).thenReturn(true);

    // Live event loop -> active.
    when(eventLoop.isShuttingDown()).thenReturn(false);
    assertTrue(client.isActive());

    // Terminated event loop -> NOT active, so it will be evicted from the pool and not reused.
    when(eventLoop.isShuttingDown()).thenReturn(true);
    assertFalse(client.isActive());
  }

  @Test
  public void sendRpcFailsFastWhenEventLoopIsDead() {
    // SPARK-58292: a request written to a dead loop orphans, and unlike pushes and fetches an
    // outstanding RPC has no timeout checker to fall back on.
    when(eventLoop.isShuttingDown()).thenReturn(true);

    AtomicReference<Throwable> failure = new AtomicReference<>();
    client.sendRpc(ByteBuffer.allocate(8), new CapturingCallback(failure));

    assertNotNull("callback must be failed, not left outstanding", failure.get());
    assertFalse(handler.hasOutstandingRequests());
    // The request was never handed to the dead loop.
    verify(channel, never()).writeAndFlush(any());
  }

  @Test
  public void invalidatesClientWhenEventLoopIsDead() {
    // SPARK-58292: owners that hold a client for the lifetime of a stream (e.g. Flink's
    // CelebornBufferStream) never reacquire it from the factory, so marking it inactive is not
    // enough -- their in-flight callbacks must be failed synchronously or they wait forever.
    when(eventLoop.isShuttingDown()).thenReturn(false);

    // An RPC issued while the loop was still alive stays outstanding.
    AtomicReference<Throwable> failure = new AtomicReference<>();
    handler.addRpcRequest(1L, new CapturingCallback(failure));
    assertTrue(handler.hasOutstandingRequests());

    // A healthy client is left alone.
    client.invalidateIfEventLoopDead();
    assertTrue(handler.hasOutstandingRequests());
    assertNull(failure.get());

    when(eventLoop.isShuttingDown()).thenReturn(true);
    client.invalidateIfEventLoopDead();
    assertNotNull(failure.get());
    assertFalse(handler.hasOutstandingRequests());

    // Idempotent: a second sweep over an already drained handler is a no-op.
    client.invalidateIfEventLoopDead();
    assertFalse(handler.hasOutstandingRequests());
  }

  private static class CapturingCallback implements RpcResponseCallback {
    private final AtomicReference<Throwable> failure;

    CapturingCallback(AtomicReference<Throwable> failure) {
      this.failure = failure;
    }

    @Override
    public void onSuccess(ByteBuffer response) {}

    @Override
    public void onFailure(Throwable e) {
      failure.set(e);
    }
  }
}
