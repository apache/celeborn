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

package org.apache.celeborn.common.network.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.OneWayMessage;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.RpcRequest;

public class TransportRequestHandlerSuiteJ {

  @Mock private Channel channel;

  @Mock private TransportClient reverseClient;

  @Mock private BaseMessageHandler msgHandler;

  private TransportRequestHandler requestHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(msgHandler.checkRegistered()).thenReturn(true);
    requestHandler = new TransportRequestHandler(channel, reverseClient, msgHandler);
  }

  @Test
  public void testHandleRpcRequest() {
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] {1});
    RpcRequest rpcRequest = new RpcRequest(1, new NettyManagedBuffer(buffer));
    requestHandler.handle(rpcRequest);
    verify(msgHandler).receive(eq(reverseClient), eq(rpcRequest), any());
    verify(msgHandler, times(0)).receive(eq(reverseClient), eq(rpcRequest));
    assertEquals(0, buffer.refCnt());
  }

  @Test
  public void testHandleOneWayMessage() {
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] {1});
    OneWayMessage oneWayMessage = new OneWayMessage(new NettyManagedBuffer(buffer));
    requestHandler.handle(oneWayMessage);
    verify(msgHandler).receive(eq(reverseClient), eq(oneWayMessage));
    verify(msgHandler, times(0)).receive(eq(reverseClient), eq(oneWayMessage), any());
    assertEquals(0, buffer.refCnt());
  }

  @Test
  public void testHandleOtherMessage() {
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] {1});
    PushData pushData =
        new PushData((byte) 0, "shuffleKey", "partitionId", new NettyManagedBuffer(buffer));
    requestHandler.handle(pushData);
    verify(msgHandler).receive(eq(reverseClient), eq(pushData));
    verify(msgHandler, times(0)).receive(eq(reverseClient), eq(pushData), any());
    assertEquals(0, buffer.refCnt());
  }
}
