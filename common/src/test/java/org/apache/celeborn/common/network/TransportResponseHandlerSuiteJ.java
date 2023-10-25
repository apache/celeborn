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

package org.apache.celeborn.common.network;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.read.FetchRequestInfo;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushRequestInfo;

public class TransportResponseHandlerSuiteJ {
  @Test
  public void handleSuccessfulFetch() throws Exception {
    StreamChunkSlice streamChunkSlice = new StreamChunkSlice(1, 0);
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.FETCH_MODULE);
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handleFetchRequest(streamChunkSlice, handler, callback, 1, 1);

    handler.handle(new ChunkFetchSuccess(streamChunkSlice, new TestManagedBuffer(123)));
    verify(callback, times(1)).onSuccess(eq(0), any());
    assertOutstandingRequests(handler, "OutstandingFetchCount", 0);
  }

  @Test
  public void handleFailedFetch() throws Exception {
    StreamChunkSlice streamChunkSlice = new StreamChunkSlice(1, 0);
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.FETCH_MODULE);
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    FetchRequestInfo info = new FetchRequestInfo(System.currentTimeMillis() + 30000, callback);
    handler.addFetchRequest(streamChunkSlice, info);
    assertOutstandingRequests(handler, "OutstandingFetchCount", 1);

    handler.handle(new ChunkFetchFailure(streamChunkSlice, "some error msg"));
    verify(callback, times(1)).onFailure(eq(0), any());
    assertOutstandingRequests(handler, "OutstandingFetchCount", 0);
  }

  @Test
  public void clearAllOutstandingRequests() throws Exception {
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.DATA_MODULE);
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    FetchRequestInfo info = new FetchRequestInfo(System.currentTimeMillis() + 30000, callback);
    handler.addFetchRequest(new StreamChunkSlice(1, 0), info);
    handler.addFetchRequest(new StreamChunkSlice(1, 1), info);
    handler.addFetchRequest(new StreamChunkSlice(1, 2), info);
    assertOutstandingRequests(handler, "OutstandingFetchCount", 3);

    handler.handle(new ChunkFetchSuccess(new StreamChunkSlice(1, 0), new TestManagedBuffer(12)));
    handler.exceptionCaught(new Exception("duh duh duhhhh"));

    // should fail both b2 and b3
    verify(callback, times(1)).onSuccess(eq(0), any());
    verify(callback, times(1)).onFailure(eq(1), any());
    verify(callback, times(1)).onFailure(eq(2), any());
    assertOutstandingRequests(handler, "OutstandingFetchCount", 0);
  }

  @Test
  public void handleSuccessfulRPC() throws Exception {
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.RPC_MODULE);
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertOutstandingRequests(handler, "OutstandingRpcCount", 1);

    // This response should be ignored.
    handler.handle(new RpcResponse(54321, new NioManagedBuffer(ByteBuffer.allocate(7))));
    assertOutstandingRequests(handler, "OutstandingRpcCount", 1);

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new RpcResponse(12345, new NioManagedBuffer(resp)));
    verify(callback, times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
    assertOutstandingRequests(handler, "OutstandingRpcCount", 0);
  }

  @Test
  public void handleFailedRPC() throws Exception {
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.RPC_MODULE);
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertOutstandingRequests(handler, "OutstandingRpcCount", 1);

    handler.handle(new RpcFailure(54321, "uh-oh!")); // should be ignored
    assertOutstandingRequests(handler, "OutstandingRpcCount", 1);

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure(any());
    assertOutstandingRequests(handler, "OutstandingRpcCount", 0);
  }

  @Test
  public void handleSuccessfulPush() throws Exception {
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.DATA_MODULE);
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    PushRequestInfo info = new PushRequestInfo(System.currentTimeMillis() + 30000, callback);
    info.setChannelFuture(mock(ChannelFuture.class));
    handler.addPushRequest(12345, info);
    assertOutstandingRequests(handler, "OutstandingPushCount", 1);

    // This response should be ignored.
    handler.handle(new RpcResponse(54321, new NioManagedBuffer(ByteBuffer.allocate(7))));
    assertEquals(1, handler.numOutstandingRequests());

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new RpcResponse(12345, new NioManagedBuffer(resp)));
    verify(callback, times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
    assertOutstandingRequests(handler, "OutstandingPushCount", 0);
  }

  @Test
  public void handleFailedPush() throws Exception {
    TransportResponseHandler handler = createResponseHandler(TransportModuleConstants.DATA_MODULE);
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    PushRequestInfo info = new PushRequestInfo(System.currentTimeMillis() + 30000L, callback);
    info.setChannelFuture(mock(ChannelFuture.class));
    handler.addPushRequest(12345, info);
    assertOutstandingRequests(handler, "OutstandingPushCount", 1);

    handler.handle(new RpcFailure(54321, "uh-oh!")); // should be ignored
    assertOutstandingRequests(handler, "OutstandingPushCount", 1);

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure(any());
    assertOutstandingRequests(handler, "OutstandingPushCount", 0);
  }

  @Test
  public void multipleHandleFetchRequest() {
    StreamChunkSlice streamChunkSlice1 = new StreamChunkSlice(1, 0);
    CelebornConf celebornConf = new CelebornConf();
    AbstractSource source = createSource(celebornConf);
    TransportResponseHandler handler1 =
        createResponseHandler(TransportModuleConstants.FETCH_MODULE, celebornConf, source);
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handleFetchRequest(streamChunkSlice1, handler1, callback, 1, 1);

    TransportResponseHandler handler2 =
        createResponseHandler(TransportModuleConstants.FETCH_MODULE, celebornConf, source);
    handleFetchRequest(streamChunkSlice1, handler2, callback, 1, 2);

    StreamChunkSlice streamChunkSlice2 = new StreamChunkSlice(1, 1);
    handleFetchRequest(streamChunkSlice2, handler1, callback, 2, 3);
    handleFetchRequest(streamChunkSlice2, handler2, callback, 2, 4);
  }

  private AbstractSource createSource(CelebornConf celebornConf) {
    return new AbstractSource(celebornConf, "Worker") {
      @Override
      public String sourceName() {
        return "worker";
      }
    };
  }

  private TransportResponseHandler createResponseHandler(String module) {
    CelebornConf celebornConf = new CelebornConf();
    return createResponseHandler(module, celebornConf, createSource(celebornConf));
  }

  private TransportResponseHandler createResponseHandler(
      String module, CelebornConf celebornConf, AbstractSource source) {
    return new TransportResponseHandler(
        Utils.fromCelebornConf(celebornConf, module, 8), new LocalChannel(), source);
  }

  private void assertOutstandingRequests(
      TransportResponseHandler handler, String name, int expected) {
    assertEquals(expected, handler.numOutstandingRequests());
    assertEquals(expected, handler.source().getGauge(name).gauge().getValue());
  }

  private void handleFetchRequest(
      StreamChunkSlice streamChunkSlice,
      TransportResponseHandler handler,
      ChunkReceivedCallback callback,
      int expectedRequests,
      int expectedGauge) {
    FetchRequestInfo info = new FetchRequestInfo(System.currentTimeMillis() + 30000, callback);
    handler.addFetchRequest(streamChunkSlice, info);
    assertFetchRequests(handler, expectedRequests, expectedGauge);
  }

  private void assertFetchRequests(
      TransportResponseHandler handler, int expectedRequests, int expectedGauge) {
    assertEquals(expectedRequests, handler.numOutstandingRequests());
    assertEquals(
        expectedGauge, handler.source().getGauge("OutstandingFetchCount").gauge().getValue());
  }
}
