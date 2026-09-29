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

package org.apache.celeborn.service.deploy.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.buffer.MemoryChunkBuffers;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.network.security.AuthorizationRequest;
import org.apache.celeborn.common.network.security.SecurityOperation;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportRequestHandler;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.CreditStreamManager;

public class DataAuthorizationSuiteJ {
  private static final String SHUFFLE_KEY = "app-a-1";
  private static final String FILE_NAME = "0-0";
  private static final long STREAM_ID = 71L;
  private static final long REQUEST_ID = 19L;

  @BeforeClass
  public static void initializeMemoryManager() {
    MemoryManager.initialize(new CelebornConf());
  }

  @AfterClass
  public static void resetMemoryManager() {
    MemoryManager.reset();
  }

  @Test
  public void deniedRawPushReturnsRpcFailure() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      PushDataHandler handler = spy(new PushDataHandler(mock(WorkerSource.class)));
      doReturn(true).when(handler).checkRegistered();
      PushData push = new PushData((byte) 0, SHUFFLE_KEY, FILE_NAME, emptyBuffer());
      push.requestId = REQUEST_ID;
      fixture.dispatch(handler, push);
      fixture.assertRpcFailure();
    }
  }

  @Test
  public void deniedRawMergedPushReturnsRpcFailure() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      PushDataHandler handler = spy(new PushDataHandler(mock(WorkerSource.class)));
      doReturn(true).when(handler).checkRegistered();
      PushMergedData push =
          new PushMergedData(
              (byte) 0, SHUFFLE_KEY, new String[] {FILE_NAME}, new int[] {0}, emptyBuffer());
      push.requestId = REQUEST_ID;
      fixture.dispatch(handler, push);
      fixture.assertRpcFailure();
    }
  }

  @Test
  public void deniedLegacyOpenStreamReturnsRpcFailure() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      fixture.dispatch(
          fixture.fetch,
          new RpcRequest(
              REQUEST_ID,
              new NioManagedBuffer(
                  new OpenStream(SHUFFLE_KEY, FILE_NAME, 0, Integer.MAX_VALUE).toByteBuffer())));
      fixture.assertRpcFailure();
      assertEquals(0, fixture.fetch.chunkStreamManager().getStreamsCount());
      verify(fixture.fetch, never()).getRawFileInfo(anyString(), anyString());
    }
  }

  @Test
  public void deniedOpenStreamDoesNotRegisterOrRead() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.OPEN_STREAM,
              PbOpenStream.newBuilder()
                  .setShuffleKey(SHUFFLE_KEY)
                  .setFileName(FILE_NAME)
                  .setInitialCredit(1)
                  .build()));
      fixture.assertRpcFailure();
      assertEquals(0, fixture.fetch.chunkStreamManager().getStreamsCount());
      verify(fixture.fetch, never()).getRawFileInfo(anyString(), anyString());
    }
  }

  @Test
  public void deniedBatchOpenStreamDoesNotRegisterOrRead() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.BATCH_OPEN_STREAM,
              PbOpenStreamList.newBuilder()
                  .setShuffleKey(SHUFFLE_KEY)
                  .addFileName(FILE_NAME)
                  .addStartIndex(0)
                  .addEndIndex(Integer.MAX_VALUE)
                  .addReadLocalShuffle(true)
                  .build()));
      fixture.assertRpcFailure();
      assertEquals(0, fixture.fetch.chunkStreamManager().getStreamsCount());
      verify(fixture.fetch, never()).getRawFileInfo(anyString(), anyString());
    }
  }

  @Test
  public void deniedRawChunkFetchReturnsChunkFailure() throws Exception {
    assertChunkDenied(false);
  }

  @Test
  public void deniedProtobufChunkFetchReturnsChunkFailure() throws Exception {
    assertChunkDenied(true);
  }

  private void assertChunkDenied(boolean protobuf) throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      fixture.registerChunk();
      fixture.dispatch(fixture.fetch, chunkRequest(protobuf));
      Object response = fixture.channel.readOutbound();
      assertTrue(
          "Expected ChunkFetchFailure, got " + response, response instanceof ChunkFetchFailure);
      assertEquals(STREAM_ID, ((ChunkFetchFailure) response).streamChunkSlice.streamId);
      verify(fixture.buffers, never()).chunk(anyInt(), anyInt(), anyInt());
      assertNotNull(fixture.fetch.chunkStreamManager().getStreamState(STREAM_ID));
    }
  }

  @Test
  public void chunkStreamCanBeFetchedAfterAuthorizedReconnect() throws Exception {
    try (Fixture fixture = new Fixture("app-a");
        Fixture reconnected = new Fixture("app-a")) {
      fixture.registerChunk();
      reconnected.dispatch(fixture.fetch, chunkRequest(true));
      Object response = reconnected.channel.readOutbound();
      assertTrue(response instanceof ChunkFetchSuccess);
      ((ChunkFetchSuccess) response).body().release();
      verify(fixture.buffers).chunk(0, 0, 1);
    }
  }

  @Test
  public void missingChunkStreamReturnsChunkFailure() throws Exception {
    for (boolean protobuf : new boolean[] {false, true}) {
      try (Fixture fixture = new Fixture("app-a")) {
        fixture.dispatch(fixture.fetch, chunkRequest(protobuf));
        Object response = fixture.channel.readOutbound();
        assertTrue(
            "Expected ChunkFetchFailure, got " + response, response instanceof ChunkFetchFailure);
      }
    }
  }

  @Test
  public void deniedChunkEndKeepsStreamAndFileReference() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      fixture.registerChunk();
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamId(STREAM_ID)
                  .setStreamType(StreamType.ChunkStream)
                  .build()));
      fixture.assertRpcFailure();
      assertNotNull(fixture.fetch.chunkStreamManager().getStreamState(STREAM_ID));
      verify(fixture.fileInfo, never()).closeStream(anyLong());
    }
  }

  @Test
  public void deniedCreditAndSegmentRequestsDoNotMutateStream() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      CreditStreamManager streams = fixture.creditStream();
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.READ_ADD_CREDIT,
              PbReadAddCredit.newBuilder().setStreamId(STREAM_ID).setCredit(3).build()));
      fixture.assertRpcFailure();
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.NOTIFY_REQUIRED_SEGMENT,
              PbNotifyRequiredSegment.newBuilder()
                  .setStreamId(STREAM_ID)
                  .setRequiredSegmentId(4)
                  .setSubPartitionId(2)
                  .build()));
      fixture.assertRpcFailure();
      fixture.dispatch(
          fixture.fetch,
          rpc(
              MessageType.BUFFER_STREAM_END,
              PbBufferStreamEnd.newBuilder()
                  .setStreamId(STREAM_ID)
                  .setStreamType(StreamType.CreditStream)
                  .build()));
      fixture.assertRpcFailure();
      verify(streams, never()).addCredit(anyInt(), anyLong());
      verify(streams, never()).notifyRequiredSegment(anyInt(), anyLong(), anyInt());
      verify(streams, never()).notifyStreamEndByClient(anyLong());
    }
  }

  @Test
  public void deniedRawCreditControlClosesCallerWithoutMutation() throws Exception {
    for (RequestMessage request :
        new RequestMessage[] {new ReadAddCredit(STREAM_ID, 3), new BufferStreamEnd(STREAM_ID)}) {
      try (Fixture fixture = new Fixture("app-b")) {
        CreditStreamManager streams = fixture.creditStream();
        fixture.dispatch(fixture.fetch, request);
        assertFalse(
            "A denied control with no callback must fail the calling channel",
            fixture.channel.isActive());
        verify(streams, never()).addCredit(anyInt(), anyLong());
        verify(streams, never()).notifyStreamEndByClient(anyLong());
      }
    }
  }

  @Test
  public void externalPolicyCanAuthorizeDifferentApplicationId() throws Exception {
    try (Fixture fixture = new Fixture("app-b")) {
      List<AuthorizationRequest> requests = new ArrayList<>();
      fixture.client.setSecurityContext(requests::add);
      fixture.registerChunk();
      fixture.dispatch(fixture.fetch, chunkRequest(true));
      Object response = fixture.channel.readOutbound();
      assertTrue(response instanceof ChunkFetchSuccess);
      ((ChunkFetchSuccess) response).body().release();
      assertEquals(1, requests.size());
      assertEquals(SecurityOperation.CHUNK_FETCH, requests.get(0).getOperation());
      assertEquals("app-a", requests.get(0).getApplicationId());
      assertEquals(AuthorizationRequest.Scope.APPLICATION, requests.get(0).getScope());
    }
  }

  @Test
  public void pushOperationsAuthorizeApplicationOrServiceBeforeHandling() throws Exception {
    for (PbPartitionLocation.Mode mode :
        new PbPartitionLocation.Mode[] {
          PbPartitionLocation.Mode.Primary, PbPartitionLocation.Mode.Replica
        }) {
      try (Fixture fixture = new Fixture("app-b")) {
        List<AuthorizationRequest> requests = new ArrayList<>();
        fixture.client.setSecurityContext(
            request -> {
              requests.add(request);
              throw new SecurityException("policy denied push");
            });
        WorkerSource source = mock(WorkerSource.class);
        PushDataHandler handler = spy(new PushDataHandler(source));
        doReturn(true).when(handler).checkRegistered();
        PushData push =
            new PushData((byte) mode.getNumber(), SHUFFLE_KEY, FILE_NAME, emptyBuffer());
        push.requestId = REQUEST_ID;
        PushMergedData merged =
            new PushMergedData(
                (byte) mode.getNumber(),
                SHUFFLE_KEY,
                new String[] {FILE_NAME},
                new int[] {0},
                emptyBuffer());
        merged.requestId = REQUEST_ID;
        RequestMessage[] messages = {
          push,
          merged,
          rpc(
              MessageType.PUSH_DATA_HAND_SHAKE,
              PbPushDataHandShake.newBuilder()
                  .setMode(mode)
                  .setShuffleKey(SHUFFLE_KEY)
                  .setPartitionUniqueId(FILE_NAME)
                  .build()),
          rpc(
              MessageType.REGION_START,
              PbRegionStart.newBuilder()
                  .setMode(mode)
                  .setShuffleKey(SHUFFLE_KEY)
                  .setPartitionUniqueId(FILE_NAME)
                  .build()),
          rpc(
              MessageType.REGION_FINISH,
              PbRegionFinish.newBuilder()
                  .setMode(mode)
                  .setShuffleKey(SHUFFLE_KEY)
                  .setPartitionUniqueId(FILE_NAME)
                  .build()),
          rpc(
              MessageType.SEGMENT_START,
              PbSegmentStart.newBuilder()
                  .setMode(mode)
                  .setShuffleKey(SHUFFLE_KEY)
                  .setPartitionUniqueId(FILE_NAME)
                  .build())
        };
        String[] operations = {
          SecurityOperation.PUSH_DATA, SecurityOperation.PUSH_MERGED_DATA,
          SecurityOperation.PUSH_DATA_HANDSHAKE, SecurityOperation.REGION_START,
          SecurityOperation.REGION_FINISH, SecurityOperation.SEGMENT_START
        };
        for (int i = 0; i < messages.length; i++) {
          fixture.dispatch(handler, messages[i]);
          Object response = fixture.channel.readOutbound();
          assertTrue("Expected RpcFailure, got " + response, response instanceof RpcFailure);
          assertTrue(((RpcFailure) response).errorString.contains("policy denied push"));
          verify(source, never()).recordAppActiveConnection(any(), anyString());
          assertEquals(i + 1, requests.size());
          AuthorizationRequest request = requests.get(i);
          assertEquals(operations[i], request.getOperation());
          assertEquals("app-a", request.getApplicationId());
          assertEquals(
              mode == PbPartitionLocation.Mode.Primary
                  ? AuthorizationRequest.Scope.APPLICATION
                  : AuthorizationRequest.Scope.SERVICE,
              request.getScope());
        }
      }
    }
  }

  @Test
  public void chunkPolicyExceptionWithoutMessageStillReturnsFailure() throws Exception {
    try (Fixture fixture = new Fixture("app-a")) {
      fixture.registerChunk();
      fixture.client.setSecurityContext(
          request -> {
            throw new IllegalStateException();
          });
      fixture.dispatch(fixture.fetch, chunkRequest(true));
      Object response = fixture.channel.readOutbound();
      assertTrue(response instanceof ChunkFetchFailure);
      assertNotNull(((ChunkFetchFailure) response).errorString);
      verify(fixture.buffers, never()).chunk(anyInt(), anyInt(), anyInt());
    }
  }

  @Test
  public void rawCreditPolicyExceptionClosesCallerWithoutMutation() throws Exception {
    try (Fixture fixture = new Fixture("app-a")) {
      CreditStreamManager streams = fixture.creditStream();
      fixture.client.setSecurityContext(
          request -> {
            throw new IllegalArgumentException("policy unavailable");
          });
      fixture.dispatch(fixture.fetch, new ReadAddCredit(STREAM_ID, 3));
      assertFalse(fixture.channel.isActive());
      verify(streams, never()).addCredit(anyInt(), anyLong());
    }
  }

  @Test
  public void rawPushPolicyExceptionWithoutMessageReturnsEncodableFailure() throws Exception {
    for (boolean merged : new boolean[] {false, true}) {
      try (Fixture fixture = new Fixture("app-a")) {
        WorkerSource source = mock(WorkerSource.class);
        PushDataHandler handler = spy(new PushDataHandler(source));
        doReturn(true).when(handler).checkRegistered();
        fixture.client.setSecurityContext(
            request -> {
              throw new SecurityException();
            });
        RequestMessage request;
        if (merged) {
          PushMergedData push =
              new PushMergedData(
                  (byte) 0, SHUFFLE_KEY, new String[] {FILE_NAME}, new int[] {0}, emptyBuffer());
          push.requestId = REQUEST_ID;
          request = push;
        } else {
          PushData push = new PushData((byte) 0, SHUFFLE_KEY, FILE_NAME, emptyBuffer());
          push.requestId = REQUEST_ID;
          request = push;
        }
        fixture.dispatch(handler, request);
        Object response = fixture.channel.readOutbound();
        assertTrue("Expected RpcFailure, got " + response, response instanceof RpcFailure);
        RpcFailure failure = (RpcFailure) response;
        // EmbeddedChannel alone does not encode outbound messages. Exercise the actual codec too.
        ByteBuf encoded = Unpooled.buffer(failure.encodedLength());
        try {
          failure.encode(encoded);
          RpcFailure decoded = RpcFailure.decode(encoded);
          assertEquals(REQUEST_ID, decoded.requestId);
          assertTrue(decoded.errorString.contains("SecurityException"));
        } finally {
          encoded.release();
        }
        verify(source, never()).recordAppActiveConnection(any(), anyString());
      }
    }
  }

  @Test
  public void creditControlsRejectAnotherChannelEvenWithinSameApplication() throws Exception {
    for (RequestMessage request : creditControlRequests()) {
      try (Fixture owner = new Fixture("app-a");
          Fixture caller = new Fixture("app-a")) {
        CreditStreamManager streams = owner.ownedCreditStream(owner.channel);
        caller.dispatch(owner.fetch, request);
        if (request instanceof RpcRequest) {
          caller.assertRpcFailure();
        } else {
          assertFalse(caller.channel.isActive());
        }
        assertTrue(owner.channel.isActive());
        assertNotNull(streams.getStreams().get(STREAM_ID));
        verify(streams, never()).addCredit(anyInt(), anyLong());
        verify(streams, never()).notifyRequiredSegment(anyInt(), anyLong(), anyInt());
        verify(streams, never()).notifyStreamEndByClient(anyLong());
      }
    }
  }

  @Test
  public void creditControlsAllowTheOwningChannel() throws Exception {
    try (Fixture owner = new Fixture("app-a")) {
      CreditStreamManager streams = owner.ownedCreditStream(owner.channel);
      RequestMessage[] requests = creditControlRequests();
      for (int i = 0; i < requests.length; i++) {
        owner.dispatch(owner.fetch, requests[i]);
        Object response = owner.channel.readOutbound();
        if (i < 2) {
          assertTrue(response instanceof RpcResponse);
          assertEquals(REQUEST_ID, ((RpcResponse) response).requestId);
          ((RpcResponse) response).body().release();
        } else {
          // Stream end retains its existing no-response behavior, even in an RPC envelope.
          assertNull(response);
        }
        assertTrue(owner.channel.isActive());
      }
      verify(streams, times(2)).addCredit(3, STREAM_ID);
      verify(streams).notifyRequiredSegment(4, STREAM_ID, 2);
      verify(streams, times(2)).notifyStreamEndByClient(STREAM_ID);
    }
  }

  @Test
  public void metadataOnlyStreamCannotBeFetchedAsChunks() throws Exception {
    for (boolean protobuf : new boolean[] {false, true}) {
      try (Fixture fixture = new Fixture("app-a")) {
        // Local and DFS readers register a stream for closing the file, without chunk buffers.
        fixture.fetch.chunkStreamManager().registerStream(STREAM_ID, SHUFFLE_KEY, FILE_NAME);
        fixture.dispatch(fixture.fetch, chunkRequest(protobuf));
        Object response = fixture.channel.readOutbound();
        assertTrue(
            "Expected ChunkFetchFailure, got " + response, response instanceof ChunkFetchFailure);
        assertNotNull(fixture.fetch.chunkStreamManager().getStreamState(STREAM_ID));
      }
    }
  }

  private static RequestMessage[] creditControlRequests() throws Exception {
    return new RequestMessage[] {
      rpc(
          MessageType.READ_ADD_CREDIT,
          PbReadAddCredit.newBuilder().setStreamId(STREAM_ID).setCredit(3).build()),
      rpc(
          MessageType.NOTIFY_REQUIRED_SEGMENT,
          PbNotifyRequiredSegment.newBuilder()
              .setStreamId(STREAM_ID)
              .setRequiredSegmentId(4)
              .setSubPartitionId(2)
              .build()),
      rpc(
          MessageType.BUFFER_STREAM_END,
          PbBufferStreamEnd.newBuilder()
              .setStreamId(STREAM_ID)
              .setStreamType(StreamType.CreditStream)
              .build()),
      new ReadAddCredit(STREAM_ID, 3),
      new BufferStreamEnd(STREAM_ID)
    };
  }

  private static NioManagedBuffer emptyBuffer() {
    return new NioManagedBuffer(ByteBuffer.allocate(0));
  }

  private static RpcRequest rpc(MessageType type, GeneratedMessageV3 message) throws Exception {
    return new RpcRequest(
        REQUEST_ID,
        new NioManagedBuffer(new TransportMessage(type, message.toByteArray()).toByteBuffer()));
  }

  private static RequestMessage chunkRequest(boolean protobuf) throws Exception {
    StreamChunkSlice slice = new StreamChunkSlice(STREAM_ID, 0, 0, 1);
    return protobuf
        ? rpc(
            MessageType.CHUNK_FETCH_REQUEST,
            PbChunkFetchRequest.newBuilder().setStreamChunkSlice(slice.toProto()).build())
        : new ChunkFetchRequest(slice);
  }

  private static class OwnedCreditStreams extends CreditStreamManager {
    OwnedCreditStreams(Channel owner) {
      super(10, 10, 1, 32);
      getStreams().put(STREAM_ID, new StreamState(owner, SHUFFLE_KEY, 1024, null));
    }
  }

  private static class Fixture implements AutoCloseable {
    final EmbeddedChannel channel = new EmbeddedChannel();
    final TransportClient client =
        new TransportClient(channel, mock(TransportResponseHandler.class));
    final FetchHandler fetch;
    final MemoryChunkBuffers buffers = mock(MemoryChunkBuffers.class);
    final FileInfo fileInfo = mock(FileInfo.class);

    Fixture(String appId) {
      // Model an already bound native identity; these tests do not perform a SASL handshake.
      client.setClientId(appId);
      CelebornConf conf = new CelebornConf();
      fetch =
          spy(
              new FetchHandler(
                  conf,
                  Utils.fromCelebornConf(conf, TransportModuleConstants.FETCH_MODULE, 1),
                  mock(WorkerSource.class)));
      doReturn(true).when(fetch).checkRegistered();
      doReturn(fileInfo).when(fetch).getRawFileInfo(anyString(), anyString());
    }

    void registerChunk() {
      when(buffers.numChunks()).thenReturn(1);
      when(buffers.chunk(0, 0, 1))
          .thenReturn(new NioManagedBuffer(ByteBuffer.wrap(new byte[] {7})));
      fetch.chunkStreamManager().registerStream(STREAM_ID, SHUFFLE_KEY, buffers, FILE_NAME, null);
    }

    CreditStreamManager creditStream() {
      CreditStreamManager streams = mock(CreditStreamManager.class);
      when(streams.getStreamShuffleKey(STREAM_ID)).thenReturn(SHUFFLE_KEY);
      doReturn(streams).when(fetch).creditStreamManager();
      return streams;
    }

    CreditStreamManager ownedCreditStream(Channel owner) {
      CreditStreamManager streams = spy(new OwnedCreditStreams(owner));
      // Keep real resource/owner lookup; isolate the downstream reader and recycling side effects.
      doNothing().when(streams).addCredit(anyInt(), anyLong());
      doNothing().when(streams).notifyRequiredSegment(anyInt(), anyLong(), anyInt());
      doNothing().when(streams).notifyStreamEndByClient(anyLong());
      doReturn(streams).when(fetch).creditStreamManager();
      return streams;
    }

    void dispatch(BaseMessageHandler handler, RequestMessage request) {
      new TransportRequestHandler(channel, client, handler).handle(request);
    }

    void assertRpcFailure() {
      Object response = channel.readOutbound();
      assertTrue("Expected RpcFailure, got " + response, response instanceof RpcFailure);
      assertEquals(REQUEST_ID, ((RpcFailure) response).requestId);
    }

    @Override
    public void close() {
      Object response;
      while ((response = channel.readOutbound()) != null) {
        if (response instanceof Message && ((Message) response).body() != null) {
          ((Message) response).body().release();
        }
      }
      channel.finishAndReleaseAll();
    }
  }
}
