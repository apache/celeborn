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

package org.apache.celeborn.service.deploy.master;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.sasl.registration.RegistrationRpcHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.SingleMasterMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HARaftServer;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.MetaHandler;

@RunWith(Parameterized.class)
public class MasterSecretRegistryImplSuiteJ {
  @Parameterized.Parameters(name = "ha={0}")
  public static Collection<Object[]> modes() {
    return Arrays.asList(new Object[] {false}, new Object[] {true});
  }

  @Parameterized.Parameter public boolean ha;

  private final CelebornConf conf =
      new CelebornConf()
          .set("celeborn.auth.enabled", "true")
          .set("celeborn.internal.port.enabled", "true");
  private final TransportConf transportConf = new TransportConf("rpc_service", conf);
  private final AtomicReference<ResourceProtos.ResourceResponse> lastMetaResponse =
      new AtomicReference<>();
  private AbstractMetaManager metadata;
  private MasterSecretRegistryImpl registry;

  @Before
  public void setUp() throws Exception {
    if (ha) {
      HAMasterMetaManager manager = new HAMasterMetaManager(null, conf, null, null);
      MetaHandler metaHandler = new MetaHandler(manager);
      HARaftServer server = mock(HARaftServer.class);
      when(server.isLeader()).thenReturn(true);
      // Keep the production metadata handler and wire response conversion, replacing only Raft I/O.
      when(server.submitRequest(any()))
          .thenAnswer(
              call -> {
                ResourceProtos.ResourceRequest request = call.getArgument(0);
                PbMetaRequestResponse response =
                    metaHandler.handleWriteRequest(PbMetaRequest.parseFrom(request.toByteArray()));
                ResourceProtos.ResourceResponse wireResponse =
                    ResourceProtos.ResourceResponse.parseFrom(response.toByteArray());
                lastMetaResponse.set(wireResponse);
                return wireResponse;
              });
      manager.setRatisServer(server);
      metadata = manager;
    } else {
      metadata = new SingleMasterMetaManager(null, conf, null, null);
    }
    registry = new MasterSecretRegistryImpl(false);
    registry.setMetadataHandler(metadata);
  }

  @Test
  public void testRegistryRejectsDuplicateRegistration() {
    registry.register("app", "original-secret");
    assertHaRegistrationResponse(true, ResourceProtos.Status.OK);
    for (String secret : Arrays.asList("original-secret", "different-secret")) {
      RuntimeException failure =
          assertThrows(RuntimeException.class, () -> registry.register("app", secret));
      assertTrue(failure.getMessage().contains("already registered"));
      assertHaRegistrationResponse(false, ResourceProtos.Status.ALREADY_EXISTS);
      assertEquals("original-secret", registry.getSecretKey("app"));
    }
    registry.register("other-app", "other-secret");
    assertHaRegistrationResponse(true, ResourceProtos.Status.OK);
    assertEquals("other-secret", registry.getSecretKey("other-app"));
  }

  private void assertHaRegistrationResponse(boolean success, ResourceProtos.Status status) {
    if (ha) {
      ResourceProtos.ResourceResponse response = lastMetaResponse.get();
      assertNotNull(response);
      assertEquals(success, response.getSuccess());
      assertEquals(status, response.getStatus());
    }
  }

  @Test
  public void testSequentialDuplicateDoesNotAuthenticateConnection() throws Exception {
    try (Connection first = new Connection(registry);
        Connection second = new Connection(registry)) {
      first.register("app", "original-secret");
      assertTrue(second.registerFailure("app", "different-secret").contains("already registered"));
      assertEquals("original-secret", registry.getSecretKey("app"));
      second.assertUnauthenticated();
    }
  }

  @Test
  public void testConcurrentDuplicateDoesNotAuthenticateConnection() throws Exception {
    CountDownLatch checkedAbsent = new CountDownLatch(1);
    CountDownLatch winnerRegistered = new CountDownLatch(1);
    MasterSecretRegistryImpl delayed =
        new MasterSecretRegistryImpl(false) {
          @Override
          public boolean isRegistered(String appId) {
            boolean registered = super.isRegistered(appId);
            assertFalse(registered);
            checkedAbsent.countDown();
            try {
              assertTrue(winnerRegistered.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
            return registered;
          }
        };
    delayed.setMetadataHandler(metadata);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection loser = new Connection(delayed);
        Connection winner = new Connection(registry)) {
      // Both connections see the app as absent; only the winner's atomic insertion may succeed.
      Future<String> failure = executor.submit(() -> loser.registerFailure("app", "losing-secret"));
      assertTrue(checkedAbsent.await(10, TimeUnit.SECONDS));
      winner.register("app", "winning-secret");
      winnerRegistered.countDown();
      assertTrue(failure.get(10, TimeUnit.SECONDS).contains("already registered"));
      assertHaRegistrationResponse(false, ResourceProtos.Status.ALREADY_EXISTS);
      assertEquals("winning-secret", registry.getSecretKey("app"));
      assertEquals("app", winner.client.getClientId());
      loser.assertUnauthenticated();
      RpcRequest businessRequest = new RpcRequest(2, new NioManagedBuffer(ByteBuffer.allocate(0)));
      RpcResponseCallback callback = mock(RpcResponseCallback.class);
      winner.handler.receive(winner.client, businessRequest, callback);
      verify(winner.delegate).receive(winner.client, businessRequest, callback);
    } finally {
      winnerRegistered.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private class Connection implements AutoCloseable {
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final TransportClient client =
        new TransportClient(channel, new TransportResponseHandler(transportConf, channel));
    private final BaseMessageHandler delegate = mock(BaseMessageHandler.class);
    private final RegistrationRpcHandler handler;

    Connection(SecretRegistry secretRegistry) {
      handler = new RegistrationRpcHandler(transportConf, channel, delegate, secretRegistry);
      exchange(
          MessageType.AUTHENTICATION_INITIATION_REQUEST,
          PbAuthenticationInitiationRequest.newBuilder()
              .setVersion("1.0")
              .setAuthEnabled(true)
              .build()
              .toByteArray());
      exchange(
          MessageType.SASL_REQUEST,
          PbSaslRequest.newBuilder()
              .setAuthType(PbAuthType.CLIENT_AUTH)
              .setMethod("ANONYMOUS")
              .build()
              .toByteArray());
    }

    ByteBuffer exchange(MessageType type, byte[] payload) {
      AtomicReference<ByteBuffer> response = new AtomicReference<>();
      handler.receive(
          client,
          new RpcRequest(
              1, new NioManagedBuffer(new TransportMessage(type, payload).toByteBuffer())),
          new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer result) {
              response.set(result);
            }

            @Override
            public void onFailure(Throwable cause) {
              throw new AssertionError(cause);
            }
          });
      Object outbound = channel.readOutbound();
      if (outbound instanceof RpcFailure) {
        assertNull(response.get());
        throw new IllegalStateException(((RpcFailure) outbound).errorString);
      }
      assertNotNull(response.get());
      return response.get();
    }

    void register(String appId, String secret) throws Exception {
      ByteBuffer response =
          exchange(
              MessageType.REGISTER_APPLICATION_REQUEST,
              PbRegisterApplicationRequest.newBuilder()
                  .setId(appId)
                  .setSecret(secret)
                  .build()
                  .toByteArray());
      PbRegisterApplicationResponse registered =
          TransportMessage.fromByteBuffer(response).getParsedPayload();
      assertTrue(registered.getStatus());
    }

    String registerFailure(String appId, String secret) {
      return assertThrows(IllegalStateException.class, () -> register(appId, secret)).getMessage();
    }

    void assertUnauthenticated() {
      assertNull(client.getClientId());
      // Neither request/reply nor one-way business traffic may reach the delegate after failure.
      assertThrows(
          IllegalStateException.class,
          () ->
              exchange(
                  MessageType.APPLICATION_META_REQUEST,
                  PbApplicationMetaRequest.newBuilder().setAppId("app").build().toByteArray()));
      assertThrows(
          SecurityException.class,
          () ->
              handler.receive(
                  client, new RpcRequest(2, new NioManagedBuffer(ByteBuffer.allocate(0)))));
      verifyNoInteractions(delegate);
    }

    @Override
    public void close() {
      handler.channelInactive(client);
      channel.finishAndReleaseAll();
    }
  }
}
