package org.apache.celeborn.common.network.sasl;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;

import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

public class SaslTestBase {

  @BeforeClass
  public static void setup() {
    SecretRegistryImpl.getInstance().register(TEST_USER, TEST_SECRET);
  }

  static final String TEST_USER = "appId";
  static final String TEST_SECRET = "secret";

  void authHelper(
      TransportConf conf,
      TransportServerBootstrap serverBootstrap,
      TransportClientBootstrap clientBootstrap)
      throws Throwable {
    BaseMessageHandler rpcHandler = mock(BaseMessageHandler.class);
    doAnswer(
            invocation -> {
              RequestMessage message = (RequestMessage) invocation.getArguments()[1];
              RpcResponseCallback cb = (RpcResponseCallback) invocation.getArguments()[2];
              assertEquals("Ping", JavaUtils.bytesToString(message.body().nioByteBuffer()));
              cb.onSuccess(JavaUtils.stringToBytes("Pong"));
              return null;
            })
        .when(rpcHandler)
        .receive(
            any(TransportClient.class), any(RequestMessage.class), any(RpcResponseCallback.class));

    doReturn(true).when(rpcHandler).checkRegistered();

    try (SaslTestCtx ctx = new SaslTestCtx(conf, rpcHandler, serverBootstrap, clientBootstrap)) {
      ByteBuffer response =
          ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), TimeUnit.SECONDS.toMillis(10));
      assertEquals("Pong", JavaUtils.bytesToString(response));
    } finally {
      // There should be 2 terminated events; one for the client, one for the server.
      Throwable error = null;
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (deadline > System.nanoTime()) {
        try {
          verify(rpcHandler, times(2)).channelInactive(any(TransportClient.class));
          error = null;
          break;
        } catch (Throwable t) {
          error = t;
          TimeUnit.MILLISECONDS.sleep(10);
        }
      }
      if (error != null) {
        throw error;
      }
    }
  }

  static class SaslTestCtx implements AutoCloseable {

    final TransportClient client;
    final TransportServer server;
    final TransportContext ctx;

    SaslTestCtx(
        TransportConf conf,
        BaseMessageHandler rpcHandler,
        TransportServerBootstrap serverBootstrap,
        TransportClientBootstrap clientBootstrap)
        throws Exception {

      this.ctx = new TransportContext(conf, rpcHandler);
      this.server = ctx.createServer(Collections.singletonList(serverBootstrap));
      List<TransportClientBootstrap> clientBootstraps = new ArrayList<>();
      clientBootstraps.add(clientBootstrap);
      try {
        this.client =
            ctx.createClientFactory(clientBootstraps)
                .createClient(JavaUtils.getLocalHost(), server.getPort());
      } catch (Exception e) {
        close();
        throw e;
      }
    }

    @Override
    public void close() {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
    }
  }
}
