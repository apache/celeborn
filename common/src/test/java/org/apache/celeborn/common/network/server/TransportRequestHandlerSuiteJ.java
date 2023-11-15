package org.apache.celeborn.common.network.server;

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
    assert buffer.refCnt() == 0;
  }

  @Test
  public void testHandleOneWayMessage() {
    when(msgHandler.checkRegistered()).thenReturn(true);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] {1});
    OneWayMessage oneWayMessage = new OneWayMessage(new NettyManagedBuffer(buffer));
    requestHandler.handle(oneWayMessage);
    verify(msgHandler).receive(eq(reverseClient), eq(oneWayMessage));
    verify(msgHandler, times(0)).receive(eq(reverseClient), eq(oneWayMessage), any());
    assert buffer.refCnt() == 0;
  }
}
