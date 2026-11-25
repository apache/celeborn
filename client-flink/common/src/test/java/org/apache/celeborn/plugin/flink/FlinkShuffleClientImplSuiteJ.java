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

package org.apache.celeborn.plugin.flink;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;

public class FlinkShuffleClientImplSuiteJ {
  static int BufferSize = 64;
  static byte[] TEST_BUF1 = new byte[BufferSize];
  protected ChannelFuture mockedFuture = mock(ChannelFuture.class);
  static CelebornConf conf;
  static FlinkShuffleClientImpl shuffleClient;
  protected static final TransportClientFactory clientFactory = mock(TransportClientFactory.class);
  protected final TransportClient client = mock(TransportClient.class);
  protected static final PartitionLocation primaryLocation =
      new PartitionLocation(0, 1, "localhost", 1, 1, 1, 1, PartitionLocation.Mode.PRIMARY);

  @Before
  public void setup() throws IOException, InterruptedException {
    conf = new CelebornConf();
    shuffleClient =
        new FlinkShuffleClientImpl(
            "APP", "localhost", 1232, System.currentTimeMillis(), conf, null, -1) {
          @Override
          public void setupLifecycleManagerRef(String host, int port) {}
        };
    when(clientFactory.createClient(primaryLocation.getHost(), primaryLocation.getPushPort(), 1))
        .thenAnswer(t -> client);
    when(clientFactory.createClient(
            primaryLocation.getHost(), primaryLocation.getPushPort(), primaryLocation.getId()))
        .thenReturn(client);

    shuffleClient.setDataClientFactory(clientFactory);
  }

  public ByteBuf createByteBuf() {
    for (int i = 16; i < BufferSize; i++) {
      TEST_BUF1[i] = 1;
    }
    ByteBuf byteBuf = Unpooled.wrappedBuffer(TEST_BUF1);
    byteBuf.writerIndex(BufferSize);
    return byteBuf;
  }

  @Test
  public void testPushDataByteBufSuccess() throws IOException {
    ByteBuf byteBuf = createByteBuf();
    when(client.pushData(any(), anyLong(), any()))
        .thenAnswer(
            t -> {
              RpcResponseCallback rpcResponseCallback = t.getArgument(1, RpcResponseCallback.class);
              ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[0]);
              rpcResponseCallback.onSuccess(byteBuffer);
              return mockedFuture;
            });

    int pushDataLen =
        shuffleClient.pushDataToLocation(2, 3, 4, 5, byteBuf, primaryLocation, () -> {});
    Assert.assertEquals(BufferSize, pushDataLen);
  }

  @Test
  public void testPushDataByteBufHardSplit() throws IOException {
    ByteBuf byteBuf = Unpooled.wrappedBuffer(TEST_BUF1);
    when(client.pushData(any(), anyLong(), any()))
        .thenAnswer(
            t -> {
              RpcResponseCallback rpcResponseCallback = t.getArgument(1, RpcResponseCallback.class);
              ByteBuffer byteBuffer =
                  ByteBuffer.wrap(new byte[] {StatusCode.HARD_SPLIT.getValue()});
              rpcResponseCallback.onSuccess(byteBuffer);
              return mockedFuture;
            });
    int pushDataLen =
        shuffleClient.pushDataToLocation(2, 3, 4, 5, byteBuf, primaryLocation, () -> {});
    Assert.assertEquals(BufferSize, pushDataLen);
  }

  @Test
  public void testPushDataByteBufFail() throws IOException {
    ByteBuf byteBuf = Unpooled.wrappedBuffer(TEST_BUF1);
    when(client.pushData(any(), anyLong(), any(), any()))
        .thenAnswer(
            t -> {
              RpcResponseCallback rpcResponseCallback = t.getArgument(1, RpcResponseCallback.class);
              rpcResponseCallback.onFailure(new Exception("pushDataFailed"));
              return mockedFuture;
            });
    // first push just  set pushdata.exception
    shuffleClient.pushDataToLocation(2, 3, 4, 5, byteBuf, primaryLocation, () -> {});

    boolean isFailed = false;
    // second push will throw exception
    try {
      shuffleClient.pushDataToLocation(2, 3, 4, 5, byteBuf, primaryLocation, () -> {});
    } catch (IOException e) {
      isFailed = true;
    } finally {
      Assert.assertTrue("should failed", isFailed);
    }
  }

  @Test
  public void testRegionFinishWithoutRetries() throws IOException {
    conf.set("celeborn.push.io.maxRetries", "0");
    shuffleClient.regionFinish(2, 3, 4, primaryLocation);
    verify(client, times(1)).sendRpcSync(any(), anyLong());
  }

  @Test
  public void testRegionFinishFailureWithoutRetries() throws IOException {
    conf.set("celeborn.push.io.maxRetries", "0");
    IOException failure = new IOException("region finish failed");
    when(client.sendRpcSync(any(), anyLong())).thenThrow(failure);

    Assert.assertSame(
        failure,
        Assert.assertThrows(
            IOException.class, () -> shuffleClient.regionFinish(2, 3, 4, primaryLocation)));
    verify(client, times(1)).sendRpcSync(any(), anyLong());
  }

  @Test
  public void testRegionFinishRetries() throws IOException {
    conf.set("celeborn.push.io.maxRetries", "1");
    conf.set("celeborn.push.io.retryWait", "0ms");
    when(client.sendRpcSync(any(), anyLong()))
        .thenThrow(new IOException("first attempt failed"))
        .thenReturn(ByteBuffer.allocate(0));

    shuffleClient.regionFinish(2, 3, 4, primaryLocation);
    verify(client, times(2)).sendRpcSync(any(), anyLong());
  }

  @Test
  public void testRegionFinishExhaustsRetries() throws IOException {
    conf.set("celeborn.push.io.maxRetries", "3");
    conf.set("celeborn.push.io.retryWait", "0ms");
    IOException failure = new IOException("region finish failed");
    when(client.sendRpcSync(any(), anyLong())).thenThrow(failure);

    Assert.assertSame(
        failure,
        Assert.assertThrows(
            IOException.class, () -> shuffleClient.regionFinish(2, 3, 4, primaryLocation)));
    verify(client, times(4)).sendRpcSync(any(), anyLong());
  }

  @Test
  public void testRegionFinishDoesNotRetryNonIoFailure() throws IOException {
    conf.set("celeborn.push.io.retryWait", "0ms");
    IllegalStateException failure = new IllegalStateException("region finish failed");
    when(client.sendRpcSync(any(), anyLong())).thenThrow(failure);

    IOException thrown =
        Assert.assertThrows(
            IOException.class, () -> shuffleClient.regionFinish(2, 3, 4, primaryLocation));
    Assert.assertSame(failure, thrown.getCause());
    verify(client, times(1)).sendRpcSync(any(), anyLong());
  }

  @Test
  public void testRegionFinishWhenAlreadyInterrupted() throws IOException {
    Thread.currentThread().interrupt();
    try {
      IOException thrown =
          Assert.assertThrows(
              IOException.class, () -> shuffleClient.regionFinish(2, 3, 4, primaryLocation));
      Assert.assertTrue(thrown.getCause() instanceof InterruptedException);
      Assert.assertTrue(Thread.currentThread().isInterrupted());
      verify(client, times(0)).sendRpcSync(any(), anyLong());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testRegionFinishInterruptedAfterFailure() throws IOException {
    conf.set("celeborn.push.io.retryWait", "0ms");
    IOException failure = new IOException("region finish failed");
    when(client.sendRpcSync(any(), anyLong()))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              throw failure;
            });

    try {
      Assert.assertSame(
          failure,
          Assert.assertThrows(
              IOException.class, () -> shuffleClient.regionFinish(2, 3, 4, primaryLocation)));
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    verify(client, times(1)).sendRpcSync(any(), anyLong());
  }
}
