/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.common.network.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.apache.celeborn.common.network.protocol.Message;

public class TransportFrameDecoderSuiteJ {

  @Test
  public void testReleasesSingleBufferFrameWhenRpcDecodeFails() {
    ByteBuf input = rpcFrame(8, 8, 0);
    EmbeddedChannel channel = new EmbeddedChannel(new TransportFrameDecoder());

    try {
      assertThrows(IndexOutOfBoundsException.class, () -> channel.writeInbound(input));
      channel.close().syncUninterruptibly();
      assertEquals(0, input.refCnt());
    } finally {
      channel.finishAndReleaseAll();
      releaseFully(input);
    }
  }

  @Test
  public void testReleasesRetainedSliceWhenRpcDecodeFails() {
    ByteBuf input = rpcFrame(8, 8, 1);
    EmbeddedChannel channel = new EmbeddedChannel(new TransportFrameDecoder());

    try {
      assertThrows(IndexOutOfBoundsException.class, () -> channel.writeInbound(input));
      channel.close().syncUninterruptibly();
      assertEquals(0, input.refCnt());
    } finally {
      channel.finishAndReleaseAll();
      releaseFully(input);
    }
  }

  @Test
  public void testReleasesCompositeFrameWhenRpcDecodeFails() {
    ByteBuf first = rpcFrame(8, 4, 0);
    ByteBuf second = directBuffer(4);
    second.writeZero(4);
    EmbeddedChannel channel = new EmbeddedChannel(new TransportFrameDecoder());

    try {
      channel.writeInbound(first);
      assertThrows(IndexOutOfBoundsException.class, () -> channel.writeInbound(second));
      channel.close().syncUninterruptibly();
      assertEquals(0, first.refCnt());
      assertEquals(0, second.refCnt());
    } finally {
      channel.finishAndReleaseAll();
      releaseFully(first);
      releaseFully(second);
    }
  }

  @Test
  public void testReleasesValidRpcFrameInDownstreamHandler() {
    ByteBuf input = rpcFrame(12, 12, 0);
    EmbeddedChannel channel =
        new EmbeddedChannel(new TransportFrameDecoder(), new ReleasingMessageHandler());

    try {
      channel.writeInbound(input);
      assertEquals(0, input.refCnt());
    } finally {
      channel.finishAndReleaseAll();
      releaseFully(input);
    }
  }

  @Test
  public void testReleasesIncompleteFrameOnClose() {
    ByteBuf input = rpcFrame(12, 4, 0);
    EmbeddedChannel channel = new EmbeddedChannel(new TransportFrameDecoder());

    try {
      channel.writeInbound(input);
      channel.close().syncUninterruptibly();
      assertEquals(0, input.refCnt());
    } finally {
      channel.finishAndReleaseAll();
      releaseFully(input);
    }
  }

  private static ByteBuf rpcFrame(int messageSize, int payloadBytes, int extraBytes) {
    ByteBuf buffer = directBuffer(FrameDecoder.HEADER_SIZE + payloadBytes + extraBytes);
    buffer.writeInt(messageSize);
    buffer.writeByte(Message.Type.RPC_REQUEST.id());
    buffer.writeInt(0);
    buffer.writeZero(payloadBytes + extraBytes);
    return buffer;
  }

  private static ByteBuf directBuffer(int capacity) {
    return UnpooledByteBufAllocator.DEFAULT.directBuffer(capacity);
  }

  private static void releaseFully(ByteBuf buffer) {
    while (buffer.refCnt() > 0) {
      buffer.release();
    }
  }

  private static class ReleasingMessageHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) {
      Message message = (Message) data;
      if (message.body() != null) {
        message.body().release();
      }
    }
  }
}
