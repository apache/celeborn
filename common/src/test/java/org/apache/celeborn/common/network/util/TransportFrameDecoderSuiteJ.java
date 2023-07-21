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

package org.apache.celeborn.common.network.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.AfterClass;
import org.junit.Test;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.OneWayMessage;

public class TransportFrameDecoderSuiteJ {

  private static Random RND = new Random();

  @AfterClass
  public static void cleanup() {
    RND = null;
  }

  @Test
  public void testFrameDecoding() throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mockChannelHandlerContext();
    ByteBuf data = createAndFeedFrames(100, decoder, ctx);
    verifyAndCloseDecoder(decoder, ctx, data);
  }

  @Test
  public void testSplitLengthField() throws Exception {
    byte[] body = new byte[1024 * (RND.nextInt(31) + 1)];

    OneWayMessage message =
        new OneWayMessage(new NioManagedBuffer(Unpooled.wrappedBuffer(body).nioBuffer()));
    // frame size | message type | message encode | message body
    int frameLength = 4 + message.type().encodedLength() + message.encodedLength() + body.length;
    ByteBuf buf = Unpooled.buffer(frameLength);
    buf.writeInt(frameLength);
    message.type().encode(buf);
    message.encode(buf);
    buf.writeBytes(message.body().nioByteBuffer().array());

    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mockChannelHandlerContext();
    try {
      decoder.channelRead(ctx, buf.readSlice(RND.nextInt(3)).retain());
      verify(ctx, never()).fireChannelRead(any(ByteBuf.class));
      decoder.channelRead(ctx, buf);
      verify(ctx).fireChannelRead(any(Message.class));
      assertEquals(0, buf.refCnt());
    } finally {
      decoder.channelInactive(ctx);
      release(buf);
    }
  }

  @Test
  public void testNegativeFrameSize() {
    assertThrows(IllegalArgumentException.class, () -> testInvalidFrame(-1));
  }

  @Test
  public void testEmptyFrame() {
    // 4 because frame size includes the frame length.
    assertThrows(IllegalArgumentException.class, () -> testInvalidFrame(4));
  }

  /**
   * Creates a number of randomly sized frames and feed them to the given decoder, verifying that
   * the frames were read.
   */
  private ByteBuf createAndFeedFrames(
      int frameCount, TransportFrameDecoder decoder, ChannelHandlerContext ctx) throws Exception {
    ByteBuf data = Unpooled.buffer();
    for (int i = 0; i < frameCount; i++) {
      byte[] body = new byte[1024 * (RND.nextInt(31) + 1)];
      OneWayMessage message =
          new OneWayMessage(new NioManagedBuffer(Unpooled.wrappedBuffer(body).nioBuffer()));
      // frame size | message type | message encode | message body
      int frameLength = 4 + message.type().encodedLength() + message.encodedLength() + body.length;
      data.writeInt(frameLength);
      message.type().encode(data);
      message.encode(data);
      data.writeBytes(message.body().nioByteBuffer().array());
    }

    try {
      while (data.isReadable()) {
        int size = RND.nextInt(4 * 1024) + 256;
        decoder.channelRead(ctx, data.readSlice(Math.min(data.readableBytes(), size)).retain());
      }

      verify(ctx, times(frameCount)).fireChannelRead(any(Message.class));
    } catch (Exception e) {
      release(data);
      throw e;
    }
    return data;
  }

  private void verifyAndCloseDecoder(
      TransportFrameDecoder decoder, ChannelHandlerContext ctx, ByteBuf data) throws Exception {
    try {
      decoder.channelInactive(ctx);
      assertTrue("There shouldn't be dangling references to the data.", data.release());
    } finally {
      release(data);
    }
  }

  private void testInvalidFrame(long size) throws Exception {
    TransportFrameDecoder decoder = new TransportFrameDecoder();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ByteBuf frame = Unpooled.copyLong(size);
    try {
      decoder.channelRead(ctx, frame);
    } finally {
      release(frame);
    }
  }

  private ChannelHandlerContext mockChannelHandlerContext() {
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.fireChannelRead(any()))
        .thenAnswer(
            in -> {
              Message message = (Message) in.getArguments()[0];
              message.body().release();
              return null;
            });
    return ctx;
  }

  private void release(ByteBuf buf) {
    if (buf.refCnt() > 0) {
      buf.release(buf.refCnt());
    }
  }
}
