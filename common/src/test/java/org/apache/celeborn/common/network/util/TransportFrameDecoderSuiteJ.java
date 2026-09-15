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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.OneWayMessage;

public class TransportFrameDecoderSuiteJ {

  private TransportFrameDecoder decoder;
  private ChannelHandlerContext ctx;
  private Channel channel;
  private ChannelPipeline pipeline;
  private final List<Object> decodedMessages = new ArrayList<>();

  @Before
  public void setUp() {
    decoder = new TransportFrameDecoder();
    ctx = mock(ChannelHandlerContext.class);
    channel = mock(Channel.class);
    pipeline = mock(ChannelPipeline.class);
    when(ctx.channel()).thenReturn(channel);
    when(channel.pipeline()).thenReturn(pipeline);
    decodedMessages.clear();
    when(ctx.fireChannelRead(any()))
        .thenAnswer(
            invocation -> {
              decodedMessages.add(invocation.getArgument(0));
              return ctx;
            });
  }

  private ByteBuf encodeMessage(Message message) throws IOException {
    ByteBuf buf = Unpooled.buffer();
    buf.writeInt(message.encodedLength());
    message.type().encode(buf);
    if (message.body() != null) {
      buf.writeInt((int) message.body().size());
    } else {
      buf.writeInt(0);
    }
    message.encode(buf);
    if (message.body() != null) {
      buf.writeBytes(message.body().nioByteBuffer());
    }
    return buf;
  }

  private OneWayMessage oneWayMessage(byte[] payload) {
    return new OneWayMessage(new NioManagedBuffer(java.nio.ByteBuffer.wrap(payload)));
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseInitially() {
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(true));
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseForASmallStuckHalfFrame() throws IOException {
    // A small amount of leftover bytes is well within what a single channelRead can deliver, so
    // it isn't evidence of a large frame stuck across multiple reads.
    ByteBuf full = encodeMessage(oneWayMessage(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    ByteBuf partial = full.retainedSlice(0, FrameDecoder.HEADER_SIZE + 2);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(true));
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsTrueOnceLeftoverExceedsSingleReadCap()
      throws IOException {
    // A single channelRead cannot deliver more than 64KB, so leftover bytes beyond that can only
    // have piled up over multiple reads — i.e. a genuinely large frame stuck mid-transfer. Here
    // the whole leftover belongs to a single frame, so both checks agree.
    int oversizedBodyLength = 65536 + 1024;
    byte[] payload = new byte[oversizedBodyLength];
    ByteBuf full = encodeMessage(oneWayMessage(payload));
    // Withhold the last byte so the frame never actually completes.
    ByteBuf partial = full.retainedSlice(0, full.readableBytes() - 1);
    decoder.channelRead(ctx, partial);

    assertTrue(decoder.hasLikelyLargeIncompleteFrame(false));
    assertTrue(decoder.hasLikelyLargeIncompleteFrame(true));
    assertTrue(decodedMessages.isEmpty());
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameIsFalseWhenLeftoverExactlyEqualsSingleReadCap()
      throws IOException {
    // Strict ">", not ">=": both totalSize and the frame's total length sit exactly at the cap,
    // so neither check should trip yet.
    int singleReadCapBytes = 65536;
    byte[] payload = new byte[singleReadCapBytes - FrameDecoder.HEADER_SIZE - 4];
    ByteBuf full = encodeMessage(oneWayMessage(payload));
    assertEquals(singleReadCapBytes, full.readableBytes());
    ByteBuf partial = full.retainedSlice(0, full.readableBytes() - 1);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(true));
    assertTrue(decodedMessages.isEmpty());
    full.release();
  }

  @Test
  public void
      hasLikelyLargeIncompleteFrameSizeIsTrueUsingDecodedFrameLengthEvenWithinSingleReadCap()
          throws IOException {
    // A 256KB frame paused right after its first 64KB read: totalSize (leftover body bytes only)
    // stays under the cap, but the decoded frame length correctly flags it as large.
    int largeFrameBodyLength = 256 * 1024 - FrameDecoder.HEADER_SIZE - 4;
    byte[] payload = new byte[largeFrameBodyLength];
    ByteBuf full = encodeMessage(oneWayMessage(payload));
    assertEquals(256 * 1024, full.readableBytes());

    ByteBuf partial = full.retainedSlice(0, 65536);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertTrue(decoder.hasLikelyLargeIncompleteFrame(true));
    assertTrue(decodedMessages.isEmpty());
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameSizeFallsBackToTotalSizeWhenHeaderNotYetFullyRead()
      throws IOException {
    // Header not fully read yet, so nextFrameSize is unknown and the check falls back to totalSize.
    ByteBuf full = encodeMessage(oneWayMessage(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    ByteBuf partial = full.retainedSlice(0, FrameDecoder.HEADER_SIZE - 2);
    decoder.channelRead(ctx, partial);

    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(true));
    full.release();
  }

  @Test
  public void hasLikelyLargeIncompleteFrameResetsAfterFrameCompletesAndNextHeaderNotYetRead()
      throws IOException {
    // After a large frame is fully decoded, nextFrameSize resets; the predicate must not keep
    // reporting true based on stale state.
    int largeFrameBodyLength = 256 * 1024 - FrameDecoder.HEADER_SIZE;
    ByteBuf full = encodeMessage(oneWayMessage(new byte[largeFrameBodyLength]));
    decoder.channelRead(ctx, full);

    assertEquals(1, decodedMessages.size());
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(false));
    assertFalse(decoder.hasLikelyLargeIncompleteFrame(true));
  }

  @Test
  public void frameDrainFiresEventOnceButStillDispatchesFramesAlreadyBuffered() throws IOException {
    // Two complete frames arrive in a single channelRead call, simulating data that piles up
    // while frame-drain resumes a channel that has more than just the stuck half-frame available.
    // Since both frames are already fully in memory (no extra I/O needed to obtain them), both
    // should still be decoded and dispatched to the downstream handler right away; only the
    // drain-completed notification (which governs whether autoRead stays on) fires once.
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    ByteBuf combined = Unpooled.wrappedBuffer(frame1, frame2);

    decoder.enableFrameDrain();
    decoder.channelRead(ctx, combined);

    // Both frames already sitting in memory should have been decoded and dispatched...
    assertEquals(2, decodedMessages.size());
    // ...while the drain-completed event fires exactly once, right after the first frame.
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);
  }

  @Test
  public void nonFrameDrainModeConsumesAllAvailableFramesWithoutFiringEvent() throws IOException {
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    ByteBuf combined = Unpooled.wrappedBuffer(frame1, frame2);

    decoder.channelRead(ctx, combined);

    assertEquals(2, decodedMessages.size());
    verify(pipeline, times(0)).fireUserEventTriggered(any());
  }

  @Test
  public void frameDrainFlagIsResetAfterFirstFrameCompletes() throws IOException {
    ByteBuf frame1 = encodeMessage(oneWayMessage(new byte[] {1, 2, 3}));
    decoder.enableFrameDrain();
    decoder.channelRead(ctx, frame1);
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);

    // A subsequent frame, arriving after frame-drain mode has already been consumed, should not
    // fire the event again since frameDrain was reset to false.
    ByteBuf frame2 = encodeMessage(oneWayMessage(new byte[] {4, 5, 6}));
    decoder.channelRead(ctx, frame2);
    verify(pipeline, times(1))
        .fireUserEventTriggered(TransportFrameDecoder.FrameDrainCompleted.INSTANCE);
    assertEquals(2, decodedMessages.size());
  }
}
