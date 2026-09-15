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

package org.apache.celeborn.service.deploy.worker.memory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.util.FrameDecoder;
import org.apache.celeborn.common.network.util.TransportFrameDecoder;
import org.apache.celeborn.common.protocol.TransportModuleConstants;

public class ChannelsLimiterSuiteJ {

  private ChannelsLimiter limiter;

  @Before
  public void setUp() {
    MemoryManager.reset();
    MemoryManager.initialize(new CelebornConf());
    limiter = new ChannelsLimiter(TransportModuleConstants.PUSH_MODULE, new CelebornConf());
  }

  @After
  public void tearDown() {
    MemoryManager.reset();
  }

  /**
   * Creates a channel whose autoRead state is backed by a real boolean, and adds it to the limiter.
   */
  private FakeChannel addPausedChannel(boolean hasIncompleteFrame) throws Exception {
    return addPausedChannel(hasIncompleteFrame, false);
  }

  private FakeChannel addPausedChannel(
      boolean hasIncompleteFrame, boolean hasLikelyLargeIncompleteFrame) throws Exception {
    return addPausedChannel(
        hasIncompleteFrame, hasLikelyLargeIncompleteFrame, hasLikelyLargeIncompleteFrame);
  }

  /**
   * Creates a channel whose autoRead state is backed by a real boolean, and adds it to the limiter.
   */
  private FakeChannel addPausedChannel(
      boolean hasIncompleteFrame,
      boolean hasLikelyLargeIncompleteTotalSize,
      boolean hasLikelyLargeIncompleteFrameSize)
      throws Exception {
    FakeChannel channel =
        new FakeChannel(
            hasIncompleteFrame,
            hasLikelyLargeIncompleteTotalSize,
            hasLikelyLargeIncompleteFrameSize);
    limiter.handlerAdded(channel.ctx);
    // pause it explicitly regardless of the global state, simulating a channel that was
    // paused while backpressure was active.
    channel.setAutoRead(false);
    return channel;
  }

  @Test
  public void drainIncompleteFrameReturnsZeroWhenNoChannelsArePaused() {
    assertEquals(0, limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE));
  }

  @Test
  public void drainIncompleteFrameReturnsZeroWhenGlobalStateIsNotPaused() throws Exception {
    // Channels exist and are individually "paused" (autoRead=false), but the limiter's global
    // isPaused flag was never set (i.e. backpressure was never triggered globally).
    addPausedChannel(true);
    addPausedChannel(false);

    assertEquals(0, limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE));
  }

  @Test
  public void drainIncompleteFrameReturnsZeroWhenModuleNameDoesNotMatch() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);
    addPausedChannel(true, true);

    assertEquals(0, limiter.drainIncompleteFrame(1.0, TransportModuleConstants.REPLICATE_MODULE));
  }

  @Test
  public void drainIncompleteFrameOnlyResumesChannelsWithLikelyLargeIncompleteFrame()
      throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);

    FakeChannel withLargeHalfFrame = addPausedChannel(true, true);
    FakeChannel withSmallHalfFrame = addPausedChannel(true, false);
    FakeChannel withoutHalfFrame = addPausedChannel(false, false);

    int resumed = limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);

    assertEquals(1, resumed);
    assertTrue(withLargeHalfFrame.isAutoRead());
    assertFalse(withSmallHalfFrame.isAutoRead());
    assertFalse(withoutHalfFrame.isAutoRead());
    assertTrue(withLargeHalfFrame.decoder.frameDrainEnabled());
  }

  @Test
  public void drainIncompleteFrameReturnsZeroWhenNoLargeIncompleteFrameFound() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);

    // Neither a small half-frame nor no half-frame at all qualifies: draining must not fall back
    // to resuming arbitrary paused channels.
    FakeChannel withSmallHalfFrame = addPausedChannel(true, false);
    FakeChannel withoutHalfFrame = addPausedChannel(false, false);

    int resumed = limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);

    assertEquals(0, resumed);
    assertFalse(withSmallHalfFrame.isAutoRead());
    assertFalse(withoutHalfFrame.isAutoRead());
  }

  @Test
  public void drainIncompleteFrameWorksThroughLargeIncompleteFrameTierInScanOrderWithoutShuffling()
      throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);

    // All channels have a likely-large incomplete frame; with ratio < 1.0 only a prefix should
    // be resumed each call, and repeated calls should eventually work through every one of them
    // since already-resumed channels stop reporting a half-frame (mirroring real behavior where
    // completing a probed frame clears totalSize) and thus drop out of the tier.
    List<FakeChannel> channels = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      channels.add(addPausedChannel(true, true));
    }

    int firstBatch = limiter.drainIncompleteFrame(0.3, TransportModuleConstants.PUSH_MODULE);
    assertEquals(3, firstBatch);
    long resumedAfterFirst = channels.stream().filter(FakeChannel::isAutoRead).count();
    assertEquals(3, resumedAfterFirst);

    // Simulate the resumed channels completing their probed frame: they no longer have an
    // incomplete frame, so they leave the large-incomplete-frame tier and re-pause.
    for (FakeChannel ch : channels) {
      if (ch.isAutoRead()) {
        ch.decoder.clearIncompleteFrame();
        ch.setAutoRead(false);
      }
    }

    // Second tick: 0.3 * 7 = 2.1 → max(1,2) = 2 resumed from the remaining 7.
    int secondBatch = limiter.drainIncompleteFrame(0.3, TransportModuleConstants.PUSH_MODULE);
    assertEquals(2, secondBatch);
    long resumedAfterSecond = channels.stream().filter(FakeChannel::isAutoRead).count();
    assertEquals(2, resumedAfterSecond);
    // The remaining 5 channels (7 - 2 resumed this tick) still report a large incomplete frame.
    long stillInLargeTier =
        channels.stream()
            .filter(ch -> !ch.isAutoRead() && ch.decoder.hasLikelyLargeIncompleteFrame(false))
            .count();
    assertEquals(5, stillInLargeTier);
  }

  @Test
  public void drainIncompleteFrameRespectsRatio() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);

    List<FakeChannel> channels = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      channels.add(addPausedChannel(true, true));
    }

    int resumed = limiter.drainIncompleteFrame(0.3, TransportModuleConstants.PUSH_MODULE);

    assertEquals(3, resumed);
    long actuallyResumed = channels.stream().filter(FakeChannel::isAutoRead).count();
    assertEquals(3, actuallyResumed);
  }

  @Test
  public void drainIncompleteFrameSkipsInactiveOrAlreadyReadingChannels() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);

    FakeChannel inactive = addPausedChannel(true, true);
    inactive.active = false;
    FakeChannel alreadyReading = addPausedChannel(true, true);
    alreadyReading.setAutoRead(true);
    FakeChannel eligible = addPausedChannel(true, true);

    int resumed = limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);

    assertEquals(1, resumed);
    assertTrue(eligible.isAutoRead());
    assertFalse(inactive.isAutoRead());
  }

  @Test
  public void onFrameDrainCompletedReClosesChannelWhenStillGloballyPaused() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);
    FakeChannel channel = addPausedChannel(true, true);
    limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);
    assertTrue(channel.isAutoRead());

    limiter.userEventTriggered(channel.ctx, TransportFrameDecoder.FrameDrainCompleted.INSTANCE);

    assertFalse(channel.isAutoRead());
  }

  @Test
  public void onFrameDrainCompletedLeavesChannelOpenWhenGlobalResumeHappenedMeanwhile()
      throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);
    FakeChannel channel = addPausedChannel(true, true);
    limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);
    assertTrue(channel.isAutoRead());

    // Backpressure is lifted globally while the frame-drain read was in flight.
    limiter.onResume(TransportModuleConstants.PUSH_MODULE);
    assertTrue(channel.isAutoRead());

    limiter.userEventTriggered(channel.ctx, TransportFrameDecoder.FrameDrainCompleted.INSTANCE);

    // Should remain open since backpressure is no longer active.
    assertTrue(channel.isAutoRead());
  }

  @Test
  public void userEventTriggeredIgnoresUnrelatedEvents() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);
    FakeChannel channel = addPausedChannel(true, true);

    // An unrelated event should not change the autoRead state.
    limiter.userEventTriggered(channel.ctx, new Object());

    assertFalse(channel.isAutoRead());
  }

  @Test
  public void pushModuleRanksCandidatesByTotalSizeNotFrameSize() throws Exception {
    limiter.onPause(TransportModuleConstants.PUSH_MODULE);
    FakeChannel largeTotalSizeOnly = addPausedChannel(true, true, false);

    int resumed = limiter.drainIncompleteFrame(1.0, TransportModuleConstants.PUSH_MODULE);

    assertEquals(1, resumed);
    assertTrue(largeTotalSizeOnly.isAutoRead());
  }

  @Test
  public void replicateModuleRanksCandidatesByFrameSizeNotTotalSize() throws Exception {
    ChannelsLimiter replicateLimiter =
        new ChannelsLimiter(TransportModuleConstants.REPLICATE_MODULE, new CelebornConf());
    replicateLimiter.onPause(TransportModuleConstants.REPLICATE_MODULE);

    FakeChannel largeFrameSizeOnly = new FakeChannel(true, false, true);
    replicateLimiter.handlerAdded(largeFrameSizeOnly.ctx);
    largeFrameSizeOnly.setAutoRead(false);

    FakeChannel largeTotalSizeOnly = new FakeChannel(true, true, false);
    replicateLimiter.handlerAdded(largeTotalSizeOnly.ctx);
    largeTotalSizeOnly.setAutoRead(false);

    int resumed =
        replicateLimiter.drainIncompleteFrame(1.0, TransportModuleConstants.REPLICATE_MODULE);

    assertEquals(1, resumed);
    assertTrue(largeFrameSizeOnly.isAutoRead());
    assertFalse(largeTotalSizeOnly.isAutoRead());
  }

  /**
   * A minimal fake {@link Channel} wired up with a real {@link TransportFrameDecoder} in its
   * pipeline, and an autoRead flag backed by a plain boolean so that ChannelsLimiter's read/write
   * of autoRead behaves like a real Netty channel would.
   */
  private static class FakeChannel {
    final Channel channel = mock(Channel.class);
    final ChannelConfig config = mock(ChannelConfig.class);
    final ChannelPipeline pipeline = mock(ChannelPipeline.class);
    final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    final AtomicBoolean autoRead = new AtomicBoolean(true);
    final TestableFrameDecoder decoder;
    boolean active = true;

    FakeChannel(
        boolean hasIncompleteFrame,
        boolean hasLikelyLargeIncompleteTotalSize,
        boolean hasLikelyLargeIncompleteFrameSize) {
      decoder =
          new TestableFrameDecoder(
              hasIncompleteFrame,
              hasLikelyLargeIncompleteTotalSize,
              hasLikelyLargeIncompleteFrameSize);
      when(channel.config()).thenReturn(config);
      when(channel.pipeline()).thenReturn(pipeline);
      when(channel.isActive()).thenAnswer(inv -> active);
      when(config.isAutoRead()).thenAnswer(inv -> autoRead.get());
      when(config.setAutoRead(org.mockito.ArgumentMatchers.anyBoolean()))
          .thenAnswer(
              inv -> {
                autoRead.set(inv.getArgument(0));
                return config;
              });
      when(pipeline.get(FrameDecoder.HANDLER_NAME)).thenReturn(decoder);
      when(ctx.channel()).thenReturn(channel);
    }

    void setAutoRead(boolean value) {
      autoRead.set(value);
    }

    boolean isAutoRead() {
      return autoRead.get();
    }
  }

  /** Exposes frameDrain state for assertions without changing TransportFrameDecoder's API. */
  private static class TestableFrameDecoder extends TransportFrameDecoder {
    private boolean incomplete;
    private boolean likelyLargeIncompleteTotalSize;
    private boolean likelyLargeIncompleteFrameSize;

    TestableFrameDecoder(
        boolean incomplete,
        boolean likelyLargeIncompleteTotalSize,
        boolean likelyLargeIncompleteFrameSize) {
      this.incomplete = incomplete;
      this.likelyLargeIncompleteTotalSize = likelyLargeIncompleteTotalSize;
      this.likelyLargeIncompleteFrameSize = likelyLargeIncompleteFrameSize;
    }

    @Override
    public boolean hasLikelyLargeIncompleteFrame(boolean byFrameSize) {
      return byFrameSize ? likelyLargeIncompleteFrameSize : likelyLargeIncompleteTotalSize;
    }

    /** Simulates the channel's stuck half-frame being fully consumed after being probed. */
    void clearIncompleteFrame() {
      incomplete = false;
      likelyLargeIncompleteTotalSize = false;
      likelyLargeIncompleteFrameSize = false;
    }

    boolean frameDrainEnabled() {
      // enableFrameDrain() has no visible side effect other than internal state; simply
      // verifying it doesn't throw and that drainIncompleteFrame called it is enough here since
      // the TransportFrameDecoder-level drain behavior itself is covered by
      // TransportFrameDecoderSuiteJ. We track a local flag by overriding enableFrameDrain.
      return enabledCalled;
    }

    private boolean enabledCalled = false;

    @Override
    public void enableFrameDrain() {
      enabledCalled = true;
      super.enableFrameDrain();
    }
  }
}
