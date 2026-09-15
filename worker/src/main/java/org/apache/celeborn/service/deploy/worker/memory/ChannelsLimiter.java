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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.util.FrameDecoder;
import org.apache.celeborn.common.network.util.TransportFrameDecoder;
import org.apache.celeborn.common.protocol.TransportModuleConstants;

@ChannelHandler.Sharable
public class ChannelsLimiter extends ChannelDuplexHandler
    implements MemoryManager.MemoryPressureListener {

  private static final Logger logger = LoggerFactory.getLogger(ChannelsLimiter.class);
  // Throttles the "drainIncompleteFrame resumed" INFO log to at most once per interval, since
  // sustained backpressure can otherwise emit it on every tick (default every 5ms).
  private static final long DRAIN_RESUME_LOG_INTERVAL_MS = 10000L;
  private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
  private final String moduleName;
  private final AtomicBoolean isPaused = new AtomicBoolean(false);
  private final AtomicInteger needTrimChannels = new AtomicInteger(0);
  private volatile long lastDrainResumeLogTime = -1L;
  private final long waitTrimInterval;
  private final boolean allowCache;

  public ChannelsLimiter(String moduleName, CelebornConf conf) {
    this.moduleName = moduleName;
    this.waitTrimInterval = conf.workerDirectMemoryTrimChannelWaitInterval();
    this.allowCache = conf.networkMemoryAllocatorAllowCache();
    MemoryManager memoryManager = MemoryManager.instance();
    memoryManager.registerMemoryListener(this);
  }

  private void pauseAllChannels() {
    isPaused.set(true);
    channels.forEach(
        c -> {
          if (c.config().isAutoRead()) {
            c.config().setAutoRead(false);
          }
        });
  }

  private void trimCache() {
    needTrimChannels.set(0);
    channels.forEach(
        c -> {
          needTrimChannels.incrementAndGet();
          c.pipeline().fireUserEventTriggered(new TrimCache());
        });
    long delta = 100L;
    int retryTime = 0;
    while (needTrimChannels.get() > 0 && retryTime * delta < waitTrimInterval) {
      try {
        retryTime += 1;
        Thread.sleep(delta);
      } catch (InterruptedException e) {
        // Do nothing
      }
    }
  }

  private void resumeAllChannels() {
    synchronized (isPaused) {
      isPaused.set(false);
      channels.forEach(
          c -> {
            if (!c.config().isAutoRead()) {
              c.config().setAutoRead(true);
            }
          });
    }
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    channels.add(ctx.channel());
    synchronized (isPaused) {
      if (isPaused.get()) {
        // If thread A runs here,and its time slice is run out while
        // another thread B running "resumeAllChannels" method.
        // It is possible that the channel connected with ctx will
        // pause auto read because of concurrent modification.
        // So we need to make sure checking the paused flag and
        // changing its status will be an atomic operation.
        ctx.channel().config().setAutoRead(false);
      }
    }
    super.handlerAdded(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (!ctx.channel().config().isAutoRead()) {
      ctx.channel().config().setAutoRead(true);
    }
    channels.remove(ctx.channel());
    super.handlerRemoved(ctx);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof TrimCache) {
      if (ctx.alloc() instanceof PooledByteBufAllocator) {
        ((PooledByteBufAllocator) ctx.alloc()).trimCurrentThreadCache();
      }
      needTrimChannels.decrementAndGet();
    } else if (evt instanceof TransportFrameDecoder.FrameDrainCompleted) {
      onFrameDrainCompleted(ctx.channel());
    }
  }

  /**
   * Re-pauses the channel after its single drain frame completes, unless a full resume happened
   * concurrently.
   */
  private void onFrameDrainCompleted(Channel channel) {
    synchronized (isPaused) {
      if (isPaused.get() && channel.config().isAutoRead()) {
        channel.config().setAutoRead(false);
      }
    }
  }

  @Override
  public void onPause(String moduleName) {
    if (this.moduleName.equals(moduleName)) {
      logger.info("{} channels pause read.", this.moduleName);
      pauseAllChannels();
    }
  }

  @Override
  public void onResume(String moduleName) {
    if (moduleName.equalsIgnoreCase("all")) {
      logger.info("{} channels resume read.", this.moduleName);
      resumeAllChannels();
    }
    if (this.moduleName.equals(moduleName)) {
      logger.info("{} channels resume read.", this.moduleName);
      resumeAllChannels();
    }
  }

  @Override
  public void onTrim() {
    if (allowCache) {
      trimCache();
    }
  }

  /**
   * Resumes {@code ratio} fraction of paused channels that have a likely-large stuck half-frame,
   * each in frame-drain mode. No-op if {@code moduleName} does not match this limiter's module.
   */
  @Override
  public int drainIncompleteFrame(double ratio, String moduleName) {
    if (!this.moduleName.equals(moduleName)) {
      return 0;
    }
    List<Channel> candidates = new ArrayList<>();
    for (Channel ch : channels) {
      if (!ch.isActive() || ch.config().isAutoRead()) {
        continue;
      }
      TransportFrameDecoder decoder = frameDecoderOf(ch);
      if (decoder != null
          && decoder.hasLikelyLargeIncompleteFrame(
              TransportModuleConstants.REPLICATE_MODULE.equals(moduleName))) {
        candidates.add(ch);
      }
    }

    if (candidates.isEmpty()) {
      // DEBUG, not INFO: under sustained backpressure with no qualifying candidate, this tick
      // repeats every drainIncompleteFrame interval (default 5ms) and would otherwise flood logs
      // (~100/s per paused limiter) right when the incident is already stressing log storage.
      if (isPaused.get() && logger.isDebugEnabled()) {
        logger.debug(
            "{} drainIncompleteFrame skipped this tick: no paused channel found with a stuck "
                + "half-frame larger than {} bytes.",
            moduleName,
            TransportFrameDecoder.MAX_SINGLE_READ_BYTES);
      }
      return 0;
    }

    int targetCount = Math.max(1, (int) (candidates.size() * ratio));
    int actualResume = Math.min(targetCount, candidates.size());

    int resumed = 0;
    synchronized (isPaused) {
      if (!isPaused.get()) {
        return 0;
      }
      for (int i = 0; i < actualResume; i++) {
        Channel ch = candidates.get(i);
        // Re-check: state may have changed since the scan above.
        if (!ch.isActive() || ch.config().isAutoRead()) {
          continue;
        }
        TransportFrameDecoder decoder = frameDecoderOf(ch);
        if (decoder != null) {
          decoder.enableFrameDrain();
        }
        ch.config().setAutoRead(true);
        resumed++;
      }
    }
    if (resumed > 0) {
      long now = System.currentTimeMillis();
      if (lastDrainResumeLogTime < 0
          || now - lastDrainResumeLogTime >= DRAIN_RESUME_LOG_INTERVAL_MS) {
        lastDrainResumeLogTime = now;
        logger.info(
            "{} drainIncompleteFrame resumed {}/{} channels with a stuck half-frame larger than "
                + "{} bytes (ratio={})",
            moduleName,
            resumed,
            candidates.size(),
            TransportFrameDecoder.MAX_SINGLE_READ_BYTES,
            ratio);
      }
    }
    return resumed;
  }

  private static TransportFrameDecoder frameDecoderOf(Channel channel) {
    Object decoder = channel.pipeline().get(FrameDecoder.HANDLER_NAME);
    return decoder instanceof TransportFrameDecoder ? (TransportFrameDecoder) decoder : null;
  }

  static class TrimCache {}
}
