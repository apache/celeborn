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

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.exception.RssException;
import com.aliyun.emr.rss.common.metrics.source.AbstractSource;
import com.aliyun.emr.rss.common.metrics.source.NetWorkSource;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.client.RpcResponseCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.protocol.*;
import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.FileManagedBuffers;
import com.aliyun.emr.rss.common.network.server.OneForOneStreamManager;
import com.aliyun.emr.rss.common.network.server.RpcHandler;
import com.aliyun.emr.rss.common.network.server.StreamManager;
import com.aliyun.emr.rss.common.network.util.NettyUtils;
import com.aliyun.emr.rss.common.network.util.TransportConf;

public final class ChunkFetchHandler extends RpcHandler {

  private static final Logger logger = LoggerFactory.getLogger(ChunkFetchHandler.class);

  private final TransportConf conf;
  private final OpenStreamHandler handler;
  private final OneForOneStreamManager streamManager;
  private final AbstractSource source; // metrics

  public ChunkFetchHandler(TransportConf conf, AbstractSource source, OpenStreamHandler handler
  ) {
    this.conf = conf;
    this.handler = handler;
    this.streamManager = new OneForOneStreamManager();
    this.source = source;
  }

  private String readString(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public void receiveRpc(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    AbstractMessage msg = AbstractMessage.fromByteBuffer(message);
    assert msg instanceof OpenBlocks;
    OpenBlocks openBlocks = (OpenBlocks) msg;
    String shuffleKey = new String(openBlocks.shuffleKey, StandardCharsets.UTF_8);
    String fileName = new String(openBlocks.fileName, StandardCharsets.UTF_8);
    int startMapIndex = openBlocks.startMapIndex;
    int endMapIndex = openBlocks.endMapIndex;

    // metrics start
    source.startTimer(WorkerSource.OpenStreamTime(), shuffleKey);
    FileInfo fileInfo = handler.handleOpenStream(shuffleKey, fileName, startMapIndex, endMapIndex);

    if (fileInfo != null) {
      logger.debug("Received chunk fetch request {} {} {} {} get file info {}", shuffleKey,
        fileName, startMapIndex, endMapIndex, fileInfo);
      try {
        FileManagedBuffers buffers = new FileManagedBuffers(fileInfo, conf);
        long streamId = streamManager.registerStream(
            client.getClientId(), buffers, client.getChannel());

        StreamHandle streamHandle = new StreamHandle(streamId, fileInfo.numChunks);
        if (fileInfo.numChunks == 0) {
          logger.debug("StreamId {} fileName {} startMapIndex {} endMapIndex {} is empty.",
            streamId, fileName, startMapIndex, endMapIndex);
        }
        callback.onSuccess(streamHandle.toByteBuffer());
      } catch (IOException e) {
        callback.onFailure(
            new RssException("Chunk offsets meta exception ", e));
      } finally {
        // metrics end
        source.stopTimer(WorkerSource.OpenStreamTime(), shuffleKey);
      }
    } else {
      // metrics end
      source.stopTimer(WorkerSource.OpenStreamTime(), shuffleKey);

      callback.onFailure(new FileNotFoundException());
    }
  }

  @Override
  public void receiveRequestMessage(TransportClient client, RequestMessage msg) {
    assert msg instanceof ChunkFetchRequest;
    ChunkFetchRequest req = (ChunkFetchRequest) msg;
    if (source != null) {
      source.startTimer(NetWorkSource.FetchChunkTime(), req.toString());
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}",
        NettyUtils.getRemoteAddress(client.getChannel()),
        req.streamChunkSlice);
    }
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= conf.maxChunksBeingTransferred()) {
      logger.error("The number of chunks being transferred {} is above {}.",
        chunksBeingTransferred, conf.maxChunksBeingTransferred());
      if (source != null) {
        source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
      }
      return;
    }
    ManagedBuffer buf;
    try {
      streamManager.checkAuthorization(client, req.streamChunkSlice.streamId);
      buf = streamManager.getChunk(req.streamChunkSlice.streamId, req.streamChunkSlice.chunkIndex,
        req.streamChunkSlice.offset, req.streamChunkSlice.len);
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        req.streamChunkSlice, NettyUtils.getRemoteAddress(client.getChannel())), e);
      client.getChannel().writeAndFlush(new ChunkFetchFailure(req.streamChunkSlice,
        Throwables.getStackTraceAsString(e)));
      if (source != null) {
        source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
      }
      return;
    }

    streamManager.chunkBeingSent(req.streamChunkSlice.streamId);
    client.getChannel().writeAndFlush(new ChunkFetchSuccess(req.streamChunkSlice, buf))
      .addListener(future -> {
        streamManager.chunkSent(req.streamChunkSlice.streamId);
        if (source != null) {
          source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
        }
      });
  }

  @Override
  public boolean checkRegistered() {
    return ((Registerable) handler).isRegistered();
  }

  @Override
  public void channelInactive(TransportClient client) {
    logger.debug("channel Inactive " + client.getSocketAddress());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    logger.debug("exception caught " + cause + " " + client.getSocketAddress());
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }
}
