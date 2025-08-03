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

package org.apache.celeborn.common.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.OneWayMessage;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.PushMergedData;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.StreamChunkSlice;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbChunkFetchRequest;
import org.apache.celeborn.common.read.FetchRequestInfo;
import org.apache.celeborn.common.write.PushRequestInfo;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * <p>Note that while this client deals with the fetching of chunks from a stream (i.e., data
 * plane), the actual setup of the streams is done outside the scope of the transport layer. The
 * convenience method "sendRPC" is provided to enable control plane communication between the client
 * and server to perform this setup.
 *
 * <p>For example, a typical workflow might be: client.sendRPC(new OpenFile("/foo")) --&gt; returns
 * StreamId = 100 client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback) ... client.sendRPC(new
 * CloseStream(100))
 *
 * <p>Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * <p>NB: This class is used to make requests to the server, while {@link TransportResponseHandler}
 * is responsible for handling responses from the server.
 *
 * <p>Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  private volatile boolean timedOut;
  @Nullable private String clientId;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  public void fetchChunk(
      long streamId, int chunkIndex, long fetchDataTimeout, ChunkReceivedCallback callback) {
    fetchChunk(streamId, chunkIndex, 0, Integer.MAX_VALUE, fetchDataTimeout, callback);
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * <p>Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times,
   * though some streams may not support this.
   *
   * <p>Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are
   * guaranteed to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should be
   *     agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param offset offset from the beginning of the chunk to fetch
   * @param len size to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      int chunkIndex,
      int offset,
      int len,
      long fetchDataTimeout,
      ChunkReceivedCallback callback) {

    StreamChunkSlice streamChunkSlice = new StreamChunkSlice(streamId, chunkIndex, offset, len);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Sending fetch chunk request {} to {}.",
          streamChunkSlice,
          NettyUtils.getRemoteAddress(channel));
    }
    StdChannelListener listener =
        new StdChannelListener(streamChunkSlice) {
          @Override
          protected void handleFailure(String errorMsg, Throwable cause) {
            handler.handleFetchFailure(streamChunkSlice, errorMsg, cause);
          }
        };

    long dueTime = System.currentTimeMillis() + fetchDataTimeout;
    FetchRequestInfo info = new FetchRequestInfo(dueTime, callback);
    handler.addFetchRequest(streamChunkSlice, info);

    ChannelFuture channelFuture =
        channel
            .writeAndFlush(
                new RpcRequest(
                    TransportClient.requestId(),
                    new NioManagedBuffer(
                        new TransportMessage(
                                MessageType.CHUNK_FETCH_REQUEST,
                                PbChunkFetchRequest.newBuilder()
                                    .setStreamChunkSlice(streamChunkSlice.toProto())
                                    .build()
                                    .toByteArray())
                            .toByteBuffer())))
            .addListener(listener);
    info.setChannelFuture(channelFuture);
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked with
   * the server's response or upon any failure.
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", NettyUtils.getRemoteAddress(channel));
    }

    long requestId = requestId();
    handler.addRpcRequest(requestId, callback);

    RpcChannelListener listener = new RpcChannelListener(requestId);
    channel
        .writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
        .addListener(listener);

    return requestId;
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void sendRpc(ByteBuffer message) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", NettyUtils.getRemoteAddress(channel));
    }

    long requestId = requestId();
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)));
  }

  public ChannelFuture pushData(
      PushData pushData, long pushDataTimeout, RpcResponseCallback callback) {
    return pushData(pushData, pushDataTimeout, callback, null);
  }

  public ChannelFuture pushData(
      PushData pushData,
      long pushDataTimeout,
      RpcResponseCallback callback,
      Runnable rpcSendoutCallback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Pushing data to {}", NettyUtils.getRemoteAddress(channel));
    }

    long requestId = requestId();
    long dueTime = System.currentTimeMillis() + pushDataTimeout;
    PushRequestInfo info = new PushRequestInfo(dueTime, callback);
    handler.addPushRequest(requestId, info);
    pushData.requestId = requestId;
    PushChannelListener listener = new PushChannelListener(requestId, rpcSendoutCallback);
    ChannelFuture channelFuture = channel.writeAndFlush(pushData).addListener(listener);
    info.setChannelFuture(channelFuture);
    return channelFuture;
  }

  public ChannelFuture pushMergedData(
      PushMergedData pushMergedData, long pushDataTimeout, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Pushing merged data to {}", NettyUtils.getRemoteAddress(channel));
    }

    long requestId = requestId();
    long dueTime = System.currentTimeMillis() + pushDataTimeout;
    PushRequestInfo info = new PushRequestInfo(dueTime, callback);
    handler.addPushRequest(requestId, info);
    pushMergedData.requestId = requestId;

    PushChannelListener listener = new PushChannelListener(requestId);
    ChannelFuture channelFuture = channel.writeAndFlush(pushMergedData).addListener(listener);
    info.setChannelFuture(channelFuture);
    return channelFuture;
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to a
   * specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) throws IOException {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(
        message,
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            try {
              ByteBuffer copy = ByteBuffer.allocate(response.remaining());
              copy.put(response);
              // flip "copy" to make it readable
              copy.flip();
              result.set(copy);
            } catch (Throwable t) {
              logger.warn("Error in responding RPC callback", t);
              result.setException(t);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            result.setException(e);
          }
        });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IOException("Exception in sendRpcSync to: " + this.getSocketAddress(), e);
    }
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to a
   * specified timeout for a response. The callback will be invoked with the server's response or
   * upon any failure.
   */
  public void sendRpcSync(ByteBuffer message, RpcResponseCallback callback, long timeoutMs)
      throws IOException {
    final SettableFuture<Void> result = SettableFuture.create();

    sendRpc(
        message,
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            try {
              ByteBuffer copy = ByteBuffer.allocate(response.remaining());
              copy.put(response);
              // flip "copy" to make it readable
              copy.flip();
              callback.onSuccess(copy);
              result.set(null);
            } catch (Throwable t) {
              logger.warn("Error in responding RPC callback", t);
              callback.onFailure(t);
              result.set(null);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            callback.onFailure(e);
            result.set(null);
          }
        });

    try {
      result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IOException("Exception in sendRpcSync to: " + this.getSocketAddress(), e);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // Mark the connection as timed out, so we do not return a connection that's being closed
    // from the TransportClientFactory if closing takes some time (e.g. with SSL)
    this.timedOut = true;
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return "TransportClient[remoteAddress="
        + channel.remoteAddress()
        + "clientId="
        + clientId
        + ",isActive="
        + isActive()
        + "]";
  }

  private static final AtomicLong counter = new AtomicLong();

  public static long requestId() {
    return counter.getAndIncrement();
  }

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * <p>Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(
        clientId == null || clientId.equals(id), "Client ID has already been set.");
    this.clientId = id;
  }

  public class StdChannelListener implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    public StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isDebugEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.debug(
              "Sending request {} to {} took {} ms",
              requestId,
              NettyUtils.getRemoteAddress(channel),
              timeTaken);
        }
      } else {
        String errorMsg =
            String.format(
                "Failed to send request %s to %s: %s, channel will be closed",
                requestId, NettyUtils.getRemoteAddress(channel), future.cause());
        logger.warn(errorMsg);
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    protected void handleFailure(String errorMsg, Throwable cause) {
      logger.error("Error encountered " + errorMsg, cause);
    }
  }

  private class RpcChannelListener extends StdChannelListener {
    final long rpcRequestId;

    RpcChannelListener(long rpcRequestId) {
      super("RPC " + rpcRequestId);
      this.rpcRequestId = rpcRequestId;
    }

    @Override
    protected void handleFailure(String errorMsg, Throwable cause) {
      handler.handleRpcFailure(rpcRequestId, errorMsg, cause);
    }
  }

  private class PushChannelListener extends StdChannelListener {
    final long pushRequestId;
    Runnable rpcSendOutCallback;

    PushChannelListener(long pushRequestId) {
      this(pushRequestId, null);
    }

    PushChannelListener(long pushRequestId, Runnable rpcSendOutCallback) {
      super("PUSH " + pushRequestId);
      this.pushRequestId = pushRequestId;
      this.rpcSendOutCallback = rpcSendOutCallback;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      super.operationComplete(future);
      if (rpcSendOutCallback != null) {
        rpcSendOutCallback.run();
      }
    }

    @Override
    protected void handleFailure(String errorMsg, Throwable cause) {
      handler.handlePushFailure(pushRequestId, errorMsg, cause);
    }
  }
}
