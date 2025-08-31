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

package org.apache.celeborn.plugin.flink.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.protocol.TransportableError;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbNotifyRequiredSegment;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbReadAddCredit;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.plugin.flink.network.FlinkTransportClientFactory;

public class CelebornBufferStream {
  private static Logger logger = LoggerFactory.getLogger(CelebornBufferStream.class);
  private FlinkTransportClientFactory clientFactory;
  private String shuffleKey;
  private PartitionLocation[] locations;
  private int subIndexStart;
  private int subIndexEnd;
  private long pushDataTimeoutMs;
  private TransportClient client;
  private AtomicInteger currentLocationIndex = new AtomicInteger(0);
  private long streamId = 0;
  private FlinkShuffleClientImpl mapShuffleClient;
  private boolean isClosed;
  private boolean isOpenSuccess;
  private final Object lock = new Object();
  private Supplier<ByteBuf> bufferSupplier;
  private int initialCredit;
  private Consumer<RequestMessage> messageConsumer;
  private ExecutorService openStreamThreadPool;

  public CelebornBufferStream() {}

  public CelebornBufferStream(
      FlinkShuffleClientImpl mapShuffleClient,
      FlinkTransportClientFactory dataClientFactory,
      String shuffleKey,
      PartitionLocation[] locations,
      int subIndexStart,
      int subIndexEnd,
      long pushDataTimeoutMs,
      ExecutorService openStreamThreadPool) {
    this.mapShuffleClient = mapShuffleClient;
    this.clientFactory = dataClientFactory;
    this.shuffleKey = shuffleKey;
    this.locations = locations;
    this.subIndexStart = subIndexStart;
    this.subIndexEnd = subIndexEnd;
    this.pushDataTimeoutMs = pushDataTimeoutMs;
    this.openStreamThreadPool = openStreamThreadPool;
  }

  public void open(
      Supplier<ByteBuf> bufferSupplier,
      int initialCredit,
      Consumer<RequestMessage> messageConsumer,
      boolean sync) {
    this.bufferSupplier = bufferSupplier;
    this.initialCredit = initialCredit;
    this.messageConsumer = messageConsumer;
    moveToNextPartitionIfPossible(0, null, sync);
  }

  public void addCredit(PbReadAddCredit pbReadAddCredit) {
    this.client.sendRpc(
        new TransportMessage(MessageType.READ_ADD_CREDIT, pbReadAddCredit.toByteArray())
            .toByteBuffer(),
        new RpcResponseCallback() {

          @Override
          public void onSuccess(ByteBuffer response) {
            // Send PbReadAddCredit do not expect response.
          }

          @Override
          public void onFailure(Throwable e) {
            logger.error(
                "Send PbReadAddCredit to {} failed, streamId {}, detail {}",
                NettyUtils.getRemoteAddress(client.getChannel()),
                streamId,
                e.getCause());
            messageConsumer.accept(new TransportableError(streamId, e));
          }
        });
  }

  public void notifyRequiredSegment(PbNotifyRequiredSegment pbNotifyRequiredSegment) {
    this.client.sendRpc(
        new TransportMessage(
                MessageType.NOTIFY_REQUIRED_SEGMENT, pbNotifyRequiredSegment.toByteArray())
            .toByteBuffer(),
        new RpcResponseCallback() {

          @Override
          public void onSuccess(ByteBuffer response) {
            // Send PbNotifyRequiredSegment do not expect response.
          }

          @Override
          public void onFailure(Throwable e) {
            logger.error(
                "Send PbNotifyRequiredSegment to {} failed, streamId {}, detail {}",
                NettyUtils.getRemoteAddress(client.getChannel()),
                streamId,
                e.getCause());
            messageConsumer.accept(new TransportableError(streamId, e));
          }
        });
  }

  public static CelebornBufferStream empty() {
    return EMPTY_CELEBORN_BUFFER_STREAM;
  }

  public static boolean isEmptyStream(CelebornBufferStream stream) {
    return stream == null || stream == EMPTY_CELEBORN_BUFFER_STREAM;
  }

  public long getStreamId() {
    return streamId;
  }

  public static CelebornBufferStream create(
      FlinkShuffleClientImpl client,
      FlinkTransportClientFactory dataClientFactory,
      String shuffleKey,
      PartitionLocation[] locations,
      int subIndexStart,
      int subIndexEnd,
      long pushDataTimeoutMs,
      ExecutorService openStreamThreadPool) {
    if (locations == null || locations.length == 0) {
      return empty();
    } else {
      return new CelebornBufferStream(
          client,
          dataClientFactory,
          shuffleKey,
          locations,
          subIndexStart,
          subIndexEnd,
          pushDataTimeoutMs,
          openStreamThreadPool);
    }
  }

  private static final CelebornBufferStream EMPTY_CELEBORN_BUFFER_STREAM =
      new CelebornBufferStream();

  private void closeStream(long streamId) {
    if (client != null && client.isActive()) {
      client.sendRpc(
          new TransportMessage(
                  MessageType.BUFFER_STREAM_END,
                  PbBufferStreamEnd.newBuilder()
                      .setStreamType(StreamType.CreditStream)
                      .setStreamId(streamId)
                      .build()
                      .toByteArray())
              .toByteBuffer());
    }
  }

  private void cleanupStream(long streamId) {
    if (isOpenSuccess) {
      mapShuffleClient.getReadClientHandler().removeHandler(streamId);
      clientFactory.unregisterSupplier(streamId);
      closeStream(streamId);
      isOpenSuccess = false;
    }
  }

  public void close() {
    synchronized (lock) {
      cleanupStream(streamId);
      isClosed = true;
    }
  }

  public void moveToNextPartitionIfPossible(
      long endedStreamId,
      @Nullable BiConsumer<Long, Integer> requiredSegmentIdConsumer,
      boolean sync) {
    logger.debug(
        "MoveToNextPartitionIfPossible in this: {},  endedStreamId: {}, currentLocationIndex: {}, currentSteamId: {}, locationsLength: {}.",
        this,
        endedStreamId,
        currentLocationIndex.get(),
        streamId,
        locations.length);
    if (currentLocationIndex.get() > 0) {
      logger.debug("Get end streamId {}", endedStreamId);
      cleanupStream(endedStreamId);
    }

    if (currentLocationIndex.get() < locations.length) {
      if (sync) {
        try {
          openStreamInternal(requiredSegmentIdConsumer, true);
          logger.debug(
              "MoveToNextPartitionIfPossible after openStream this: {},  endedStreamId: {}, currentLocationIndex: {}, currentSteamId: {}, locationsLength: {}.",
              this,
              endedStreamId,
              currentLocationIndex.get(),
              streamId,
              locations.length);
        } catch (Exception e) {
          logger.warn("Failed to open stream and report to flink framework. ", e);
          messageConsumer.accept(new TransportableError(0L, e));
        }
      } else {
        CompletableFuture.runAsync(
                () -> {
                  try {
                    openStreamInternal(requiredSegmentIdConsumer, false);
                  } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                },
                openStreamThreadPool)
            .whenComplete(
                (result, throwable) -> {
                  if (throwable == null) {
                    logger.debug(
                        "MoveToNextPartitionIfPossible after openStream this: {},  endedStreamId: {}, currentLocationIndex: {}, currentSteamId: {}, locationsLength: {}.",
                        this,
                        endedStreamId,
                        currentLocationIndex.get(),
                        streamId,
                        locations.length);
                  } else {
                    logger.warn("Failed to open stream and report to flink framework. ", throwable);
                    messageConsumer.accept(new TransportableError(0L, throwable));
                  }
                });
      }
    }
  }

  /**
   * Open the stream, note that if the openReaderFuture is not null, requiredSegmentIdConsumer will
   * be invoked for every subPartition when open stream success.
   */
  private void openStreamInternal(
      @Nullable BiConsumer<Long, Integer> requiredSegmentIdConsumer, boolean sync)
      throws IOException, InterruptedException {
    this.client =
        clientFactory.createClientWithRetry(
            locations[currentLocationIndex.get()].getHost(),
            locations[currentLocationIndex.get()].getFetchPort());
    String fileName = locations[currentLocationIndex.getAndIncrement()].getFileName();
    TransportMessage openStream =
        new TransportMessage(
            MessageType.OPEN_STREAM,
            PbOpenStream.newBuilder()
                .setShuffleKey(shuffleKey)
                .setFileName(fileName)
                .setStartIndex(subIndexStart)
                .setEndIndex(subIndexEnd)
                .setInitialCredit(initialCredit)
                .build()
                .toByteArray());
    RpcResponseCallback rpcResponseCallback =
        new RpcResponseCallback() {

          @Override
          public void onSuccess(ByteBuffer response) {
            try {
              PbStreamHandler pbStreamHandler =
                  TransportMessage.fromByteBuffer(response).getParsedPayload();
              CelebornBufferStream.this.streamId = pbStreamHandler.getStreamId();
              synchronized (lock) {
                if (!isClosed) {
                  clientFactory.registerSupplier(
                      CelebornBufferStream.this.streamId, bufferSupplier);
                  mapShuffleClient
                      .getReadClientHandler()
                      .registerHandler(streamId, messageConsumer, client);
                  isOpenSuccess = true;
                  if (requiredSegmentIdConsumer != null) {
                    for (int subPartitionId = subIndexStart;
                        subPartitionId <= subIndexEnd;
                        subPartitionId++) {
                      requiredSegmentIdConsumer.accept(streamId, subPartitionId);
                    }
                  }
                  logger.debug(
                      "open stream success from remote:{}, stream id:{}, fileName: {}",
                      client.getSocketAddress(),
                      streamId,
                      fileName);
                } else {
                  logger.debug(
                      "open stream success from remote:{}, but stream reader is already closed, stream id:{}, fileName: {}",
                      client.getSocketAddress(),
                      streamId,
                      fileName);
                  closeStream(streamId);
                }
              }
            } catch (Exception e) {
              logger.error(
                  "Open file {} stream for {} error from {}",
                  fileName,
                  shuffleKey,
                  NettyUtils.getRemoteAddress(client.getChannel()));
              messageConsumer.accept(new TransportableError(streamId, e));
            }
          }

          @Override
          public void onFailure(Throwable e) {
            logger.error(
                "Open file {} stream for {} error from {}",
                fileName,
                shuffleKey,
                NettyUtils.getRemoteAddress(client.getChannel()));
            messageConsumer.accept(new TransportableError(streamId, e));
          }
        };

    if (sync) {
      client.sendRpcSync(openStream.toByteBuffer(), rpcResponseCallback, pushDataTimeoutMs);
    } else {
      client.sendRpc(openStream.toByteBuffer(), rpcResponseCallback);
    }
  }

  public TransportClient getClient() {
    return client;
  }

  public boolean isOpened() {
    return isOpenSuccess;
  }
}
