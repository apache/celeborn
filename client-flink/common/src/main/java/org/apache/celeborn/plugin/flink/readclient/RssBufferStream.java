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

package org.apache.celeborn.plugin.flink.readclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.plugin.flink.network.FlinkTransportClientFactory;

public class RssBufferStream {

  private static final Logger logger = LoggerFactory.getLogger(RssBufferStream.class);
  private FlinkTransportClientFactory clientFactory;
  private String shuffleKey;
  private PartitionLocation[] locations;
  private int subIndexStart;
  private int subIndexEnd;
  private TransportClient client;
  private volatile int currentLocationIndex = 0;
  private volatile long streamId = 0;
  private Supplier<ByteBuf> supplier;
  private int initialCredit;
  private FlinkShuffleClientImpl mapShuffleClient;
  private Consumer<RequestMessage> messageConsumer;
  private Consumer<Throwable> failureListener;

  public RssBufferStream() {}

  public RssBufferStream(
      FlinkTransportClientFactory dataClientFactory,
      String shuffleKey,
      PartitionLocation[] locations,
      int subIndexStart,
      int subIndexEnd) {
    this.clientFactory = dataClientFactory;
    this.shuffleKey = shuffleKey;
    this.locations = locations;
    this.subIndexStart = subIndexStart;
    this.subIndexEnd = subIndexEnd;
  }

  public void initByReader(
      Supplier<ByteBuf> supplier,
      int initialCredit,
      FlinkShuffleClientImpl mapShuffleClient,
      Consumer<RequestMessage> messageConsumer,
      Consumer<Throwable> failureListener) {
    this.supplier = supplier;
    this.initialCredit = initialCredit;
    this.mapShuffleClient = mapShuffleClient;
    this.messageConsumer = messageConsumer;
    this.failureListener = failureListener;
  }

  public void open() throws IOException, InterruptedException {
    this.client =
        clientFactory.createClient(
            locations[currentLocationIndex].getHost(),
            locations[currentLocationIndex].getFetchPort());

    String fileName = locations[currentLocationIndex].getFileName();
    OpenStreamWithCredit openBufferStream =
        new OpenStreamWithCredit(shuffleKey, fileName, subIndexStart, subIndexEnd, initialCredit);
    client.sendRpc(
        openBufferStream.toByteBuffer(),
        new RpcResponseCallback() {

          @Override
          public void onSuccess(ByteBuffer response) {
            StreamHandle streamHandle = (StreamHandle) Message.decode(response);
            RssBufferStream.this.streamId = streamHandle.streamId;
            clientFactory.registerSupplier(RssBufferStream.this.streamId, supplier);
            mapShuffleClient
                .getReadClientHandler()
                .registerHandler(streamId, messageConsumer, client);
            logger.debug("open stream success stream id:{}, fileName: {}", streamId, fileName);
          }

          @Override
          public void onFailure(Throwable e) {
            throw new RuntimeException("OpenStream failed.", e);
          }
        });
  }

  public void addCredit(ReadAddCredit addCredit) {
    this.client
        .getChannel()
        .writeAndFlush(addCredit)
        .addListener(
            future -> {
              if (!future.isSuccess()) {
                // Send ReadAddCredit do not expect response.
                logger.warn(
                    "Send ReadAddCredit to {} failed, detail {}",
                    this.client.getSocketAddress().toString(),
                    future.cause());
              }
            });
  }

  public static RssBufferStream empty() {
    return emptyRssBufferStream;
  }

  public long getStreamId() {
    return streamId;
  }

  public static RssBufferStream create(
      FlinkTransportClientFactory dataClientFactory,
      String shuffleKey,
      PartitionLocation[] locations,
      int subIndexStart,
      int subIndexEnd) {
    if (locations == null || locations.length == 0) {
      return empty();
    } else {
      return new RssBufferStream(
          dataClientFactory, shuffleKey, locations, subIndexStart, subIndexEnd);
    }
  }

  private static final RssBufferStream emptyRssBufferStream = new RssBufferStream();

  public TransportClient getClient() {
    return client;
  }

  public void close() {
    freeSupplier(streamId);
  }

  private void freeSupplier(long streamId) {
    clientFactory.unregisterSupplier(streamId);
  }

  public void endStream(long streamId) {
    if (this.streamId != streamId) {
      logger.error("Unexpected end of stream with {}", streamId);
      return;
    }

    if (currentLocationIndex >= locations.length) {
      return;
    }

    currentLocationIndex++;
    freeSupplier(streamId);
    // release netty thread
    Thread thread =
        new Thread(
            () -> {
              try {
                this.open();
              } catch (Exception e) {
                this.failureListener.accept(
                    new IOException(
                        "Read location " + locations[currentLocationIndex] + " failed.", e));
              }
            });
    thread.start();
  }
}
