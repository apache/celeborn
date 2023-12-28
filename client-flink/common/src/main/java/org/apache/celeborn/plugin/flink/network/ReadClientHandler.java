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

package org.apache.celeborn.plugin.flink.network;

import static org.apache.celeborn.common.protocol.MessageType.BACKLOG_ANNOUNCEMENT_VALUE;
import static org.apache.celeborn.common.protocol.MessageType.BUFFER_STREAM_END_VALUE;
import static org.apache.celeborn.common.protocol.MessageType.TRANSPORTABLE_ERROR_VALUE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.BacklogAnnouncement;
import org.apache.celeborn.common.network.protocol.BufferStreamEnd;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.protocol.TransportableError;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.plugin.flink.protocol.ReadData;
import org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData;

public class ReadClientHandler extends BaseMessageHandler {
  private static Logger logger = LoggerFactory.getLogger(ReadClientHandler.class);
  private ConcurrentHashMap<Long, Consumer<RequestMessage>> streamHandlers =
      JavaUtils.newConcurrentHashMap();
  private ConcurrentHashMap<Long, TransportClient> streamClients = JavaUtils.newConcurrentHashMap();

  public void registerHandler(
      long streamId, Consumer<RequestMessage> handle, TransportClient client) {
    streamHandlers.put(streamId, handle);
    streamClients.put(streamId, client);
  }

  public void removeHandler(long streamId) {
    streamHandlers.remove(streamId);
    streamClients.remove(streamId);
  }

  private void processMessageInternal(long streamId, RequestMessage msg) {
    Consumer<RequestMessage> handler = streamHandlers.get(streamId);
    if (handler != null) {
      logger.debug("received streamId: {}, msg :{}", streamId, msg);
      handler.accept(msg);
    } else {
      if (msg != null && msg instanceof ReadData) {
        ((ReadData) msg).getFlinkBuffer().release();
      } else if (msg != null && msg instanceof SubPartitionReadData) {
        ((SubPartitionReadData) msg).getFlinkBuffer().release();
      }

      logger.warn("Unexpected streamId received: {}", streamId);
    }
  }

  @Override
  public void receive(TransportClient client, RequestMessage msg, RpcResponseCallback callback) {
    receive(client, msg);
  }

  @Override
  public void receive(TransportClient client, RequestMessage msg) {
    switch (msg.type()) {
      case READ_DATA:
        ReadData readData = (ReadData) msg;
        processMessageInternal(readData.getStreamId(), readData);
        break;
      case SUBPARTITION_READ_DATA:
        SubPartitionReadData subPartitionReadData = (SubPartitionReadData) msg;
        processMessageInternal(subPartitionReadData.getStreamId(), subPartitionReadData);
        break;
      case BACKLOG_ANNOUNCEMENT:
        BacklogAnnouncement backlogAnnouncement = (BacklogAnnouncement) msg;
        processMessageInternal(backlogAnnouncement.getStreamId(), backlogAnnouncement);
        break;
      case TRANSPORTABLE_ERROR:
        TransportableError transportableError = ((TransportableError) msg);
        logger.warn(
            "Received TransportableError from worker {} with content {}",
            client.getSocketAddress().toString(),
            transportableError.getErrorMessage());
        processMessageInternal(transportableError.getStreamId(), transportableError);
        break;
      case BUFFER_STREAM_END:
        BufferStreamEnd streamEnd = (BufferStreamEnd) msg;
        processMessageInternal(streamEnd.getStreamId(), streamEnd);
        break;
      case RPC_REQUEST:
        try {
          TransportMessage transportMessage =
              TransportMessage.fromByteBuffer(msg.body().nioByteBuffer());
          switch (transportMessage.getMessageTypeValue()) {
            case BACKLOG_ANNOUNCEMENT_VALUE:
              receive(client, BacklogAnnouncement.fromProto(transportMessage.getParsedPayload()));
              break;
            case BUFFER_STREAM_END_VALUE:
              receive(client, BufferStreamEnd.fromProto(transportMessage.getParsedPayload()));
              break;
            case TRANSPORTABLE_ERROR_VALUE:
              receive(client, TransportableError.fromProto(transportMessage.getParsedPayload()));
              break;
          }
        } catch (IOException e) {
          logger.warn("Failed to process RpcRequest message {}. ", msg, e);
        }
        break;
      case ONE_WAY_MESSAGE:
        // ignore it.
        break;
      default:
        logger.error("Unexpected msg type {} content {}", msg.type(), msg);
    }
  }

  @Override
  public boolean checkRegistered() {
    return true;
  }

  @Override
  public void channelInactive(TransportClient client) {
    streamClients.forEach(
        (streamId, savedClient) -> {
          if (savedClient == client) {
            String message =
                "Client "
                    + client.getSocketAddress()
                    + " is lost, notify related stream "
                    + streamId;
            logger.warn(message);
            processMessageInternal(
                streamId,
                new TransportableError(streamId, message.getBytes(StandardCharsets.UTF_8)));
          }
        });
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    logger.warn("exception caught {}", client.getSocketAddress(), cause);
  }

  public void close() {
    streamHandlers.clear();
    for (TransportClient value : streamClients.values()) {
      value.close();
    }
    streamClients.clear();
  }
}
