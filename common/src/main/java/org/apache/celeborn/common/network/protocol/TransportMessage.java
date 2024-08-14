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

package org.apache.celeborn.common.network.protocol;

import static org.apache.celeborn.common.protocol.MessageType.*;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.protocol.*;

public class TransportMessage implements Serializable {
  private static final long serialVersionUID = -3259000920699629773L;
  private static Logger logger = LoggerFactory.getLogger(TransportMessage.class);
  @Deprecated private final transient MessageType type;
  private final int messageTypeValue;
  private final byte[] payload;

  public TransportMessage(MessageType type, byte[] payload) {
    this.type = type;
    this.messageTypeValue = type.getNumber();
    this.payload = payload;
  }

  public MessageType getType() {
    return type;
  }

  public int getMessageTypeValue() {
    return messageTypeValue;
  }

  public byte[] getPayload() {
    return payload;
  }

  public <T extends GeneratedMessageV3> T getParsedPayload() throws InvalidProtocolBufferException {
    switch (messageTypeValue) {
      case OPEN_STREAM_VALUE:
        return (T) PbOpenStream.parseFrom(payload);
      case STREAM_HANDLER_VALUE:
        return (T) PbStreamHandler.parseFrom(payload);
      case PUSH_DATA_HAND_SHAKE_VALUE:
        return (T) PbPushDataHandShake.parseFrom(payload);
      case REGION_START_VALUE:
        return (T) PbRegionStart.parseFrom(payload);
      case REGION_FINISH_VALUE:
        return (T) PbRegionFinish.parseFrom(payload);
      case BACKLOG_ANNOUNCEMENT_VALUE:
        return (T) PbBacklogAnnouncement.parseFrom(payload);
      case BUFFER_STREAM_END_VALUE:
        return (T) PbBufferStreamEnd.parseFrom(payload);
      case READ_ADD_CREDIT_VALUE:
        return (T) PbReadAddCredit.parseFrom(payload);
      case STREAM_CHUNK_SLICE_VALUE:
        return (T) PbStreamChunkSlice.parseFrom(payload);
      case CHUNK_FETCH_REQUEST_VALUE:
        return (T) PbChunkFetchRequest.parseFrom(payload);
      case TRANSPORTABLE_ERROR_VALUE:
        return (T) PbTransportableError.parseFrom(payload);
      case GET_SHUFFLE_ID_VALUE:
        return (T) PbGetShuffleId.parseFrom(payload);
      case GET_SHUFFLE_ID_RESPONSE_VALUE:
        return (T) PbGetShuffleIdResponse.parseFrom(payload);
      case REPORT_SHUFFLE_FETCH_FAILURE_VALUE:
        return (T) PbReportShuffleFetchFailure.parseFrom(payload);
      case REPORT_SHUFFLE_FETCH_FAILURE_RESPONSE_VALUE:
        return (T) PbReportShuffleFetchFailureResponse.parseFrom(payload);
      case REPORT_BARRIER_STAGE_ATTEMPT_FAILURE_VALUE:
        return (T) PbReportBarrierStageAttemptFailure.parseFrom(payload);
      case REPORT_BARRIER_STAGE_ATTEMPT_FAILURE_RESPONSE_VALUE:
        return (T) PbReportBarrierStageAttemptFailureResponse.parseFrom(payload);
      case SASL_REQUEST_VALUE:
        return (T) PbSaslRequest.parseFrom(payload);
      case AUTHENTICATION_INITIATION_REQUEST_VALUE:
        return (T) PbAuthenticationInitiationRequest.parseFrom(payload);
      case AUTHENTICATION_INITIATION_RESPONSE_VALUE:
        return (T) PbAuthenticationInitiationResponse.parseFrom(payload);
      case REGISTER_APPLICATION_REQUEST_VALUE:
        return (T) PbRegisterApplicationRequest.parseFrom(payload);
      case REGISTER_APPLICATION_RESPONSE_VALUE:
        return (T) PbRegisterApplicationResponse.parseFrom(payload);
      case APPLICATION_META_VALUE:
        return (T) PbApplicationMeta.parseFrom(payload);
      case APPLICATION_META_REQUEST_VALUE:
        return (T) PbApplicationMetaRequest.parseFrom(payload);
      case BATCH_OPEN_STREAM_VALUE:
        return (T) PbOpenStreamList.parseFrom(payload);
      case BATCH_OPEN_STREAM_RESPONSE_VALUE:
        return (T) PbOpenStreamListResponse.parseFrom(payload);
      case SEGMENT_START_VALUE:
        return (T) PbSegmentStart.parseFrom(payload);
      case NOTIFY_REQUIRED_SEGMENT_VALUE:
        return (T) PbNotifyRequiredSegment.parseFrom(payload);
      default:
        logger.error("Unexpected type {}", type);
    }
    return null;
  }

  public ByteBuffer toByteBuffer() {
    int totalBufferSize = payload.length + 4 + 4;
    ByteBuffer buffer = ByteBuffer.allocate(totalBufferSize);
    buffer.putInt(messageTypeValue);
    buffer.putInt(payload.length);
    buffer.put(payload);
    buffer.flip();
    return buffer;
  }

  public static TransportMessage fromByteBuffer(ByteBuffer buffer) throws CelebornIOException {
    int messageTypeValue = buffer.getInt();
    if (MessageType.forNumber(messageTypeValue) == null) {
      throw new CelebornIOException("Decode failed, fallback to legacy messages.");
    }
    int payloadLen = buffer.getInt();
    byte[] payload = new byte[payloadLen];
    buffer.get(payload);
    MessageType msgType = MessageType.forNumber(messageTypeValue);
    return new TransportMessage(msgType, payload);
  }
}
