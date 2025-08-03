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

import java.util.Objects;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.server.BaseMessageHandler;

/**
 * A generic RPC which is handled by a remote {@link BaseMessageHandler}. This will correspond to a
 * single {@link ResponseMessage} (either success or failure).
 */
public final class RpcRequest extends RequestMessage {
  /** Used to link an RPC request with its response. */
  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message);
    this.requestId = requestId;
  }

  @Override
  public Type type() {
    return Type.RPC_REQUEST;
  }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  public static RpcRequest decode(ByteBuf buf) {
    return decode(buf, true);
  }

  public static RpcRequest decode(ByteBuf buf, boolean decodeBody) {
    long requestId = buf.readLong();
    // See comment in encodedLength().
    buf.readInt();
    if (decodeBody) {
      return new RpcRequest(requestId, new NettyManagedBuffer(buf));
    } else {
      return new RpcRequest(requestId, NettyManagedBuffer.EmptyBuffer);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return "RpcRequest[requestId=" + requestId + ",body=" + body() + "]";
  }
}
