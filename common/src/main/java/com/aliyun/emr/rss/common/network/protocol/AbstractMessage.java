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

package com.aliyun.emr.rss.common.network.protocol;

import java.nio.ByteBuffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;

/**
 * Abstract class for messages which optionally contain a body kept in a separate buffer.
 */
public abstract class AbstractMessage implements Message {
  private final ManagedBuffer body;
  private final boolean isBodyInFrame;

  protected AbstractMessage() {
    this(null, false);
  }

  protected AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
    this.body = body;
    this.isBodyInFrame = isBodyInFrame;
  }

  @Override
  public ManagedBuffer body() {
    return body;
  }

  @Override
  public boolean isBodyInFrame() {
    return isBodyInFrame;
  }

  protected boolean equals(AbstractMessage other) {
    return isBodyInFrame == other.isBodyInFrame && Objects.equal(body, other.body);
  }

  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }

  public static AbstractMessage fromByteBuffer(ByteBuffer msg) {
    ByteBuf buf = Unpooled.wrappedBuffer(msg);
    Type type = Type.decode(buf);
    switch (type) {
      case OpenBlocks:
        return OpenBlocks.decode(buf);
      case StreamHandle:
        return StreamHandle.decode(buf);
      default: throw new IllegalArgumentException("Unknown message type: " + type);
    }
  }
}
