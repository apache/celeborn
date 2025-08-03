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

/**
 * Identifier for a fixed number of chunks to read from a stream created by an "open blocks"
 * message. Use PbStreamHandler instead of this.
 */
@Deprecated
public final class StreamHandle extends RequestMessage {
  public final long streamId;
  public final int numChunks;

  public StreamHandle(long streamId, int numChunks) {
    this.streamId = streamId;
    this.numChunks = numChunks;
  }

  @Override
  public Type type() {
    return Type.STREAM_HANDLE;
  }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(numChunks);
  }

  public static StreamHandle decode(ByteBuf buf) {
    return new StreamHandle(buf.readLong(), buf.readInt());
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamId, numChunks);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamHandle) {
      StreamHandle o = (StreamHandle) other;
      return streamId == o.streamId && numChunks == o.numChunks;
    }
    return false;
  }

  @Override
  public String toString() {
    return "StreamHandle[streamId=" + streamId + ",numChunks=" + numChunks + "]";
  }
}
