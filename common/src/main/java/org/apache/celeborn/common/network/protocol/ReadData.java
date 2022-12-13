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

import java.util.Arrays;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

public class ReadData extends RequestMessage {
  private long streamId;
  private int backlog;
  private int[] offsets;
  private ByteBuf buf;

  protected static class StreamState {

    final Channel associatedChannel;

    volatile long chunksBeingTransferred = 0L;

    StreamState(Channel channel) {
      this.associatedChannel = channel;
    }
  }

  public ReadData(long streamId, int backlog, int[] offsets, ByteBuf buf) {
    this.streamId = streamId;
    this.backlog = backlog;
    this.offsets = offsets;
    this.buf = buf;
  }

  @Override
  public int encodedLength() {
    return 8 + 4 + 4 + offsets.length * 4 + 4 + buf.readableBytes();
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(backlog);
    buf.writeInt(offsets.length);
    for (int offset : offsets) {
      buf.writeInt(offset);
    }
    buf.writeInt(buf.readableBytes());
    buf.writeBytes(buf);
  }

  public static ReadData decode(ByteBuf buf) {
    long streamId = buf.readLong();
    int credit = buf.readInt();
    int offsetCount = buf.readInt();
    int offsets[] = new int[offsetCount];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = buf.readInt();
    }
    int tmpBufSize = buf.readInt();
    ByteBuf tmpBuf = Unpooled.buffer(tmpBufSize, tmpBufSize);
    buf.readBytes(tmpBuf);
    return new ReadData(streamId, credit, offsets, tmpBuf);
  }

  public long getStreamId() {
    return streamId;
  }

  public int getBacklog() {
    return backlog;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public ByteBuf getBuf() {
    return buf;
  }

  @Override
  public Type type() {
    return Type.READ_DATA;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReadData readData = (ReadData) o;
    return streamId == readData.streamId
        && backlog == readData.backlog
        && Arrays.equals(offsets, readData.offsets)
        && buf.equals(readData.buf);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(streamId, backlog, buf);
    result = 31 * result + Arrays.hashCode(offsets);
    return result;
  }

  @Override
  public String toString() {
    return "ReadData{"
        + "streamId="
        + streamId
        + ", backlog="
        + backlog
        + ", offsets="
        + Arrays.toString(offsets)
        + ", buf="
        + buf
        + '}';
  }
}
