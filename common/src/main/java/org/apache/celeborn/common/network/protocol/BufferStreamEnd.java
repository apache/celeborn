package org.apache.celeborn.common.network.protocol;

import io.netty.buffer.ByteBuf;

public class BufferStreamEnd extends RequestMessage {
  private long streamId;

  public BufferStreamEnd(long streamId) {
    this.streamId = streamId;
  }

  @Override
  public int encodedLength() {
    return 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
  }

  @Override
  public Type type() {
    return Type.BUFFER_STREAM_END;
  }

  public static Message decode(ByteBuf buffer) {
    long streamId = buffer.readLong();
    return new BufferStreamEnd(streamId);
  }

  public long getStreamId() {
    return streamId;
  }
}
