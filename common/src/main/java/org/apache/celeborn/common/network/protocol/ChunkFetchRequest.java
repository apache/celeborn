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

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link ResponseMessage} (either success or failure).
 */
@Deprecated
public final class ChunkFetchRequest extends RequestMessage {
  public final StreamChunkSlice streamChunkSlice;

  public ChunkFetchRequest(StreamChunkSlice streamChunkSlice) {
    this.streamChunkSlice = streamChunkSlice;
  }

  @Override
  public Type type() {
    return Type.CHUNK_FETCH_REQUEST;
  }

  @Override
  public int encodedLength() {
    return streamChunkSlice.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    streamChunkSlice.encode(buf);
  }

  public static ChunkFetchRequest decode(ByteBuf buf) {
    return new ChunkFetchRequest(StreamChunkSlice.decode(buf));
  }

  @Override
  public int hashCode() {
    return streamChunkSlice.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchRequest) {
      ChunkFetchRequest o = (ChunkFetchRequest) other;
      return streamChunkSlice.equals(o.streamChunkSlice);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("streamChunkId", streamChunkSlice)
        .toString();
  }
}
