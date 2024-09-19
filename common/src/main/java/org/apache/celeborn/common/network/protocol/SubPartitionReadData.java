/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

/**
 * Comparing {@link ReadData}, this class has an additional field of subpartitionId. This class is
 * added to keep the backward compatibility.
 */
public class SubPartitionReadData extends RequestMessage {
  private long streamId;

  private int subPartitionId;

  public SubPartitionReadData(long streamId, int subPartitionId, ByteBuf buf) {
    super(new NettyManagedBuffer(buf));
    this.streamId = streamId;
    this.subPartitionId = subPartitionId;
  }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(subPartitionId);
  }

  public long getStreamId() {
    return streamId;
  }

  public int getSubPartitionId() {
    return subPartitionId;
  }

  @Override
  public Type type() {
    return Type.SUBPARTITION_READ_DATA;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubPartitionReadData readData = (SubPartitionReadData) o;
    return streamId == readData.streamId
        && subPartitionId == readData.subPartitionId
        && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamId, subPartitionId, super.hashCode());
  }

  @Override
  public String toString() {
    return "SubpartitionReadData{"
        + "streamId="
        + streamId
        + ", subPartitionId="
        + subPartitionId
        + '}';
  }
}
