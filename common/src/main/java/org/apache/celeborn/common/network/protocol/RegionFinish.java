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

@Deprecated
public final class RegionFinish extends RequestMessage {

  // 0 for primary, 1 for replica, see PartitionLocation.Mode
  public final byte mode;

  public final String shuffleKey;
  public final String partitionUniqueId;
  public final int attemptId;

  public RegionFinish(byte mode, String shuffleKey, String partitionUniqueId, int attemptId) {
    this.mode = mode;
    this.shuffleKey = shuffleKey;
    this.partitionUniqueId = partitionUniqueId;
    this.attemptId = attemptId;
  }

  @Override
  public Type type() {
    return Type.REGION_FINISH;
  }

  @Override
  public int encodedLength() {
    return 1
        + Encoders.Strings.encodedLength(shuffleKey)
        + Encoders.Strings.encodedLength(partitionUniqueId)
        + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(mode);
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, partitionUniqueId);
    buf.writeInt(attemptId);
  }

  public static RegionFinish decode(ByteBuf buf) {
    byte mode = buf.readByte();
    String shuffleKey = Encoders.Strings.decode(buf);
    String partitionUniqueId = Encoders.Strings.decode(buf);
    int attemptId = buf.readInt();
    return new RegionFinish(mode, shuffleKey, partitionUniqueId, attemptId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, shuffleKey, partitionUniqueId, attemptId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RegionFinish) {
      RegionFinish o = (RegionFinish) other;
      return mode == o.mode
          && shuffleKey.equals(o.shuffleKey)
          && partitionUniqueId.equals(o.partitionUniqueId)
          && attemptId == o.attemptId
          && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return "RegionFinish[mode="
        + mode
        + ",shuffleKey="
        + shuffleKey
        + ",partitionUniqueId="
        + partitionUniqueId
        + ",attemptId="
        + attemptId
        + "]";
  }
}
