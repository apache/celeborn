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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.celeborn.common.protocol.message.ControlMessages.UserIdentifier;

/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public final class OpenStream extends RequestMessage {
  public byte[] shuffleKey;
  public byte[] fileName;
  public byte[] userIdentifier;
  public int startMapIndex;
  public int endMapIndex;

  public OpenStream(
      String shuffleKey,
      String fileName,
      UserIdentifier userIdentifier,
      int startMapIndex,
      int endMapIndex) {
    this(
        shuffleKey.getBytes(StandardCharsets.UTF_8),
        fileName.getBytes(StandardCharsets.UTF_8),
        userIdentifier.toString().getBytes(StandardCharsets.UTF_8),
        startMapIndex,
        endMapIndex);
  }

  public OpenStream(
      byte[] shuffleKey,
      byte[] fileName,
      byte[] userIdentifier,
      int startMapIndex,
      int endMapIndex) {
    this.shuffleKey = shuffleKey;
    this.fileName = fileName;
    this.userIdentifier = userIdentifier;
    this.startMapIndex = startMapIndex;
    this.endMapIndex = endMapIndex;
  }

  @Override
  public Type type() {
    return Type.OPEN_STREAM;
  }

  @Override
  public int encodedLength() {
    return 4 + shuffleKey.length + 4 + fileName.length + 4 + userIdentifier.length + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeInt(shuffleKey.length);
    buf.writeBytes(shuffleKey);
    buf.writeInt(fileName.length);
    buf.writeBytes(fileName);
    buf.writeInt(userIdentifier.length);
    buf.writeBytes(userIdentifier);
    buf.writeInt(startMapIndex);
    buf.writeInt(endMapIndex);
  }

  public static OpenStream decode(ByteBuf buf) {
    int shuffleKeySize = buf.readInt();
    byte[] shuffleKey = new byte[shuffleKeySize];
    buf.readBytes(shuffleKey);
    int fileNameSize = buf.readInt();
    byte[] fileName = new byte[fileNameSize];
    buf.readBytes(fileName);
    int userIdentifierSize = buf.readInt();
    byte[] userIdentifier = new byte[userIdentifierSize];
    buf.readBytes(userIdentifier);
    return new OpenStream(shuffleKey, fileName, userIdentifier, buf.readInt(), buf.readInt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, fileName, userIdentifier, startMapIndex, endMapIndex);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OpenStream) {
      OpenStream o = (OpenStream) other;
      return startMapIndex == o.startMapIndex
          && endMapIndex == o.endMapIndex
          && Arrays.equals(shuffleKey, o.shuffleKey)
          && Arrays.equals(fileName, o.fileName)
          && Arrays.equals(userIdentifier, o.userIdentifier);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("shuffleKey", new String(shuffleKey, StandardCharsets.UTF_8))
        .add("fileName", new String(fileName, StandardCharsets.UTF_8))
        .add("userIdentifier", new String(userIdentifier, StandardCharsets.UTF_8))
        .add("startMapIndex", startMapIndex)
        .add("endMapIndex", endMapIndex)
        .toString();
  }
}
