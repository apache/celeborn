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

import static org.apache.celeborn.common.network.protocol.Message.Type.OPEN_BUFFER_STREAM;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public class OpenBufferStream extends OpenStream {
  public int startSubIndex;
  public int endSubIndex;
  public int initialCredit;

  public OpenBufferStream(
      byte[] shuffleKey, byte[] fileName, int startSubIndex, int endSubIndex, int initialCredit) {
    super(shuffleKey, fileName);
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
    this.initialCredit = initialCredit;
  }

  public OpenBufferStream(
      String shuffleKey, String fileName, int startSubIndex, int endSubIndex, int initialCredit) {
    super(shuffleKey.getBytes(StandardCharsets.UTF_8), fileName.getBytes(StandardCharsets.UTF_8));
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
    this.initialCredit = initialCredit;
  }

  @Override
  public int encodedLength() {
    return 4 + shuffleKey.length + 4 + fileName.length + 4 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeInt(shuffleKey.length);
    buf.writeBytes(shuffleKey);
    buf.writeInt(fileName.length);
    buf.writeBytes(fileName);
    buf.writeInt(startSubIndex);
    buf.writeInt(endSubIndex);
    buf.writeInt(initialCredit);
  }

  @Override
  public Type type() {
    return OPEN_BUFFER_STREAM;
  }

  public static OpenBufferStream decode(ByteBuf in) {
    int shuffleKeyLength = in.readInt();
    byte[] tmpShuffleKey = new byte[shuffleKeyLength];
    in.readBytes(tmpShuffleKey);
    int fileNameLength = in.readInt();
    byte[] tmpFileName = new byte[fileNameLength];
    in.readBytes(tmpFileName);
    int startSubIndex = in.readInt();
    int endSubIndex = in.readInt();
    int initialCredit = in.readInt();
    return new OpenBufferStream(
        tmpShuffleKey, tmpFileName, startSubIndex, endSubIndex, initialCredit);
  }
}
