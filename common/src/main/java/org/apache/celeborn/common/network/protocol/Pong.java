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

/** Request to read a set of blocks. Returns {@link StreamHandle}. */
public final class Pong extends Message {
  byte type = Type.Pong.id();

  @Override
  public Type type() { return Type.Pong; }

  @Override
  public int encodedLength() {
    return 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(type);
  }

  public static Pong decode(ByteBuf buf) {
    buf.readByte();
    return new Pong();
  }

  @Override
  public int hashCode() {
    return Type.Pong.id();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Pong;
  }

  @Override
  public String toString() {
    return "Pong";
  }
}
