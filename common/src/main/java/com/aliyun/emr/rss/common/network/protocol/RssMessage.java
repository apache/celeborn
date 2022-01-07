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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import com.google.protobuf.GeneratedMessageV3;

import com.aliyun.emr.rss.common.protocol.RssMessages.MessageType;

public class RssMessage implements Serializable {
  private static final long serialVersionUID = -3259000920699629773L;
  private MessageType type;
  private byte[] proto;

  private RssMessage() {
    type = null;
    proto = null;
  }

  public MessageType getType() {
    return type;
  }

  public byte[] getProto() {
    return proto;
  }

  public RssMessage(MessageType type) {
    this.type = type;
  }

  public RssMessage(MessageType type, byte[] proto) {
    this.type = type;
    this.proto = proto;
  }

  public RssMessage ptype(MessageType type) {
    this.type = type;
    return this;
  }

  public RssMessage proto(GeneratedMessageV3 proto) {
    this.proto = proto.toByteArray();
    return this;
  }

  public static RssMessage newMessage() {
    return new RssMessage();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RssMessage that = (RssMessage) o;
    return type == that.type && Arrays.equals(proto, that.proto);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type);
    result = 31 * result + Arrays.hashCode(proto);
    return result;
  }
}
