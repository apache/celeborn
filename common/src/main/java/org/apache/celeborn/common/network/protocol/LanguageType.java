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

/**
 * LanguageType represents which language the message is deserialized from / will be serialized
 * into. For JAVA, the leading byte would be 0xAC according to Java's serialization stack. For CPP,
 * the leading byte would be 0xFF as defined in CelebornCpp module. In this way, messages from/for
 * different language could be distinguished and deser/ser accordingly.
 */
public enum LanguageType {
  JAVA((byte) 0xAC),
  CPP((byte) 0xFF);

  private final byte marker;

  LanguageType(byte marker) {
    this.marker = marker;
  }

  public byte getMarker() {
    return marker;
  }
}
