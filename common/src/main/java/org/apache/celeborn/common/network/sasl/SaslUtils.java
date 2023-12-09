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

package org.apache.celeborn.common.network.sasl;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.security.sasl.Sasl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;

public class SaslUtils {
  /** Sasl Mechanisms */
  static final String DIGEST = "DIGEST-MD5";

  /** Quality of protection value that does not include encryption. */
  static final String QOP_AUTH = "auth";

  static final String DEFAULT_REALM = "default";

  static final Map<String, String> DEFAULT_SASL_CLIENT_PROPS =
      ImmutableMap.<String, String>builder().put(Sasl.QOP, QOP_AUTH).build();

  /* Encode a byte[] identifier as a Base64-encoded string. */
  static String encodeIdentifier(String identifier) {
    Preconditions.checkNotNull(identifier, "User cannot be null if SASL is enabled");
    return getBase64EncodedString(identifier);
  }

  /** Encode a password as a base64-encoded char[] array. */
  static char[] encodePassword(String password) {
    Preconditions.checkNotNull(password, "Password cannot be null if SASL is enabled");
    return getBase64EncodedString(password).toCharArray();
  }

  /** Return a Base64-encoded string. */
  private static String getBase64EncodedString(String str) {
    ByteBuf byteBuf = null;
    ByteBuf encodedByteBuf = null;
    try {
      byteBuf = Unpooled.wrappedBuffer(str.getBytes(StandardCharsets.UTF_8));
      encodedByteBuf = Base64.encode(byteBuf);
      return encodedByteBuf.toString(StandardCharsets.UTF_8);
    } finally {
      // The release is called to suppress the memory leak error messages raised by netty.
      if (byteBuf != null) {
        byteBuf.release();
        if (encodedByteBuf != null) {
          encodedByteBuf.release();
        }
      }
    }
  }
}
