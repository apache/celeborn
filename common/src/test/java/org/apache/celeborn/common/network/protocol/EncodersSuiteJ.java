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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

public class EncodersSuiteJ {

  private static final String[] TEST_STRINGS = {
    "",
    "ascii",
    "application_1700000000000_0001-0",
    "中文 shuffle key",
    "emoji 🚀🔥",
    "mixed ASCII 中文 🚀"
  };

  @Test
  public void testStringsRoundTrip() {
    for (String s : TEST_STRINGS) {
      ByteBuf buf = Unpooled.buffer(Encoders.Strings.encodedLength(s));
      Encoders.Strings.encode(buf, s);
      assertEquals(Encoders.Strings.encodedLength(s), buf.readableBytes());
      assertEquals(s, Encoders.Strings.decode(buf));
      assertEquals(0, buf.readableBytes());
      buf.release();
    }
  }

  @Test
  public void testStringsWireCompatibleWithGetBytesUtf8() {
    for (String s : TEST_STRINGS) {
      byte[] utf8Bytes = s.getBytes(StandardCharsets.UTF_8);
      ByteBuf expected = Unpooled.buffer(4 + utf8Bytes.length);
      expected.writeInt(utf8Bytes.length);
      expected.writeBytes(utf8Bytes);

      ByteBuf actual = Unpooled.buffer(Encoders.Strings.encodedLength(s));
      Encoders.Strings.encode(actual, s);

      byte[] expectedBytes = new byte[expected.readableBytes()];
      expected.readBytes(expectedBytes);
      byte[] actualBytes = new byte[actual.readableBytes()];
      actual.readBytes(actualBytes);
      assertArrayEquals(expectedBytes, actualBytes);

      expected.release();
      actual.release();
    }
  }

  @Test
  public void testStringArraysRoundTrip() {
    ByteBuf buf = Unpooled.buffer(Encoders.StringArrays.encodedLength(TEST_STRINGS));
    Encoders.StringArrays.encode(buf, TEST_STRINGS);
    assertEquals(Encoders.StringArrays.encodedLength(TEST_STRINGS), buf.readableBytes());
    assertArrayEquals(TEST_STRINGS, Encoders.StringArrays.decode(buf));
    assertEquals(0, buf.readableBytes());
    buf.release();
  }
}
