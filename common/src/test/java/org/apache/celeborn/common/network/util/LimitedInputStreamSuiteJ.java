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

package org.apache.celeborn.common.network.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

public class LimitedInputStreamSuiteJ {

  @Test
  public void testZeroLengthReadAtZeroLimit() throws IOException {
    try (LimitedInputStream stream =
        new LimitedInputStream(new ByteArrayInputStream(new byte[] {1}), 0)) {
      byte[] buffer = new byte[1];
      assertEquals(0, stream.read(buffer, 0, 0));
      assertEquals(-1, stream.read(buffer, 0, 1));
    }
  }

  @Test
  public void testZeroLengthReadAfterLimitReached() throws IOException {
    ByteArrayInputStream wrapped = new ByteArrayInputStream(new byte[] {1, 2, 3});
    try (LimitedInputStream stream = new LimitedInputStream(wrapped, 2)) {
      byte[] buffer = new byte[3];
      assertEquals(0, stream.read(buffer, 0, 0));
      assertEquals(2, stream.read(buffer, 0, buffer.length));
      assertEquals(1, buffer[0]);
      assertEquals(2, buffer[1]);
      assertEquals(0, stream.read(buffer, buffer.length, 0));
      assertEquals(-1, stream.read(buffer, 0, 1));
      assertEquals(3, wrapped.read());
    }
  }

  @Test
  public void testZeroLengthReadAtUnderlyingEof() throws IOException {
    try (LimitedInputStream stream =
        new LimitedInputStream(new ByteArrayInputStream(new byte[0]), 1)) {
      byte[] buffer = new byte[1];
      assertEquals(-1, stream.read(buffer, 0, 1));
      assertEquals(0, stream.read(buffer, 0, 0));
    }
  }

  @Test
  public void testInvalidReadArgumentsAtZeroLimit() throws IOException {
    try (LimitedInputStream stream =
        new LimitedInputStream(new ByteArrayInputStream(new byte[0]), 0)) {
      byte[] buffer = new byte[1];
      assertThrows(NullPointerException.class, () -> stream.read(null, 0, 0));
      assertThrows(IndexOutOfBoundsException.class, () -> stream.read(buffer, -1, 0));
      assertThrows(IndexOutOfBoundsException.class, () -> stream.read(buffer, 0, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> stream.read(buffer, 2, 0));
    }
  }
}
