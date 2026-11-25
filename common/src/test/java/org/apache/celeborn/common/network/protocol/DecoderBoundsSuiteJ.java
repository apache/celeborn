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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.protocol.MessageType;

public class DecoderBoundsSuiteJ {
  private void assertInvalid(Function<ByteBuf, ?> decoder, int... values) {
    ByteBuf buf = Unpooled.buffer(128);
    try {
      for (int value : values) {
        buf.writeInt(value);
      }
      assertThrows(IllegalArgumentException.class, () -> decoder.apply(buf));
    } finally {
      buf.release();
    }
  }

  @Test
  public void rejectsInvalidLengthsBeforeAllocating() {
    for (int length : new int[] {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 1 << 30, 1}) {
      assertInvalid(Encoders.Strings::decode, length);
      assertInvalid(Encoders.IntArrays::decode, length);
      assertInvalid(Encoders.StringArrays::decode, length);
    }
    assertInvalid(Encoders.IntArrays::decode, (1 << 30) + 1, 0);
    assertInvalid(Encoders.StringArrays::decode, 1, Integer.MAX_VALUE);
  }

  @Test
  public void preservesValidEncodingsIncludingEmptyValues() {
    ByteBuf buf = Unpooled.buffer();
    try {
      for (String value : new String[] {"", "application-1", "中文"}) {
        Encoders.Strings.encode(buf, value);
        assertEquals(value, Encoders.Strings.decode(buf));
      }
      for (int[] value : new int[][] {{}, {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}}) {
        Encoders.IntArrays.encode(buf, value);
        assertArrayEquals(value, Encoders.IntArrays.decode(buf));
      }
      for (String[] value : new String[][] {{}, {"", "a", "中文"}}) {
        Encoders.StringArrays.encode(buf, value);
        assertArrayEquals(value, Encoders.StringArrays.decode(buf));
      }
      assertEquals(0, buf.readableBytes());
    } finally {
      buf.release();
    }
  }

  @Test
  public void usesReadableBytesRatherThanCapacityOrWriterIndex() {
    ByteBuf buf = Unpooled.buffer(128);
    try {
      buf.writeLong(0).writeInt(4).writeByte(1);
      buf.readerIndex(Long.BYTES);
      assertThrows(IllegalArgumentException.class, () -> Encoders.Strings.decode(buf));
    } finally {
      buf.release();
    }
  }

  @Test
  public void boundsTransportPayloadByRemainingBytes() throws Exception {
    int type = MessageType.SASL_REQUEST.getNumber();
    for (int length : new int[] {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 1}) {
      ByteBuffer buf = ByteBuffer.allocate(128);
      buf.putInt(0).putInt(type).putInt(length).flip();
      buf.position(Integer.BYTES);
      assertThrows(CelebornIOException.class, () -> TransportMessage.fromByteBuffer(buf));
    }
    for (SerdeVersion version : SerdeVersion.values()) {
      for (byte[] value : new byte[][] {{}, {1, 2, 3}}) {
        TransportMessage decoded =
            TransportMessage.fromByteBuffer(
                new TransportMessage(MessageType.SASL_REQUEST, value).toByteBuffer(), version);
        assertEquals(type, decoded.getMessageTypeValue());
        assertArrayEquals(value, decoded.getPayload());
        assertEquals(version, decoded.getSerdeVersion());
      }
    }
  }

  @Test
  public void boundsBothLegacyStreamFields() {
    for (int length : new int[] {-1, Integer.MAX_VALUE, 1 << 30}) {
      assertInvalid(OpenStream::decode, length);
      assertInvalid(OpenStream::decode, 0, length);
      assertInvalid(OpenStreamWithCredit::decode, length);
      assertInvalid(OpenStreamWithCredit::decode, 0, length);
      assertInvalid(TransportableError::decode, 0, 0, length);
    }
  }

  @Test
  public void preservesLegacyStreamAndErrorEncodings() {
    ByteBuf buf = Unpooled.buffer();
    try {
      OpenStream stream = new OpenStream("app-1-0", "0-0-0", 0, 2);
      stream.encode(buf);
      assertEquals(stream, OpenStream.decode(buf));
      OpenStreamWithCredit credit = new OpenStreamWithCredit("app-1-0", "0-0-0", 0, 2, 3);
      credit.encode(buf);
      OpenStreamWithCredit decoded = OpenStreamWithCredit.decode(buf);
      assertArrayEquals(credit.shuffleKey, decoded.shuffleKey);
      assertArrayEquals(credit.fileName, decoded.fileName);
      assertEquals(credit.startIndex, decoded.startIndex);
      assertEquals(credit.endIndex, decoded.endIndex);
      assertEquals(credit.initialCredit, decoded.initialCredit);
      TransportableError error = new TransportableError(1L, new byte[] {65});
      error.encode(buf);
      TransportableError decodedError = TransportableError.decode(buf);
      assertEquals(error.getStreamId(), decodedError.getStreamId());
      assertEquals(error.getErrorMessage(), decodedError.getErrorMessage());
      assertEquals(0, buf.readableBytes());
    } finally {
      buf.release();
    }
  }
}
