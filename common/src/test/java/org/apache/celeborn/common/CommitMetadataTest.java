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

package org.apache.celeborn.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class CommitMetadataTest {

  @Test
  public void testAddDataWithOffsetAndLength() {
    CommitMetadata metadata = new CommitMetadata();
    byte[] data = "testdata".getBytes();
    metadata.addDataWithOffsetAndLength(data, 4, 4);
    assertEquals(4, metadata.getBytes());
    assertEquals(CelebornCRC32.compute(data, 4, 4), metadata.getChecksum());
  }

  @Test
  public void testAddCommitData() {
    CommitMetadata metadata1 = new CommitMetadata();
    byte[] data1 = "test".getBytes();
    metadata1.addDataWithOffsetAndLength(data1, 0, data1.length);

    CommitMetadata metadata2 = new CommitMetadata();
    byte[] data2 = "data".getBytes();
    metadata2.addDataWithOffsetAndLength(data2, 0, data2.length);

    metadata1.addCommitData(metadata2);

    // Verify that the metadata1 now contains the combined data and checksum of data1 and data2
    assertEquals(data1.length + data2.length, metadata1.getBytes());
    assertEquals(
        CelebornCRC32.combine(CelebornCRC32.compute(data1), CelebornCRC32.compute(data2)),
        metadata1.getChecksum());
  }

  @Test
  public void testAddDataByteBuffer() {
    byte[] data = "testdata".getBytes();
    // The ByteBuffer overload (read path) must match the byte[] overload (write path).
    CommitMetadata fromBuffer = new CommitMetadata();
    fromBuffer.addData(ByteBuffer.wrap(data));
    CommitMetadata fromBytes = new CommitMetadata();
    fromBytes.addDataWithOffsetAndLength(data, 0, data.length);
    assertEquals(fromBytes.getBytes(), fromBuffer.getBytes());
    assertEquals(fromBytes.getChecksum(), fromBuffer.getChecksum());
  }

  @Test
  public void testAddDataTwoByteBuffers() {
    byte[] header = "test".getBytes();
    byte[] body = "data".getBytes();
    // A split header/data buffer must hash equal to its concatenation.
    CommitMetadata fromTwoBuffers = new CommitMetadata();
    fromTwoBuffers.addData(ByteBuffer.wrap(header), ByteBuffer.wrap(body));
    CommitMetadata fromConcatenation = new CommitMetadata();
    fromConcatenation.addData(ByteBuffer.wrap("testdata".getBytes()));
    assertEquals(fromConcatenation.getBytes(), fromTwoBuffers.getBytes());
    assertEquals(fromConcatenation.getChecksum(), fromTwoBuffers.getChecksum());
  }

  @Test
  public void testRangeCombineDetectsCorruption() {
    // Mirror the map-partition flow: per-subpartition write checksums combined vs read bytes
    // served.
    byte[][] perSubpartition = {
      "sub-0-bytes".getBytes(), "sub-1-bytes".getBytes(), "sub-2-bytes".getBytes()
    };
    int startSubIndex = 0;
    int endSubIndex = 1;

    CommitMetadata expected = new CommitMetadata();
    for (int i = startSubIndex; i <= endSubIndex; i++) {
      CommitMetadata writeSide = new CommitMetadata();
      writeSide.addDataWithOffsetAndLength(perSubpartition[i], 0, perSubpartition[i].length);
      expected.addCommitData(writeSide.getChecksum(), writeSide.getBytes());
    }

    // A faithful read over the same range matches.
    CommitMetadata cleanRead = new CommitMetadata();
    for (int i = startSubIndex; i <= endSubIndex; i++) {
      cleanRead.addData(ByteBuffer.wrap(perSubpartition[i]));
    }
    assertTrue(CommitMetadata.checkCommitMetadata(expected, cleanRead));

    // A single flipped byte on the read path is detected.
    byte[] corrupted = perSubpartition[endSubIndex].clone();
    corrupted[0] ^= 0x01;
    CommitMetadata corruptedRead = new CommitMetadata();
    corruptedRead.addData(ByteBuffer.wrap(perSubpartition[startSubIndex]));
    corruptedRead.addData(ByteBuffer.wrap(corrupted));
    assertFalse(CommitMetadata.checkCommitMetadata(expected, corruptedRead));
  }

  @Test
  public void testRangeCombineIsOrderIndependent() {
    // Tiered reads accumulate interleaved buffers while the driver combines per-subpartition
    // checksums in order; the combine must be order-independent for the two to match.
    byte[][] perSubpartition = {
      "sub-0-bytes".getBytes(), "sub-1-bytes".getBytes(), "sub-2-bytes".getBytes()
    };

    CommitMetadata expected = new CommitMetadata();
    for (byte[] payload : perSubpartition) {
      CommitMetadata writeSide = new CommitMetadata();
      writeSide.addDataWithOffsetAndLength(payload, 0, payload.length);
      expected.addCommitData(writeSide.getChecksum(), writeSide.getBytes());
    }

    // Read side accumulates the same bytes in reverse order; the result must be identical.
    CommitMetadata reversedRead = new CommitMetadata();
    for (int i = perSubpartition.length - 1; i >= 0; i--) {
      reversedRead.addData(ByteBuffer.wrap(perSubpartition[i]));
    }
    assertEquals(expected.getChecksum(), reversedRead.getChecksum());
    assertEquals(expected.getBytes(), reversedRead.getBytes());
    assertTrue(CommitMetadata.checkCommitMetadata(expected, reversedRead));
  }

  @Test
  public void testCheckCommitMetadata() {
    CommitMetadata expected = new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 8);
    CommitMetadata actualMatching =
        new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 8);
    CommitMetadata actualNonMatchingBytesOnly =
        new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 12);
    CommitMetadata actualNonMatchingChecksumOnly =
        new CommitMetadata(CelebornCRC32.compute("foo".getBytes()), 8);
    CommitMetadata actualNonMatching =
        new CommitMetadata(CelebornCRC32.compute("bar".getBytes()), 16);

    Assert.assertTrue(CommitMetadata.checkCommitMetadata(expected, actualMatching));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatchingBytesOnly));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatchingChecksumOnly));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatching));
  }
}
