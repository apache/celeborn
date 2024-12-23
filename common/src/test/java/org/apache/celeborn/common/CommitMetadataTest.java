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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.celeborn.common.util.JavaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
    assertEquals(CelebornCRC32.combine(CelebornCRC32.compute(data1), CelebornCRC32.compute(data2)), metadata1.getChecksum());
  }

  @Test
  public void testEncodeDecode() {
    CommitMetadata metadata = new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 8);
    ByteBuf buffer = Unpooled.buffer();
    metadata.encode(buffer);

    CommitMetadata decodedMetadata = CommitMetadata.decode(buffer);
    assertEquals(metadata.getChecksum(), decodedMetadata.getChecksum());
    assertEquals(metadata.getBytes(), decodedMetadata.getBytes());
  }


  @Test
  public void testCheckCommitMetadata() {
    CommitMetadata expected = new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 8);
    CommitMetadata actualMatching = new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 8);
    CommitMetadata actualNonMatchingBytesOnly = new CommitMetadata(CelebornCRC32.compute("testdata".getBytes()), 12);
    CommitMetadata actualNonMatchingChecksumOnly = new CommitMetadata(CelebornCRC32.compute("foo".getBytes()), 8);
    CommitMetadata actualNonMatching = new CommitMetadata(CelebornCRC32.compute("bar".getBytes()), 16);

    Assert.assertTrue(CommitMetadata.checkCommitMetadata(expected, actualMatching));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatchingBytesOnly));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatchingChecksumOnly));
    Assert.assertFalse(CommitMetadata.checkCommitMetadata(expected, actualNonMatching));
  }

  @Test
  public void testCheckMissingCommitMetadatas() {
    int startMapIndex = 0;
    int endMapIndex = 4;
    int[] attempts = {0, 0, 0, 0};
    int shuffleId = 1;

    Map<String, CommitMetadata> expectedCommitMetadataMap = new LinkedHashMap<>();
    expectedCommitMetadataMap.put("1-0-0", new CommitMetadata(222957957, 11));
    expectedCommitMetadataMap.put("1-1-0", new CommitMetadata(222957957, 11));
    expectedCommitMetadataMap.put("1-2-0", new CommitMetadata(222957957, 11));

    List<String> missingKeys = CommitMetadata.checkMissingCommitMetadatas(
        startMapIndex, endMapIndex, attempts, shuffleId, expectedCommitMetadataMap);

    Assert.assertEquals(1, missingKeys.size());
    Assert.assertTrue(missingKeys.contains("1-3-0"));
  }
}