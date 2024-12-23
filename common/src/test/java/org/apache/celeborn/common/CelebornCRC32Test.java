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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

// Test data is generated from https://crccalc.com/ using "CRC-32/ISO-HDLC".
public class CelebornCRC32Test {

  @Test
  public void testCompute() {
    byte[] data = "test".getBytes();
    int checksum = CelebornCRC32.compute(data);
    assertEquals(3632233996L, checksum & 0xFFFFFFFFL);
  }

  @Test
  public void testComputeWithOffset() {
    byte[] data = "testdata".getBytes();
    int checksum = CelebornCRC32.compute(data, 4, 4);
    assertEquals(2918445923L, checksum & 0xFFFFFFFFL);
  }

  @Test
  public void testCombine() {
    int first = 123456789;
    int second = 987654321;
    int combined = CelebornCRC32.combine(first, second);
    assertNotEquals(first, combined);
    assertNotEquals(second, combined);
  }

  @Test
  public void testAddChecksum() {
    CelebornCRC32 crc = new CelebornCRC32();
    crc.addChecksum(123456789);
    assertEquals(123456789, crc.get());
  }

  @Test
  public void testAddDataWithOffset() {
    CelebornCRC32 crc = new CelebornCRC32();
    byte[] data = "testdata".getBytes();
    crc.addData(data, 4, 4);
    assertEquals(2918445923L, crc.get() & 0xFFFFFFFFL);
  }

  @Test
  public void testToString() {
    CelebornCRC32 crc = new CelebornCRC32(123456789);
    assertEquals("CelebornCRC32{current=123456789}", crc.toString());
  }
}