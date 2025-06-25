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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CommitMetadata {

  private final AtomicLong bytes;
  private final CelebornCRC32 crc;

  public CommitMetadata() {
    this.bytes = new AtomicLong();
    this.crc = new CelebornCRC32();
  }

  public CommitMetadata(int checksum, long numBytes) {
    this.bytes = new AtomicLong(numBytes);
    this.crc = new CelebornCRC32(checksum);
  }

  public void addDataWithOffsetAndLength(byte[] rawDataBuf, int offset, int length) {
    this.bytes.addAndGet(length);
    this.crc.addData(rawDataBuf, offset, length);
  }

  public void addCommitData(CommitMetadata commitMetadata) {
    addCommitData(commitMetadata.getChecksum(), commitMetadata.getBytes());
  }

  public void addCommitData(int checksum, long numBytes) {
    this.bytes.addAndGet(numBytes);
    this.crc.addChecksum(checksum);
  }

  public int getChecksum() {
    return crc.get();
  }

  public long getBytes() {
    return bytes.get();
  }

  public static boolean checkCommitMetadata(CommitMetadata expected, CommitMetadata actual) {
    boolean bytesMatch = expected.getBytes() == actual.getBytes();
    boolean checksumsMatch = expected.getChecksum() == actual.getChecksum();
    return bytesMatch && checksumsMatch;
  }

  @Override
  public String toString() {
    return "CommitMetadata{" + "bytes=" + bytes.get() + ", crc=" + crc + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    CommitMetadata that = (CommitMetadata) o;
    return bytes.get() == that.bytes.get() && Objects.equals(crc, that.crc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bytes, crc);
  }
}
