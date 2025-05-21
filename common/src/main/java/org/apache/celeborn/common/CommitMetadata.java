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

import io.netty.buffer.ByteBuf;

public class CommitMetadata {

  private AtomicLong bytes = new AtomicLong();
  private CelebornCRC32 crc = new CelebornCRC32();

  public CommitMetadata() {}

  public CommitMetadata(long checksum, long numBytes) {
    this.bytes = new AtomicLong(numBytes);
    this.crc = new CelebornCRC32((int) checksum);
  }

  void addBytes(long bytes) {
    while (true) {
      long val = this.bytes.get();
      long newVal = val + bytes;
      if (this.bytes.compareAndSet(val, newVal)) {
        break;
      }
    }
  }

  public void addDataWithOffsetAndLength(byte[] rawDataBuf, int offset, int length) {
    addBytes(length);
    this.crc.addData(rawDataBuf, offset, length);
  }

  public void addCommitData(CommitMetadata commitMetadata) {
    addBytes(commitMetadata.bytes.longValue());
    this.crc.addChecksum(commitMetadata.getChecksum());
  }

  public int getChecksum() {
    return crc.get();
  }

  public long getBytes() {
    return bytes.get();
  }

  public void encode(ByteBuf buf) {
    buf.writeLong(this.getChecksum());
    buf.writeLong(this.bytes.get());
  }

  public static CommitMetadata decode(ByteBuf buf) {
    long checksum = buf.readLong();
    long numBytes = buf.readLong();
    return new CommitMetadata(checksum, numBytes);
  }

  public static boolean checkCommitMetadata(CommitMetadata expected, CommitMetadata actual) {
    boolean bytesMatch = expected.getBytes() == actual.getBytes();
    boolean checksumsMatch = expected.getChecksum() == actual.getChecksum();
    return bytesMatch && checksumsMatch;
  }

  public static CommitMetadata combineMetadata(
      CommitMetadata commitMetadata1, CommitMetadata commitMetadata2) {
    CommitMetadata commitMetadata =
        new CommitMetadata(commitMetadata1.getChecksum(), commitMetadata1.getBytes());
    commitMetadata.addCommitData(commitMetadata2);
    return commitMetadata;
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
