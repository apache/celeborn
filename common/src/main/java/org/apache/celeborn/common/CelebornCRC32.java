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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class CelebornCRC32 {

  private AtomicInteger current;

  CelebornCRC32(int i) {
    this.current = new AtomicInteger(i);
  }

  CelebornCRC32() {
    this(0);
  }

  static int compute(byte[] bytes) {
    CRC32 hashFunction = new CRC32();
    hashFunction.update(bytes);
    return (int) hashFunction.getValue();
  }

  static int compute(byte[] bytes, int offset, int length) {
    CRC32 hashFunction = new CRC32();
    hashFunction.update(bytes, offset, length);
    return (int) hashFunction.getValue();
  }

  static int combine(int first, int second) {
    first =
        (((byte) second + (byte) first) & 0xFF)
            | ((((byte) (second >> 8) + (byte) (first >> 8)) & 0xFF) << 8)
            | ((((byte) (second >> 16) + (byte) (first >> 16)) & 0xFF) << 16)
            | (((byte) (second >> 24) + (byte) (first >> 24)) << 24);
    return first;
  }

  public void addChecksum(int checksum) {
    while (true) {
      int val = current.get();
      int newVal = combine(checksum, val);
      if (current.compareAndSet(val, newVal)) {
        break;
      }
    }
  }

  void addData(byte[] bytes, int offset, int length) {
    addChecksum(compute(bytes, offset, length));
  }

  int get() {
    return current.get();
  }

  @Override
  public String toString() {
    return "CelebornCRC32{" + "current=" + current + '}';
  }
}
