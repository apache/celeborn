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

package com.aliyun.emr.rss.client.compress;

import java.util.zip.Checksum;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

public class RssLz4Compressor extends RssLz4Trait {
  private final int compressionLevel;
  private final LZ4Compressor compressor;
  private final Checksum checksum;
  private byte[] compressedBuffer;
  private int compressedTotalSize;

  public RssLz4Compressor() {
    this(256 * 1024);
  }

  public RssLz4Compressor(int blockSize) {
    int level = 32 - Integer.numberOfLeadingZeros(blockSize - 1) - COMPRESSION_LEVEL_BASE;
    this.compressionLevel = Math.max(0, level);
    this.compressor = LZ4Factory.fastestInstance().fastCompressor();
    checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
    initCompressBuffer(blockSize);
  }

  private void initCompressBuffer(int size) {
    int compressedBlockSize = HEADER_LENGTH + compressor.maxCompressedLength(size);
    compressedBuffer = new byte[compressedBlockSize];
    System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
  }

  public void compress(byte[] data, int offset, int length) {
    checksum.reset();
    checksum.update(data, offset, length);
    final int check = (int) checksum.getValue();
    if (compressedBuffer.length - HEADER_LENGTH < length) {
      initCompressBuffer(length);
    }
    int compressedLength = compressor.compress(
        data, offset, length, compressedBuffer, HEADER_LENGTH);
    final int compressMethod;
    if (compressedLength >= length) {
      compressMethod = COMPRESSION_METHOD_RAW;
      compressedLength = length;
      System.arraycopy(data, offset, compressedBuffer, HEADER_LENGTH, length);
    } else {
      compressMethod = COMPRESSION_METHOD_LZ4;
    }

    compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
    writeIntLE(compressedLength, compressedBuffer, MAGIC_LENGTH + 1);
    writeIntLE(length, compressedBuffer, MAGIC_LENGTH + 5);
    writeIntLE(check, compressedBuffer, MAGIC_LENGTH + 9);

    compressedTotalSize = HEADER_LENGTH + compressedLength;
  }

  public int getCompressedTotalSize() {
    return compressedTotalSize;
  }

  public byte[] getCompressedBuffer() {
    return compressedBuffer;
  }

  private static void writeIntLE(int i, byte[] buf, int off) {
    buf[off++] = (byte) i;
    buf[off++] = (byte) (i >>> 8);
    buf[off++] = (byte) (i >>> 16);
    buf[off++] = (byte) (i >>> 24);
  }
}
