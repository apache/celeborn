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

#include <zlib.h>
#include <zstd.h>

#include "celeborn/client/compress/ZstdCompressor.h"

namespace celeborn {
namespace client {
namespace compress {

ZstdCompressor::ZstdCompressor(const int compressionLevel)
    : compressionLevel_(compressionLevel) {}

size_t ZstdCompressor::compress(
    const uint8_t* src,
    const int srcOffset,
    const int srcLength,
    uint8_t* dst,
    const int dstOffset) {
  const auto srcPtr = src + srcOffset;
  const auto dstPtr = dst + dstOffset;
  const auto dstDataPtr = dstPtr + kHeaderLength;

  uLong check = crc32(0L, Z_NULL, 0);
  check = crc32(check, srcPtr, srcLength);

  std::copy_n(kMagic, kMagicLength, dstPtr);

  size_t compressedLength = ZSTD_compress(
      dstDataPtr,
      ZSTD_compressBound(srcLength),
      srcPtr,
      srcLength,
      compressionLevel_);

  int compressionMethod;
  if (ZSTD_isError(compressedLength) ||
      compressedLength >= static_cast<size_t>(srcLength)) {
    compressionMethod = kCompressionMethodRaw;
    compressedLength = srcLength;
    std::copy_n(srcPtr, srcLength, dstDataPtr);
  } else {
    compressionMethod = kCompressionMethodZstd;
  }

  dstPtr[kMagicLength] = static_cast<uint8_t>(compressionMethod);
  writeIntLE(compressedLength, dstPtr, kMagicLength + 1);
  writeIntLE(srcLength, dstPtr, kMagicLength + 5);
  writeIntLE(static_cast<int>(check), dstPtr, kMagicLength + 9);

  return kHeaderLength + compressedLength;
}

size_t ZstdCompressor::getDstCapacity(const int length) {
  return ZSTD_compressBound(length) + kHeaderLength;
}

} // namespace compress
} // namespace client
} // namespace celeborn
