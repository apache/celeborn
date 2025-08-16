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

#include <lz4.h>
#include <cstring>

#include "celeborn/client/compress/Lz4Compressor.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {

Lz4Compressor::Lz4Compressor() {
  xxhashState_ = XXH32_createState();
  if (!xxhashState_) {
    CELEBORN_FAIL("Failed to create XXH32 state.")
  }
  XXH32_reset(xxhashState_, kDefaultSeed);
}

Lz4Compressor::~Lz4Compressor() {
  if (xxhashState_) {
    XXH32_freeState(xxhashState_);
  }
}

size_t Lz4Compressor::compress(
    const uint8_t* src,
    const int srcOffset,
    const int srcLength,
    uint8_t* dst,
    const int dstOffset) {
  const auto srcPtr = reinterpret_cast<const char*>(src + srcOffset);
  const auto dstPtr = dst + dstOffset;
  const auto dstDataPtr = reinterpret_cast<char*>(dstPtr + kHeaderLength);

  XXH32_reset(xxhashState_, kDefaultSeed);
  XXH32_update(xxhashState_, srcPtr, srcLength);
  const uint32_t check = XXH32_digest(xxhashState_) & 0xFFFFFFFL;

  std::copy_n(kMagic, kMagicLength, dstPtr);

  int compressedLength = LZ4_compress_default(
      srcPtr, dstDataPtr, srcLength, LZ4_compressBound(srcLength));

  int compressionMethod;
  if (compressedLength <= 0 || compressedLength >= srcLength) {
    compressionMethod = kCompressionMethodRaw;
    compressedLength = srcLength;
    std::copy_n(srcPtr, srcLength, dstDataPtr);
  } else {
    compressionMethod = kCompressionMethodLZ4;
  }

  dstPtr[kMagicLength] = static_cast<uint8_t>(compressionMethod);
  writeIntLE(compressedLength, dstPtr, kMagicLength + 1);
  writeIntLE(srcLength, dstPtr, kMagicLength + 5);
  writeIntLE(static_cast<int>(check), dstPtr, kMagicLength + 9);

  return kHeaderLength + compressedLength;
}

size_t Lz4Compressor::getDstCapacity(const int length) {
  return LZ4_compressBound(length) + kHeaderLength;
}

} // namespace compress
} // namespace client
} // namespace celeborn
