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

#include "celeborn/client/compress/Lz4Decompressor.h"
#include <lz4.h>
#include <cstring>
#include <iostream>
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {
Lz4Decompressor::Lz4Decompressor() {
  xxhash_state_ = XXH32_createState();
  if (!xxhash_state_) {
    CELEBORN_FAIL("Failed to create XXH32 state.")
  }
  XXH32_reset(xxhash_state_, kDefaultSeed);
}

Lz4Decompressor::~Lz4Decompressor() {
  if (xxhash_state_) {
    XXH32_freeState(xxhash_state_);
  }
}

int Lz4Decompressor::getOriginalLen(const uint8_t* src) {
  return readIntLE(src, kMagicLength + 5);
}

int Lz4Decompressor::decompress(
    const uint8_t* src,
    uint8_t* dst,
    const int dstOff) {
  const int compressionMethod = static_cast<unsigned char>(src[kMagicLength]);
  const int compressedLen = readIntLE(src, kMagicLength + 1);
  const int originalLen = readIntLE(src, kMagicLength + 5);
  const int expectedCheck = readIntLE(src, kMagicLength + 9);

  const uint8_t* compressedDataPtr = src + kHeaderLength;
  uint8_t* dstPtr = dst + dstOff;

  switch (compressionMethod) {
    case kCompressionMethodRaw:
      std::memcpy(dstPtr, compressedDataPtr, originalLen);
      break;
    case kCompressionMethodLZ4: {
      const int decompressedBytes = LZ4_decompress_safe(
          reinterpret_cast<const char*>(compressedDataPtr),
          reinterpret_cast<char*>(dstPtr),
          compressedLen,
          originalLen);

      if (decompressedBytes != originalLen) {
        CELEBORN_FAIL(
            std::string("Decompression failed! LZ4 error or size mismatch. ") +
            "Expected: " + std::to_string(originalLen) +
            ", Got: " + std::to_string(decompressedBytes));
      }
      break;
    }
    default:
      CELEBORN_FAIL(
          std::string("Unsupported compression method: ") +
          std::to_string(compressionMethod));
  }

  XXH32_reset(xxhash_state_, kDefaultSeed);
  XXH32_update(xxhash_state_, dstPtr, originalLen);
  const uint32_t actualCheck = XXH32_digest(xxhash_state_) & 0xFFFFFFFL;

  if (static_cast<uint32_t>(expectedCheck) != actualCheck) {
    CELEBORN_FAIL(
        std::string("Checksum mismatch! Expected: ") +
        std::to_string(expectedCheck) +
        ", Actual: " + std::to_string(actualCheck));
  }

  return originalLen;
}
} // namespace compress
} // namespace client
} // namespace celeborn
