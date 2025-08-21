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

#include <gtest/gtest.h>

#include "celeborn/client/compress/ZstdCompressor.h"
#include "celeborn/client/compress/ZstdDecompressor.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::protocol;

TEST(ZstdCompressorTest, CompressWithZstd) {
  compress::ZstdCompressor compressor;
  const std::string toCompressData = "Helloooooooooooo Celeborn!!!!!!!!!!!!!!";

  const auto maxLength = compressor.getDstCapacity(toCompressData.size());
  std::vector<uint8_t> compressedData(maxLength);
  compressor.compress(
      reinterpret_cast<const uint8_t*>(toCompressData.data()),
      0,
      toCompressData.size(),
      compressedData.data(),
      0);

  compress::ZstdDecompressor decompressor;

  const int originalLen = decompressor.getOriginalLen(compressedData.data());

  std::vector<uint8_t> decompressedData(originalLen + 1);
  decompressedData[originalLen] = '\0';

  const int decompressedLen = decompressor.decompress(
      compressedData.data(), decompressedData.data(), 0);

  EXPECT_GT(decompressedLen, 0);
  EXPECT_EQ(reinterpret_cast<char*>(decompressedData.data()), toCompressData);
}

TEST(ZstdCompressorTest, CompressWithRaw) {
  compress::ZstdCompressor compressor(-5); // Very low compression level may result in raw data
  const std::string toCompressData = "Hello Celeborn!";

  const auto maxLength = compressor.getDstCapacity(toCompressData.size());
  std::vector<uint8_t> compressedData(maxLength);
  compressor.compress(
      reinterpret_cast<const uint8_t*>(toCompressData.data()),
      0,
      toCompressData.size(),
      compressedData.data(),
      0);

  compress::ZstdDecompressor decompressor;

  const int originalLen = decompressor.getOriginalLen(compressedData.data());

  std::vector<uint8_t> decompressedData(originalLen + 1);
  decompressedData[originalLen] = '\0';

  const int decompressedLen = decompressor.decompress(
      compressedData.data(), decompressedData.data(), 0);

  EXPECT_GT(decompressedLen, 0);
  EXPECT_EQ(reinterpret_cast<char*>(decompressedData.data()), toCompressData);
}

TEST(ZstdCompressorTest, CompressWithDifferentLevels) {
  const std::string toCompressData = "This is a test string for checking different compression levels in ZSTD.";

  for (int level = -5; level <= 5; level += 5) {
    compress::ZstdCompressor compressor(level);

    const auto maxLength = compressor.getDstCapacity(toCompressData.size());
    std::vector<uint8_t> compressedData(maxLength);
    compressor.compress(
        reinterpret_cast<const uint8_t*>(toCompressData.data()),
        0,
        toCompressData.size(),
        compressedData.data(),
        0);

    compress::ZstdDecompressor decompressor;

    const int originalLen = decompressor.getOriginalLen(compressedData.data());

    std::vector<uint8_t> decompressedData(originalLen + 1);
    decompressedData[originalLen] = '\0';

    const int decompressedLen = decompressor.decompress(
        compressedData.data(), decompressedData.data(), 0);

    EXPECT_GT(decompressedLen, 0);
    EXPECT_EQ(reinterpret_cast<char*>(decompressedData.data()), toCompressData);
  }
}
