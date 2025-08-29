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
#include "client/compress/ZstdDecompressor.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::protocol;

TEST(ZstdCompressorTest, CompressWithZstd) {
  for (int compressionLevel = -5; compressionLevel <= 22; compressionLevel++) {
    compress::ZstdCompressor compressor(compressionLevel);
    const std::string toCompressData =
        "Helloooooooooooo Celeborn!!!!!!!!!!!!!!";

    const auto maxLength = compressor.getDstCapacity(toCompressData.size());
    std::vector<uint8_t> compressedData(maxLength);
    compressor.compress(
        reinterpret_cast<const uint8_t*>(toCompressData.data()),
        0,
        toCompressData.size(),
        compressedData.data(),
        0);

    compress::ZstdDecompressor decompressor;
    const auto oriLength = decompressor.getOriginalLen(compressedData.data());
    std::vector<uint8_t> decompressedData(oriLength + 1);
    decompressedData[oriLength] = '\0';
    const bool success = decompressor.decompress(
        compressedData.data(), decompressedData.data(), 0);
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<char*>(decompressedData.data()), toCompressData);
  }
}

TEST(ZstdCompressorTest, CompressWithRaw) {
  for (int compressionLevel = -5; compressionLevel <= 22; compressionLevel++) {
    compress::ZstdCompressor compressor(compressionLevel);
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
    const auto oriLength = decompressor.getOriginalLen(compressedData.data());
    std::vector<uint8_t> decompressedData(oriLength + 1);
    decompressedData[oriLength] = '\0';
    const bool success = decompressor.decompress(
        compressedData.data(), decompressedData.data(), 0);
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<char*>(decompressedData.data()), toCompressData);
  }
}
