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

#include "celeborn/client/compress/Compressor.h"
#include "celeborn/client/compress/Decompressor.h"
#include "celeborn/conf/CelebornConf.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::conf;
using namespace celeborn::protocol;

TEST(CompressorFactoryTest, CreateLz4CompressorFromConf) {
  CelebornConf conf;
  conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "LZ4");

  auto compressor = compress::Compressor::createCompressor(conf);
  ASSERT_NE(compressor, nullptr);

  const std::string testData = "Test data for compression";
  const size_t maxLength = compressor->getDstCapacity(testData.size());
  std::vector<uint8_t> compressedData(maxLength);

  const size_t compressedSize = compressor->compress(
      reinterpret_cast<const uint8_t*>(testData.data()),
      0,
      testData.size(),
      compressedData.data(),
      0);

  ASSERT_GT(compressedSize, 8);
  EXPECT_EQ(compressedData[0], 'L');
  EXPECT_EQ(compressedData[1], 'Z');
  EXPECT_EQ(compressedData[2], '4');
  EXPECT_EQ(compressedData[3], 'B');
  EXPECT_EQ(compressedData[4], 'l');
  EXPECT_EQ(compressedData[5], 'o');
  EXPECT_EQ(compressedData[6], 'c');
  EXPECT_EQ(compressedData[7], 'k');
}

TEST(CompressorFactoryTest, CreateZstdCompressorFromConf) {
  CelebornConf conf;
  conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
  conf.registerProperty(
      CelebornConf::kShuffleCompressionZstdCompressLevel, "3");

  auto compressor = compress::Compressor::createCompressor(conf);
  ASSERT_NE(compressor, nullptr);

  const std::string testData = "Test data for compression";
  const size_t maxLength = compressor->getDstCapacity(testData.size());
  std::vector<uint8_t> compressedData(maxLength);

  const size_t compressedSize = compressor->compress(
      reinterpret_cast<const uint8_t*>(testData.data()),
      0,
      testData.size(),
      compressedData.data(),
      0);

  ASSERT_GT(compressedSize, 9);
  EXPECT_EQ(compressedData[0], 'Z');
  EXPECT_EQ(compressedData[1], 'S');
  EXPECT_EQ(compressedData[2], 'T');
  EXPECT_EQ(compressedData[3], 'D');
  EXPECT_EQ(compressedData[4], 'B');
  EXPECT_EQ(compressedData[5], 'l');
  EXPECT_EQ(compressedData[6], 'o');
  EXPECT_EQ(compressedData[7], 'c');
  EXPECT_EQ(compressedData[8], 'k');
}

TEST(CompressorFactoryTest, CompressionCodecNoneDisablesCompression) {
  CelebornConf conf;
  // Verify default is NONE
  EXPECT_EQ(conf.shuffleCompressionCodec(), CompressionCodec::NONE);
}

TEST(CompressorFactoryTest, ZstdCompressionLevelFromConf) {
  // Test that configuration correctly reads ZSTD compression levels
  const std::string testData = "Test data for compression";

  for (int level = -5; level <= 10; level++) {
    CelebornConf conf;
    conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
    conf.registerProperty(
        CelebornConf::kShuffleCompressionZstdCompressLevel,
        std::to_string(level));

    // Verify the compression level is set correctly
    EXPECT_EQ(conf.shuffleCompressionZstdCompressLevel(), level);

    // Verify the compressor is created correctly and produces ZSTD output
    auto compressor = compress::Compressor::createCompressor(conf);
    ASSERT_NE(compressor, nullptr);

    const size_t maxLength = compressor->getDstCapacity(testData.size());
    std::vector<uint8_t> compressedData(maxLength);

    const size_t compressedSize = compressor->compress(
        reinterpret_cast<const uint8_t*>(testData.data()),
        0,
        testData.size(),
        compressedData.data(),
        0);

    ASSERT_GT(compressedSize, 9);
    EXPECT_EQ(compressedData[0], 'Z');
    EXPECT_EQ(compressedData[1], 'S');
    EXPECT_EQ(compressedData[2], 'T');
    EXPECT_EQ(compressedData[3], 'D');
  }
}

TEST(CompressorFactoryTest, CompressWithOffsetLz4) {
  CelebornConf conf;
  conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "LZ4");

  auto compressor = compress::Compressor::createCompressor(conf);
  ASSERT_NE(compressor, nullptr);

  const std::string prefix = "SKIP_THIS_PREFIX";
  const std::string testData =
      "Celeborn compression offset test with structured data: "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4 "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4 "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4";
  std::string fullData = prefix + testData;

  const auto maxLength = compressor->getDstCapacity(testData.size());
  std::vector<uint8_t> compressedData(maxLength);

  // Compress with offset (simulating pushData usage pattern)
  const size_t compressedSize = compressor->compress(
      reinterpret_cast<const uint8_t*>(fullData.data()),
      prefix.size(),
      testData.size(),
      compressedData.data(),
      0);

  ASSERT_GT(compressedSize, 0);
  ASSERT_LE(compressedSize, maxLength);

  auto decompressor =
      compress::Decompressor::createDecompressor(CompressionCodec::LZ4);
  ASSERT_NE(decompressor, nullptr);

  const int originalLen = decompressor->getOriginalLen(compressedData.data());
  EXPECT_EQ(originalLen, testData.size());

  std::vector<uint8_t> decompressedData(originalLen);
  const int decompressedSize = decompressor->decompress(
      compressedData.data(), decompressedData.data(), 0);
  EXPECT_EQ(decompressedSize, originalLen);

  const std::string decompressedStr(
      reinterpret_cast<const char*>(decompressedData.data()), decompressedSize);
  EXPECT_EQ(decompressedStr, testData);
  EXPECT_NE(decompressedStr, fullData);
}

TEST(CompressorFactoryTest, CompressWithOffsetZstd) {
  CelebornConf conf;
  conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
  conf.registerProperty(
      CelebornConf::kShuffleCompressionZstdCompressLevel, "3");

  auto compressor = compress::Compressor::createCompressor(conf);
  ASSERT_NE(compressor, nullptr);

  const std::string prefix = "SKIP_THIS_PREFIX";
  const std::string testData =
      "Celeborn compression offset test with structured data: "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4 "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4 "
      "partition_0:shuffle_1:map_2:attempt_0:batch_3:data_block_4";
  std::string fullData = prefix + testData;

  const auto maxLength = compressor->getDstCapacity(testData.size());
  std::vector<uint8_t> compressedData(maxLength);

  // Compress with offset (simulating pushData usage pattern)
  const size_t compressedSize = compressor->compress(
      reinterpret_cast<const uint8_t*>(fullData.data()),
      prefix.size(),
      testData.size(),
      compressedData.data(),
      0);

  ASSERT_GT(compressedSize, 0);
  ASSERT_LE(compressedSize, maxLength);

  auto decompressor =
      compress::Decompressor::createDecompressor(CompressionCodec::ZSTD);
  ASSERT_NE(decompressor, nullptr);

  const int originalLen = decompressor->getOriginalLen(compressedData.data());
  EXPECT_EQ(originalLen, testData.size());

  std::vector<uint8_t> decompressedData(originalLen);
  const int decompressedSize = decompressor->decompress(
      compressedData.data(), decompressedData.data(), 0);
  EXPECT_EQ(decompressedSize, originalLen);

  const std::string decompressedStr(
      reinterpret_cast<const char*>(decompressedData.data()), decompressedSize);
  EXPECT_EQ(decompressedStr, testData);
  EXPECT_NE(decompressedStr, fullData);
}
