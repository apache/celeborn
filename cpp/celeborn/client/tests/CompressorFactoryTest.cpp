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

  // Verify it's an LZ4 compressor
  EXPECT_GT(compressor->getDstCapacity(100), 0);
}

TEST(CompressorFactoryTest, CreateZstdCompressorFromConf) {
  CelebornConf conf;
  conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
  conf.registerProperty(
      CelebornConf::kShuffleCompressionZstdCompressLevel, "3");

  auto compressor = compress::Compressor::createCompressor(conf);
  ASSERT_NE(compressor, nullptr);

  // Verify it's a ZSTD compressor
  EXPECT_GT(compressor->getDstCapacity(100), 0);
}

TEST(CompressorFactoryTest, CompressionCodecNoneDisablesCompression) {
  CelebornConf conf;
  // Verify default is NONE
  EXPECT_EQ(conf.shuffleCompressionCodec(), CompressionCodec::NONE);
}

TEST(CompressorFactoryTest, ZstdCompressionLevelFromConf) {
  // Test that configuration correctly reads ZSTD compression levels
  for (int level = -5; level <= 10; level++) {
    CelebornConf conf;
    conf.registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
    conf.registerProperty(
        CelebornConf::kShuffleCompressionZstdCompressLevel,
        std::to_string(level));

    // Verify the compression level is set correctly
    EXPECT_EQ(conf.shuffleCompressionZstdCompressLevel(), level);

    // Verify the compressor is created correctly
    auto compressor = compress::Compressor::createCompressor(conf);
    ASSERT_NE(compressor, nullptr);
    EXPECT_GT(compressor->getDstCapacity(100), 0);
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

  // Verify compression succeeded with offset
  EXPECT_GT(compressedSize, 0);
  EXPECT_LE(compressedSize, maxLength);
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

  // Verify compression succeeded with offset
  EXPECT_GT(compressedSize, 0);
  EXPECT_LE(compressedSize, maxLength);
}
