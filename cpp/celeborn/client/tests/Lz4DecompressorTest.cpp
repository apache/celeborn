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

#include "celeborn/client/compress/Lz4Decompressor.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::protocol;

TEST(Lz4DecompressorTest, DecompressWithLz4) {
  compress::Lz4Decompressor decompressor;

  std::vector<uint8_t> compressedData = {
      76,  90, 52, 66,  108, 111, 99,  107, 32,  29,  0,   0,   0,
      31,  0,  0,  0,   116, 18,  177, 8,   83,  72,  101, 108, 108,
      111, 1,  0,  240, 4,   32,  67,  101, 108, 101, 98,  111, 114,
      110, 33, 33, 33,  33,  33,  33,  33,  33,  33,  33};

  const int originalLen = decompressor.getOriginalLen(compressedData.data());

  const auto decompressedData = new uint8_t[originalLen + 1];
  decompressedData[originalLen] = '\0';

  const bool success =
      decompressor.decompress(compressedData.data(), decompressedData, 0);

  EXPECT_TRUE(success);

  EXPECT_EQ(
      reinterpret_cast<char*>(decompressedData),
      std::string("Helloooooooo Celeborn!!!!!!!!!!"));
}

TEST(Lz4DecompressorTest, DecompressWithRaw) {
  compress::Lz4Decompressor decompressor;

  std::vector<uint8_t> compressedData = {
      76, 90, 52,  66,  108, 111, 99,  107, 16,  15,  0,   0,   0,
      15, 0,  0,   0,   188, 66,  58,  13,  72,  101, 108, 108, 111,
      32, 67, 101, 108, 101, 98,  111, 114, 110, 33,  110, 33};

  const int originalLen = decompressor.getOriginalLen(compressedData.data());

  const auto decompressedData = new uint8_t[originalLen + 1];
  decompressedData[originalLen] = '\0';

  const bool success =
      decompressor.decompress(compressedData.data(), decompressedData, 0);

  EXPECT_TRUE(success);

  EXPECT_EQ(
      reinterpret_cast<char*>(decompressedData),
      std::string("Hello Celeborn!"));
}
