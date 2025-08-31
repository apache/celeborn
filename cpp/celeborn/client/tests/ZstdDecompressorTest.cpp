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

#include "celeborn/client/compress/ZstdDecompressor.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::protocol;

TEST(ZstdDecompressorTest, DecompressWithZstd) {
  compress::ZstdDecompressor decompressor;

  std::vector<uint8_t> compressedData = {
      90,  83, 84,  68,  66,  108, 111, 99,  107, 48,  34,  0,   0,   0,
      39,  0,  0,   0,   56,  207, 204, 32,  40,  181, 47,  253, 32,  39,
      205, 0,  0,   136, 72,  101, 108, 108, 111, 111, 32,  67,  101, 108,
      101, 98, 111, 114, 110, 33,  33,  2,   0,   128, 251, 13,  20,  1,
  };

  const int originalLen = decompressor.getOriginalLen(compressedData.data());

  const auto decompressedData = new uint8_t[originalLen + 1];
  decompressedData[originalLen] = '\0';

  const bool success =
      decompressor.decompress(compressedData.data(), decompressedData, 0);

  EXPECT_TRUE(success);

  EXPECT_EQ(
      reinterpret_cast<char*>(decompressedData),
      std::string("Helloooooooooooo Celeborn!!!!!!!!!!!!!!"));
}

TEST(ZstdDecompressorTest, DecompressWithRaw) {
  compress::ZstdDecompressor decompressor;

  std::vector<uint8_t> compressedData = {
      90,  83,  84,  68,  66,  108, 111, 99,  107, 16,  15,  0,
      0,   0,   15,  0,   0,   0,   15,  118, 81,  228, 72,  101,
      108, 108, 111, 32,  67,  101, 108, 101, 98,  111, 114, 110,
      33,  67,  101, 108, 101, 98,  111, 114, 110, 33,  33,
  };

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
