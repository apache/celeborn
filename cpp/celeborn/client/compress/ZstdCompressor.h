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

#pragma once

#include "celeborn/client/compress/Compressor.h"
#include "celeborn/client/compress/ZstdTrait.h"

namespace celeborn {
namespace client {
namespace compress {

class ZstdCompressor final : public Compressor, ZstdTrait {
 public:
  explicit ZstdCompressor(int compressionLevel);
  ~ZstdCompressor() override = default;

  size_t compress(
      const uint8_t* src,
      int srcOffset,
      int srcLength,
      uint8_t* dst,
      int dstOffset) override;

  size_t getDstCapacity(int length) override;

  ZstdCompressor(const ZstdCompressor&) = delete;
  ZstdCompressor& operator=(const ZstdCompressor&) = delete;

 private:
  const int compressionLevel_;
};

} // namespace compress
} // namespace client
} // namespace celeborn
