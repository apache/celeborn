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

#include <xxhash.h>

#include "celeborn/client/compress/Compressor.h"
#include "celeborn/client/compress/Lz4Trait.h"

namespace celeborn {
namespace client {
namespace compress {

class Lz4Compressor final : public Compressor, Lz4Trait {
 public:
  explicit Lz4Compressor();
  ~Lz4Compressor() override;

  size_t compress(
      const uint8_t* src,
      int srcOffset,
      int srcLength,
      uint8_t* dst,
      int dstOffset) override;

  size_t getDstCapacity(int length) override;

  Lz4Compressor(const Lz4Compressor&) = delete;
  Lz4Compressor& operator=(const Lz4Compressor&) = delete;

 private:
  XXH32_state_t* xxhashState_;
};

} // namespace compress
} // namespace client
} // namespace celeborn
