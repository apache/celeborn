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

#include <memory>

#include "celeborn/protocol/CompressionCodec.h"

namespace celeborn {
namespace client {
namespace compress {

class Decompressor {
 public:
  virtual ~Decompressor() = default;

  virtual int decompress(const char* src, char* dst, int dst_off) = 0;

  virtual int getOriginalLen(const char* src) = 0;

  static std::unique_ptr<Decompressor> getDecompressor(
      protocol::CompressionCodec codec);

 protected:
  static int32_t readIntLE(const char* buf, int i) {
    const auto p = reinterpret_cast<const unsigned char*>(buf);
    return (p[i]) | (p[i + 1] << 8) | (p[i + 2] << 16) | (p[i + 3] << 24);
  }
};

} // namespace compress
} // namespace client
} // namespace celeborn
