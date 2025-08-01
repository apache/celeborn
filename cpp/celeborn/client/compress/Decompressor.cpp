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

#include <stdexcept>

#include "celeborn/client/compress/Lz4Decompressor.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {

std::unique_ptr<Decompressor> Decompressor::createDecompressor(
    protocol::CompressionCodec codec) {
  switch (codec) {
    case protocol::CompressionCodec::LZ4:
      return std::make_unique<Lz4Decompressor>();
    case protocol::CompressionCodec::ZSTD:
      // TODO: impl zstd
      CELEBORN_FAIL("Compression codec ZSTD is not supported.");
    default:
      CELEBORN_FAIL("Unknown compression codec.");
  }
}

} // namespace compress
} // namespace client
} // namespace celeborn
