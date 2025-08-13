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

namespace celeborn {
namespace client {
namespace compress {

struct Lz4Trait {
  static constexpr char kMagic[] = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
  static constexpr int kMagicLength = sizeof(kMagic);

  static constexpr int kHeaderLength = kMagicLength + 1 + 4 + 4 + 4;

  static constexpr int kCompressionMethodRaw = 0x10;
  static constexpr int kCompressionMethodLZ4 = 0x20;

  static constexpr int kDefaultSeed = 0x9747b28c;
};

} // namespace compress
} // namespace client
} // namespace celeborn
