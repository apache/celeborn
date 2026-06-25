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

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "celeborn/client/reader/CelebornInputStream.h"

using namespace celeborn;
using namespace celeborn::client;

// empty() returns a no-op stream: read() reports EOF (-1) immediately and stays
// at EOF across repeated and larger reads.
TEST(CelebornInputStreamTest, emptyStreamReadsReturnEof) {
  auto stream = CelebornInputStream::empty();
  ASSERT_NE(stream, nullptr);

  std::vector<uint8_t> buffer(16, 0);
  EXPECT_EQ(stream->read(buffer.data(), 0, buffer.size()), -1);
  // No state to advance: a second read still reports EOF.
  EXPECT_EQ(stream->read(buffer.data(), 0, buffer.size()), -1);
  // A zero-length read is a no-op that returns 0, not EOF.
  EXPECT_EQ(stream->read(buffer.data(), 0, 0), 0);
}
