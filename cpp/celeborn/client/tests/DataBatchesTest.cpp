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

#include "celeborn/client/writer/DataBatches.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
std::shared_ptr<const protocol::PartitionLocation> makeLocation(int id) {
  auto loc = std::make_shared<protocol::PartitionLocation>();
  loc->id = id;
  loc->epoch = 0;
  loc->host = "host1";
  loc->pushPort = 9092;
  loc->rpcPort = 9091;
  loc->fetchPort = 9093;
  loc->replicatePort = 9094;
  loc->mode = protocol::PartitionLocation::PRIMARY;
  return loc;
}

std::unique_ptr<memory::ReadOnlyByteBuffer> makeBody(int size) {
  auto buf = memory::ByteBuffer::createWriteOnly(size);
  for (int i = 0; i < size; i++) {
    buf->write<uint8_t>(static_cast<uint8_t>(i & 0xFF));
  }
  return memory::ByteBuffer::toReadOnly(std::move(buf));
}
} // namespace

TEST(DataBatchesTest, addAndGetTotalSize) {
  DataBatches batches;
  EXPECT_EQ(batches.getTotalSize(), 0);

  batches.addDataBatch(makeLocation(0), 1, makeBody(100));
  EXPECT_EQ(batches.getTotalSize(), 100);

  batches.addDataBatch(makeLocation(1), 2, makeBody(200));
  EXPECT_EQ(batches.getTotalSize(), 300);
}

TEST(DataBatchesTest, requireAllBatches) {
  DataBatches batches;
  batches.addDataBatch(makeLocation(0), 1, makeBody(50));
  batches.addDataBatch(makeLocation(1), 2, makeBody(60));
  batches.addDataBatch(makeLocation(2), 3, makeBody(70));

  auto all = batches.requireBatches();
  EXPECT_EQ(all.size(), 3);
  EXPECT_EQ(all[0].batchId, 1);
  EXPECT_EQ(all[1].batchId, 2);
  EXPECT_EQ(all[2].batchId, 3);
  EXPECT_EQ(batches.getTotalSize(), 0);
}

TEST(DataBatchesTest, requireBatchesWithSize) {
  DataBatches batches;
  batches.addDataBatch(makeLocation(0), 1, makeBody(100));
  batches.addDataBatch(makeLocation(1), 2, makeBody(100));
  batches.addDataBatch(makeLocation(2), 3, makeBody(100));

  auto partial = batches.requireBatches(150);
  EXPECT_EQ(partial.size(), 2);
  EXPECT_EQ(partial[0].batchId, 1);
  EXPECT_EQ(partial[1].batchId, 2);
  EXPECT_EQ(batches.getTotalSize(), 100);

  auto rest = batches.requireBatches(200);
  EXPECT_EQ(rest.size(), 1);
  EXPECT_EQ(rest[0].batchId, 3);
  EXPECT_EQ(batches.getTotalSize(), 0);
}

TEST(DataBatchesTest, requireBatchesLargerThanTotal) {
  DataBatches batches;
  batches.addDataBatch(makeLocation(0), 1, makeBody(50));

  auto all = batches.requireBatches(1000);
  EXPECT_EQ(all.size(), 1);
  EXPECT_EQ(batches.getTotalSize(), 0);
}

TEST(DataBatchesTest, emptyBatches) {
  DataBatches batches;
  auto all = batches.requireBatches();
  EXPECT_EQ(all.size(), 0);

  auto partial = batches.requireBatches(100);
  EXPECT_EQ(partial.size(), 0);
}
