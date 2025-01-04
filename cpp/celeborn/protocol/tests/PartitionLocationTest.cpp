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

#include "celeborn/proto/TransportMessagesCpp.pb.h"
#include "celeborn/protocol/PartitionLocation.h"

using namespace celeborn::protocol;

std::unique_ptr<PbStorageInfo> generateStorageInfoPb() {
  auto pbStorageInfo = std::make_unique<PbStorageInfo>();
  pbStorageInfo->set_type(1);
  pbStorageInfo->set_mountpoint("test_mountpoint");
  pbStorageInfo->set_finalresult(true);
  pbStorageInfo->set_filepath("test_filepath");
  pbStorageInfo->set_availablestoragetypes(1);
  return std::move(pbStorageInfo);
}

void verifyStorageInfo(const StorageInfo* storageInfo) {
  EXPECT_EQ(storageInfo->type, 1);
  EXPECT_EQ(storageInfo->mountPoint, "test_mountpoint");
  EXPECT_EQ(storageInfo->finalResult, true);
  EXPECT_EQ(storageInfo->filePath, "test_filepath");
  EXPECT_EQ(storageInfo->availableStorageTypes, 1);
}

std::unique_ptr<PbPartitionLocation> generateBasicPartitionLocationPb() {
  auto pbPartitionLocation = std::make_unique<PbPartitionLocation>();
  pbPartitionLocation->set_id(1);
  pbPartitionLocation->set_epoch(101);
  pbPartitionLocation->set_host("test_host");
  pbPartitionLocation->set_rpcport(1001);
  pbPartitionLocation->set_pushport(1002);
  pbPartitionLocation->set_fetchport(1003);
  pbPartitionLocation->set_replicateport(1004);
  return std::move(pbPartitionLocation);
}

void verifyBasicPartitionLocation(const PartitionLocation* partitionLocation) {
  EXPECT_EQ(partitionLocation->id, 1);
  EXPECT_EQ(partitionLocation->epoch, 101);
  EXPECT_EQ(partitionLocation->host, "test_host");
  EXPECT_EQ(partitionLocation->rpcPort, 1001);
  EXPECT_EQ(partitionLocation->pushPort, 1002);
  EXPECT_EQ(partitionLocation->fetchPort, 1003);
  EXPECT_EQ(partitionLocation->replicatePort, 1004);
}

TEST(PartitionLocationTest, storageInfoFromPb) {
  auto pbStorageInfo = generateStorageInfoPb();
  auto storageInfo = StorageInfo::fromPb(*pbStorageInfo);
  verifyStorageInfo(storageInfo.get());
}

TEST(PartitionLocationTest, fromPbWithoutPeer) {
  auto pbPartitionLocation = generateBasicPartitionLocationPb();
  pbPartitionLocation->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfo = generateStorageInfoPb();
  pbPartitionLocation->set_allocated_storageinfo(pbStorageInfo.release());

  auto partitionLocation = PartitionLocation::fromPb(*pbPartitionLocation);

  verifyBasicPartitionLocation(partitionLocation.get());
  EXPECT_EQ(partitionLocation->mode, PartitionLocation::Mode::PRIMARY);
  verifyStorageInfo(partitionLocation->storageInfo.get());
}

TEST(PartitionLocationTest, fromPbWithPeer) {
  auto pbPartitionLocationPrimary = generateBasicPartitionLocationPb();
  pbPartitionLocationPrimary->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfoPrimary = generateStorageInfoPb();
  pbPartitionLocationPrimary->set_allocated_storageinfo(
      pbStorageInfoPrimary.release());

  auto pbPartitionLocationReplica = generateBasicPartitionLocationPb();
  pbPartitionLocationReplica->set_mode(PbPartitionLocation_Mode_Replica);
  auto pbStorageInfoReplica = generateStorageInfoPb();
  pbPartitionLocationReplica->set_allocated_storageinfo(
      pbStorageInfoReplica.release());

  pbPartitionLocationPrimary->set_allocated_peer(
      pbPartitionLocationReplica.release());

  auto partitionLocationPrimary =
      PartitionLocation::fromPb(*pbPartitionLocationPrimary);

  verifyBasicPartitionLocation(partitionLocationPrimary.get());
  EXPECT_EQ(partitionLocationPrimary->mode, PartitionLocation::Mode::PRIMARY);
  verifyStorageInfo(partitionLocationPrimary->storageInfo.get());

  auto partitionLocationReplica = partitionLocationPrimary->replicaPeer.get();
  verifyBasicPartitionLocation(partitionLocationReplica);
  EXPECT_EQ(partitionLocationReplica->mode, PartitionLocation::Mode::REPLICA);
  verifyStorageInfo(partitionLocationReplica->storageInfo.get());
}
