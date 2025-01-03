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

#include "celeborn/proto/TransportMessagesCpp.pb.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace protocol {
struct StorageInfo {
  static std::unique_ptr<StorageInfo> fromPb(const PbStorageInfo& pb);

  StorageInfo() = default;

  StorageInfo(const StorageInfo& other) = default;

  enum Type {
    MEMORY = 0,
    HDD = 1,
    SSD = 2,
    HDFS = 3,
    OSS = 4,
    S3 = 5,
  };

  static const int MEMORY_MASK = 0b1;
  static const int LOCAL_DISK_MASK = 0b10;
  static const int HDFS_MASK = 0b100;
  static const int OSS_MASK = 0b1000;
  static const int S3_MASK = 0b10000;
  static const int ALL_TYPES_AVAILABLE_MASK = 0;

  Type type{MEMORY};
  std::string mountPoint;
  // if a file is committed, field "finalResult" will be true
  bool finalResult{false};
  std::string filePath;
  int availableStorageTypes{0};
};

struct PartitionLocation {
  enum Mode {
    PRIMARY = 0,
    REPLICA = 1,
  };

  int id;
  int epoch;
  std::string host;
  int rpcPort;
  int pushPort;
  int fetchPort;
  int replicatePort;
  Mode mode;
  // Only primary PartitionLocation might have replicaPeer.
  // Replica PartitionLocation would not have replicaPeer.
  // The lifecycle of Replica is bounded with Primary.
  std::unique_ptr<PartitionLocation> replicaPeer{nullptr};
  std::unique_ptr<StorageInfo> storageInfo;
  // TODO: RoaringBitmap is not supported yet.
  // RoaringBitmap mapIdBitMap;

  static std::unique_ptr<const PartitionLocation> fromPb(
      const PbPartitionLocation& pb);

  PartitionLocation() = default;

  PartitionLocation(const PartitionLocation& other);

  std::string filename() const {
    return std::to_string(id) + "-" + std::to_string(epoch) + "-" +
        std::to_string(mode);
  }

 private:
  static std::unique_ptr<PartitionLocation> fromPbWithoutPeer(
      const PbPartitionLocation& pb);
};
} // namespace protocol
} // namespace celeborn
