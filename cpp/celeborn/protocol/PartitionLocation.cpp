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

#include "celeborn/protocol/PartitionLocation.h"

#include "celeborn/protocol/StatusCode.h"
#include "celeborn/utils/CelebornUtils.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace protocol {
std::unique_ptr<StorageInfo> StorageInfo::fromPb(const PbStorageInfo& pb) {
  auto result = std::make_unique<StorageInfo>();
  result->type = static_cast<Type>(pb.type());
  result->mountPoint = pb.mountpoint();
  result->finalResult = pb.finalresult();
  result->filePath = pb.filepath();
  result->availableStorageTypes = pb.availablestoragetypes();
  return std::move(result);
}

std::unique_ptr<const PartitionLocation> PartitionLocation::fromPb(
    const PbPartitionLocation& pb) {
  auto result = fromPbWithoutPeer(pb);
  if (pb.has_peer()) {
    auto peer = fromPbWithoutPeer(pb.peer());
    if (result->mode == PRIMARY) {
      CELEBORN_CHECK(
          peer->mode == REPLICA, "PRIMARY's peer mode should be REPLICA");
      result->replicaPeer = std::move(peer);
    } else {
      CELEBORN_CHECK(
          peer->mode == PRIMARY, "REPLICA's peer mode should be PRIMARY");
      peer->replicaPeer = std::move(result);
      result = std::move(peer);
    }
  }
  CELEBORN_CHECK(result->mode == PRIMARY, "non-peer's mode should be PRIMARY");
  return std::move(result);
}

std::unique_ptr<PartitionLocation> PartitionLocation::fromPackedPb(
    const PbPackedPartitionLocations& pb,
    int idx) {
  auto& workerIdStr = pb.workeridsset(pb.workerids(idx));
  auto workerIdParts = utils::parseColonSeparatedHostPorts(workerIdStr, 4);
  std::string filePath = pb.filepaths(idx);
  if (!filePath.empty()) {
    filePath = pb.mountpointsset(pb.mountpoints(idx)) + pb.filepaths(idx);
  }

  auto result = std::make_unique<PartitionLocation>();
  result->id = pb.ids(idx);
  result->epoch = pb.epoches(idx);
  result->host = workerIdParts[0];
  result->rpcPort = utils::strv2val<int>(workerIdParts[1]);
  result->pushPort = utils::strv2val<int>(workerIdParts[2]);
  result->fetchPort = utils::strv2val<int>(workerIdParts[3]);
  result->replicatePort = utils::strv2val<int>(workerIdParts[4]);
  result->mode = static_cast<Mode>(pb.modes(idx));
  result->replicaPeer = nullptr;
  result->storageInfo = std::make_unique<StorageInfo>();
  result->storageInfo->type = static_cast<StorageInfo::Type>(pb.types(idx));
  result->storageInfo->mountPoint = pb.mountpointsset(pb.mountpoints(idx));
  result->storageInfo->finalResult = pb.finalresult(idx);
  result->storageInfo->filePath = filePath;
  result->storageInfo->availableStorageTypes = pb.availablestoragetypes(idx);

  return std::move(result);
}

PartitionLocation::PartitionLocation(const PartitionLocation& other)
    : id(other.id),
      epoch(other.epoch),
      host(other.host),
      rpcPort(other.rpcPort),
      pushPort(other.pushPort),
      fetchPort(other.fetchPort),
      replicatePort(other.replicatePort),
      mode(other.mode),
      replicaPeer(
          other.replicaPeer
              ? std::make_unique<PartitionLocation>(*other.replicaPeer)
              : nullptr),
      storageInfo(std::make_unique<StorageInfo>(*other.storageInfo)) {}

std::unique_ptr<PartitionLocation> PartitionLocation::fromPbWithoutPeer(
    const PbPartitionLocation& pb) {
  auto result = std::make_unique<PartitionLocation>();
  result->id = pb.id();
  result->epoch = pb.epoch();
  result->host = pb.host();
  result->rpcPort = pb.rpcport();
  result->pushPort = pb.pushport();
  result->fetchPort = pb.fetchport();
  result->replicatePort = pb.replicateport();
  result->mode = static_cast<Mode>(pb.mode());
  result->replicaPeer = nullptr;
  result->storageInfo = StorageInfo::fromPb(pb.storageinfo());
  return std::move(result);
}

StatusCode toStatusCode(int32_t code) {
  CELEBORN_CHECK(code >= 0);
  CELEBORN_CHECK(code <= StatusCode::TAIL);
  return static_cast<StatusCode>(code);
}
} // namespace protocol
} // namespace celeborn
