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

#include <map>
#include <set>

#include "celeborn/protocol/PartitionLocation.h"
#include "celeborn/protocol/StatusCode.h"
#include "celeborn/protocol/TransportMessage.h"

namespace celeborn {
namespace protocol {
struct MapperEnd {
  long shuffleId;
  int mapId;
  int attemptId;
  int numMappers;
  int partitionId;

  TransportMessage toTransportMessage() const;
};

struct MapperEndResponse {
  StatusCode status;

  static std::unique_ptr<MapperEndResponse> fromTransportMessage(
      const TransportMessage& transportMessage);
};

struct GetReducerFileGroup {
  int shuffleId;

  TransportMessage toTransportMessage() const;
};

struct GetReducerFileGroupResponse {
  StatusCode status;
  std::map<int, std::set<std::shared_ptr<const PartitionLocation>>> fileGroups;
  std::vector<int> attempts;
  std::set<int> partitionIds;

  static std::unique_ptr<GetReducerFileGroupResponse> fromTransportMessage(
      const TransportMessage& transportMessage);
};

struct OpenStream {
  std::string shuffleKey;
  std::string filename;
  int32_t startMapIndex;
  int32_t endMapIndex;

  OpenStream(
      const std::string& shuffleKey,
      const std::string& filename,
      int32_t startMapIndex,
      int32_t endMapIndex);

  TransportMessage toTransportMessage() const;
};

struct StreamHandler {
  int64_t streamId;
  int32_t numChunks;
  std::vector<int64_t> chunkOffsets;
  std::string fullPath;

  static std::unique_ptr<StreamHandler> fromTransportMessage(
      const TransportMessage& transportMessage);
};

struct StreamChunkSlice {
  long streamId;
  int chunkIndex;
  int offset{0};
  int len{INT_MAX};

  std::unique_ptr<PbStreamChunkSlice> toProto() const;

  static StreamChunkSlice decodeFrom(memory::ReadOnlyByteBuffer& data);

  std::string toString() const {
    return std::to_string(streamId) + "-" + std::to_string(chunkIndex) + "-" +
        std::to_string(offset) + "-" + std::to_string(len);
  }

  bool operator==(const StreamChunkSlice& rhs) const {
    return streamId == rhs.streamId && chunkIndex == rhs.chunkIndex &&
        offset == rhs.offset && len == rhs.len;
  }

  struct Hasher {
    size_t operator()(const StreamChunkSlice& lhs) const;
  };
};

struct ChunkFetchRequest {
  StreamChunkSlice streamChunkSlice;

  TransportMessage toTransportMessage() const;
};

struct BufferStreamEnd {
  long streamId;

  TransportMessage toTransportMessage() const;
};
} // namespace protocol
} // namespace celeborn
