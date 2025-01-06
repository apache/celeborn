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

#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace protocol {
TransportMessage GetReducerFileGroup::toTransportMessage() const {
  MessageType type = GET_REDUCER_FILE_GROUP;
  PbGetReducerFileGroup pb;
  pb.set_shuffleid(shuffleId);
  std::string payload = pb.SerializeAsString();
  return TransportMessage(type, std::move(payload));
}

std::unique_ptr<GetReducerFileGroupResponse>
GetReducerFileGroupResponse::fromTransportMessage(
    const TransportMessage& transportMessage) {
  CELEBORN_CHECK(
      transportMessage.type() == GET_REDUCER_FILE_GROUP_RESPONSE,
      "transportMessageType mismatch");
  auto payload = transportMessage.payload();
  auto pbGetReducerFileGroupResponse =
      utils::parseProto<PbGetReducerFileGroupResponse>(
          reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  auto response = std::make_unique<GetReducerFileGroupResponse>();
  response->status = toStatusCode(pbGetReducerFileGroupResponse->status());
  auto fileGroups = pbGetReducerFileGroupResponse->filegroups();
  for (auto& kv : fileGroups) {
    auto& fileGroup = response->fileGroups[kv.first];
    // Legacy mode: the locations are valid, so use it.
    if (kv.second.locations().size() > 0) {
      for (const auto& location : kv.second.locations()) {
        fileGroup.insert(PartitionLocation::fromPb(location));
      }
    }
    // Packed mode: must use packedPartitionLocations.
    else {
      auto& pbPackedPartitionLocationsPair = kv.second.partitionlocationspair();
      int inputLocationSize =
          pbPackedPartitionLocationsPair.inputlocationsize();
      auto& pbPackedPartitionLocations =
          pbPackedPartitionLocationsPair.locations();
      std::vector<std::unique_ptr<PartitionLocation>> partialLocations;
      auto& pbIds = pbPackedPartitionLocations.ids();
      for (int idx = 0; idx < pbIds.size(); idx++) {
        partialLocations.push_back(
            PartitionLocation::fromPackedPb(pbPackedPartitionLocations, idx));
      }
      for (int idx = 0; idx < inputLocationSize; idx++) {
        auto replicaIdx = pbPackedPartitionLocationsPair.peerindexes(idx);
        // has peer
        if (replicaIdx != INT_MAX) {
          CELEBORN_CHECK_GE(replicaIdx, inputLocationSize);
          CELEBORN_CHECK_LT(replicaIdx, partialLocations.size());
          auto location = std::move(partialLocations[idx]);
          auto peerLocation = std::move(partialLocations[replicaIdx]);
          // make sure the location is primary and peer location is replica
          if (location->mode == PartitionLocation::Mode::REPLICA) {
            std::swap(location, peerLocation);
          }
          CELEBORN_CHECK(location->mode == PartitionLocation::Mode::PRIMARY);
          CELEBORN_CHECK(
              peerLocation->mode == PartitionLocation::Mode::REPLICA);
          location->replicaPeer = std::move(peerLocation);
          fileGroup.insert(std::move(location));
        }
        // has no peer
        else {
          fileGroup.insert(std::move(partialLocations[idx]));
        }
      }
    }
  }
  auto attempts = pbGetReducerFileGroupResponse->attempts();
  response->attempts.reserve(attempts.size());
  for (auto attempt : attempts) {
    response->attempts.push_back(attempt);
  }
  auto partitionIds = pbGetReducerFileGroupResponse->partitionids();
  for (auto partitionId : partitionIds) {
    response->partitionIds.insert(partitionId);
  }
  return std::move(response);
}

OpenStream::OpenStream(
    const std::string& shuffleKey,
    const std::string& filename,
    int32_t startMapIndex,
    int32_t endMapIndex)
    : shuffleKey(shuffleKey),
      filename(filename),
      startMapIndex(startMapIndex),
      endMapIndex(endMapIndex) {}

TransportMessage OpenStream::toTransportMessage() const {
  MessageType type = OPEN_STREAM;
  PbOpenStream pb;
  pb.set_shufflekey(shuffleKey);
  pb.set_filename(filename);
  pb.set_startindex(startMapIndex);
  pb.set_endindex(endMapIndex);
  std::string payload = pb.SerializeAsString();
  return TransportMessage(type, std::move(payload));
}

std::unique_ptr<StreamHandler> StreamHandler::fromTransportMessage(
    const TransportMessage& transportMessage) {
  CELEBORN_CHECK(
      transportMessage.type() == STREAM_HANDLER,
      "transportMessageType should be STREAM_HANDLER");
  auto payload = transportMessage.payload();
  auto pbStreamHandler = utils::parseProto<PbStreamHandler>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  auto streamHandler = std::make_unique<StreamHandler>();
  streamHandler->streamId = pbStreamHandler->streamid();
  streamHandler->numChunks = pbStreamHandler->numchunks();
  for (auto chunkOffset : pbStreamHandler->chunkoffsets()) {
    streamHandler->chunkOffsets.push_back(chunkOffset);
  }
  streamHandler->fullPath = pbStreamHandler->fullpath();
  return std::move(streamHandler);
}

std::unique_ptr<PbStreamChunkSlice> StreamChunkSlice::toProto() const {
  auto pb = std::make_unique<PbStreamChunkSlice>();
  pb->set_streamid(streamId);
  pb->set_chunkindex(chunkIndex);
  pb->set_offset(offset);
  pb->set_len(len);
  return std::move(pb);
}

StreamChunkSlice StreamChunkSlice::decodeFrom(
    memory::ReadOnlyByteBuffer& data) {
  CELEBORN_CHECK_GE(data.remainingSize(), 20);
  StreamChunkSlice slice;
  slice.streamId = data.read<long>();
  slice.chunkIndex = data.read<int>();
  slice.offset = data.read<int>();
  slice.len = data.read<int>();
  return slice;
}

size_t StreamChunkSlice::Hasher::operator()(const StreamChunkSlice& lhs) const {
  const auto hashStreamId = std::hash<long>()(lhs.streamId);
  const auto hashChunkIndex = std::hash<int>()(lhs.chunkIndex) << 1;
  const auto hashOffset = std::hash<int>()(lhs.offset) << 2;
  const auto hashLen = std::hash<int>()(lhs.len) << 3;
  return hashStreamId ^ hashChunkIndex ^ hashOffset ^ hashLen;
}

TransportMessage ChunkFetchRequest::toTransportMessage() const {
  MessageType type = CHUNK_FETCH_REQUEST;
  PbChunkFetchRequest pb;
  pb.unsafe_arena_set_allocated_streamchunkslice(
      streamChunkSlice.toProto().release());
  std::string payload = pb.SerializeAsString();
  return TransportMessage(type, std::move(payload));
}

TransportMessage BufferStreamEnd::toTransportMessage() const {
  MessageType type = BUFFER_STREAM_END;
  PbBufferStreamEnd pb;
  pb.set_streamtype(ChunkStream);
  pb.set_streamid(streamId);
  std::string payload = pb.SerializeAsString();
  return TransportMessage(type, std::move(payload));
}
} // namespace protocol
} // namespace celeborn
