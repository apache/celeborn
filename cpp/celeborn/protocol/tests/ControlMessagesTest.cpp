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
#include "celeborn/protocol/ControlMessages.h"

#include "celeborn/utils/CelebornUtils.h"

using namespace celeborn;
using namespace celeborn::protocol;

namespace {
void generatePackedPartitionLocationPb(
    PbPackedPartitionLocations& pbPackedPartitionLocations,
    int idx,
    PartitionLocation::Mode mode) {
  pbPackedPartitionLocations.add_ids(1);
  pbPackedPartitionLocations.add_epoches(101);
  pbPackedPartitionLocations.add_workerids(idx);
  pbPackedPartitionLocations.add_workeridsset("test-host:1001:1002:1003:1004");
  pbPackedPartitionLocations.add_mountpoints(idx);
  pbPackedPartitionLocations.add_mountpointsset("test-mountpoint/");
  pbPackedPartitionLocations.add_filepaths("test-filepath");
  pbPackedPartitionLocations.add_types(1);
  pbPackedPartitionLocations.add_finalresult(true);
  pbPackedPartitionLocations.add_availablestoragetypes(1);
  pbPackedPartitionLocations.add_modes(mode);
}

void verifyUnpackedPartitionLocation(
    const PartitionLocation* partitionLocation) {
  EXPECT_EQ(partitionLocation->id, 1);
  EXPECT_EQ(partitionLocation->epoch, 101);
  EXPECT_EQ(partitionLocation->host, "test-host");
  EXPECT_EQ(partitionLocation->rpcPort, 1001);
  EXPECT_EQ(partitionLocation->pushPort, 1002);
  EXPECT_EQ(partitionLocation->fetchPort, 1003);
  EXPECT_EQ(partitionLocation->replicatePort, 1004);

  auto storageInfo = partitionLocation->storageInfo.get();
  EXPECT_EQ(storageInfo->type, 1);
  EXPECT_EQ(storageInfo->mountPoint, "test-mountpoint/");
  EXPECT_EQ(storageInfo->finalResult, true);
  EXPECT_EQ(storageInfo->filePath, "test-mountpoint/test-filepath");
  EXPECT_EQ(storageInfo->availableStorageTypes, 1);
}
} // namespace

TEST(ControlMessagesTest, registerShuffle) {
  auto registerShuffle = std::make_unique<RegisterShuffle>();
  registerShuffle->shuffleId = 1000;
  registerShuffle->numMappers = 1001;
  registerShuffle->numPartitions = 1002;

  auto transportMessage = registerShuffle->toTransportMessage();
  EXPECT_EQ(transportMessage.type(), REGISTER_SHUFFLE);
  auto payload = transportMessage.payload();
  auto pbRegisterShuffle = utils::parseProto<PbRegisterShuffle>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  EXPECT_EQ(pbRegisterShuffle->shuffleid(), registerShuffle->shuffleId);
  EXPECT_EQ(pbRegisterShuffle->nummappers(), registerShuffle->numMappers);
  EXPECT_EQ(pbRegisterShuffle->numpartitions(), registerShuffle->numPartitions);
}

TEST(ControlMessagesTest, mapperEnd) {
  auto mapperEnd = std::make_unique<MapperEnd>();
  mapperEnd->shuffleId = 1000;
  mapperEnd->mapId = 1001;
  mapperEnd->attemptId = 1002;
  mapperEnd->numMappers = 1003;
  mapperEnd->partitionId = 1004;

  auto transportMessage = mapperEnd->toTransportMessage();
  EXPECT_EQ(transportMessage.type(), MAPPER_END);
  auto payload = transportMessage.payload();
  auto pbMapperEnd = utils::parseProto<PbMapperEnd>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  EXPECT_EQ(pbMapperEnd->shuffleid(), mapperEnd->shuffleId);
  EXPECT_EQ(pbMapperEnd->mapid(), mapperEnd->mapId);
  EXPECT_EQ(pbMapperEnd->attemptid(), mapperEnd->attemptId);
  EXPECT_EQ(pbMapperEnd->nummappers(), mapperEnd->numMappers);
  EXPECT_EQ(pbMapperEnd->partitionid(), mapperEnd->partitionId);
}

TEST(ControlMessagesTest, mapperEndResponse) {
  PbMapperEndResponse pbMapperEndResponse;
  pbMapperEndResponse.set_status(1);
  TransportMessage transportMessage(
      MAPPER_END_RESPONSE, pbMapperEndResponse.SerializeAsString());

  auto mapperEndResponse =
      MapperEndResponse::fromTransportMessage(transportMessage);
  EXPECT_EQ(mapperEndResponse->status, 1);
}

// TEST MapperEnd/Response

TEST(ControlMessagesTest, getReducerFileGroup) {
  auto getReducerFileGroup = std::make_unique<GetReducerFileGroup>();
  getReducerFileGroup->shuffleId = 1000;

  auto transportMessage = getReducerFileGroup->toTransportMessage();
  EXPECT_EQ(transportMessage.type(), GET_REDUCER_FILE_GROUP);
  auto payload = transportMessage.payload();
  auto pbGetReducerFileGroup = utils::parseProto<PbGetReducerFileGroup>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  EXPECT_EQ(pbGetReducerFileGroup->shuffleid(), 1000);
}

TEST(ControlMessagesTest, getReducerFileGroupResponseLegacyModeDeprecated) {
  PbGetReducerFileGroupResponse pbGetReducerFileGroupResponse;
  pbGetReducerFileGroupResponse.set_status(1);
  for (int i = 0; i < 4; i++) {
    pbGetReducerFileGroupResponse.add_attempts(i);
  }
  for (int i = 0; i < 6; i++) {
    pbGetReducerFileGroupResponse.add_partitionids(i);
  }
  auto id2FileGroups = pbGetReducerFileGroupResponse.mutable_filegroups();
  PbFileGroup pbFileGroup;
  pbFileGroup.add_locations();
  id2FileGroups->insert({0, pbFileGroup});

  TransportMessage transportMessage(
      GET_REDUCER_FILE_GROUP_RESPONSE,
      pbGetReducerFileGroupResponse.SerializeAsString());
  EXPECT_THROW(
      GetReducerFileGroupResponse::fromTransportMessage(transportMessage),
      utils::CelebornRuntimeError);
}

TEST(ControlMessagesTest, getReducerFileGroupResponsePackedMode) {
  PbGetReducerFileGroupResponse pbGetReducerFileGroupResponse;
  pbGetReducerFileGroupResponse.set_status(1);
  for (int i = 0; i < 4; i++) {
    pbGetReducerFileGroupResponse.add_attempts(i);
  }
  for (int i = 0; i < 6; i++) {
    pbGetReducerFileGroupResponse.add_partitionids(i);
  }

  PbFileGroup pbFileGroup;
  auto pbPackedPartitionLocationsPair =
      pbFileGroup.mutable_partitionlocationspair();
  auto pbPackedPartitionLocations =
      pbPackedPartitionLocationsPair->mutable_locations();
  // Has one inputLocation, with offset 0.
  pbPackedPartitionLocationsPair->set_inputlocationsize(1);
  // The peerIndex 1 is replica.
  pbPackedPartitionLocationsPair->add_peerindexes(1);
  // Add the two partitionLocations, one is primary and the other is replica.
  generatePackedPartitionLocationPb(
      *pbPackedPartitionLocations, 0, PartitionLocation::Mode::PRIMARY);
  generatePackedPartitionLocationPb(
      *pbPackedPartitionLocations, 1, PartitionLocation::Mode::REPLICA);

  auto id2FileGroups = pbGetReducerFileGroupResponse.mutable_filegroups();
  id2FileGroups->insert({0, pbFileGroup});

  TransportMessage transportMessage(
      GET_REDUCER_FILE_GROUP_RESPONSE,
      pbGetReducerFileGroupResponse.SerializeAsString());
  auto getReducerFileGroupResponse =
      GetReducerFileGroupResponse::fromTransportMessage(transportMessage);
  EXPECT_EQ(getReducerFileGroupResponse->status, 1);
  EXPECT_EQ(getReducerFileGroupResponse->attempts.size(), 4);
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(getReducerFileGroupResponse->attempts[i], i);
  }
  EXPECT_EQ(getReducerFileGroupResponse->partitionIds.size(), 6);
  for (int i = 0; i < 6; i++) {
    EXPECT_EQ(getReducerFileGroupResponse->partitionIds.count(i), 1);
  }
  EXPECT_EQ(getReducerFileGroupResponse->fileGroups.size(), 1);
  const auto& partitionLocations = getReducerFileGroupResponse->fileGroups[0];
  EXPECT_EQ(partitionLocations.size(), 1);
  auto primaryPartitionLocation = partitionLocations.begin()->get();
  verifyUnpackedPartitionLocation(primaryPartitionLocation);
  EXPECT_EQ(primaryPartitionLocation->mode, PartitionLocation::Mode::PRIMARY);
  auto replicaPartitionLocation = primaryPartitionLocation->replicaPeer.get();
  verifyUnpackedPartitionLocation(replicaPartitionLocation);
  EXPECT_EQ(replicaPartitionLocation->mode, PartitionLocation::Mode::REPLICA);
}

TEST(ControlMessagesTest, openStream) {
  auto openStream = std::make_unique<OpenStream>(
      "test-shuffle-key", "test-filename", 100, 200);
  auto transportMessage = openStream->toTransportMessage();
  EXPECT_EQ(transportMessage.type(), OPEN_STREAM);
  auto payload = transportMessage.payload();
  auto pbOpenStream = utils::parseProto<PbOpenStream>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  EXPECT_EQ(pbOpenStream->shufflekey(), "test-shuffle-key");
  EXPECT_EQ(pbOpenStream->filename(), "test-filename");
  EXPECT_EQ(pbOpenStream->startindex(), 100);
  EXPECT_EQ(pbOpenStream->endindex(), 200);
}

TEST(ControlMessagesTest, streamHandler) {
  PbStreamHandler pb;
  pb.set_streamid(100);
  pb.set_numchunks(4);
  for (int i = 0; i < 4; i++) {
    pb.add_chunkoffsets(i);
  }
  pb.set_fullpath("test-fullpath");
  TransportMessage transportMessage(STREAM_HANDLER, pb.SerializeAsString());

  auto streamHandler = StreamHandler::fromTransportMessage(transportMessage);
  EXPECT_EQ(streamHandler->streamId, 100);
  EXPECT_EQ(streamHandler->numChunks, 4);
  EXPECT_EQ(streamHandler->chunkOffsets.size(), 4);
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(streamHandler->chunkOffsets[i], i);
  }
  EXPECT_EQ(streamHandler->fullPath, "test-fullpath");
}

TEST(ControlMessagesTest, streamChunkSlice) {
  StreamChunkSlice streamChunkSlice;
  streamChunkSlice.streamId = 100;
  streamChunkSlice.chunkIndex = 1000;
  streamChunkSlice.offset = 111;
  streamChunkSlice.len = 1111;

  auto pb = streamChunkSlice.toProto();
  EXPECT_EQ(pb->streamid(), 100);
  EXPECT_EQ(pb->chunkindex(), 1000);
  EXPECT_EQ(pb->offset(), 111);
  EXPECT_EQ(pb->len(), 1111);

  auto writeBuffer = memory::ByteBuffer::createWriteOnly(20);
  writeBuffer->write<long>(streamChunkSlice.streamId);
  writeBuffer->write<int>(streamChunkSlice.chunkIndex);
  writeBuffer->write<int>(streamChunkSlice.offset);
  writeBuffer->write<int>(streamChunkSlice.len);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto decodedStreamChunkSlice = StreamChunkSlice::decodeFrom(*readBuffer);
  EXPECT_EQ(readBuffer->remainingSize(), 0);
  EXPECT_EQ(streamChunkSlice.streamId, decodedStreamChunkSlice.streamId);
  EXPECT_EQ(streamChunkSlice.chunkIndex, decodedStreamChunkSlice.chunkIndex);
  EXPECT_EQ(streamChunkSlice.offset, decodedStreamChunkSlice.offset);
  EXPECT_EQ(streamChunkSlice.len, decodedStreamChunkSlice.len);
}

TEST(ControlMessagesTest, chunkFetchRequest) {
  ChunkFetchRequest chunkFetchRequest;
  StreamChunkSlice streamChunkSlice;
  streamChunkSlice.streamId = 100;
  streamChunkSlice.chunkIndex = 1000;
  streamChunkSlice.offset = 111;
  streamChunkSlice.len = 1111;
  chunkFetchRequest.streamChunkSlice = streamChunkSlice;

  auto transportMessage = chunkFetchRequest.toTransportMessage();
  EXPECT_EQ(transportMessage.type(), CHUNK_FETCH_REQUEST);
  auto payload = transportMessage.payload();
  auto pbChunkFetchRequest = utils::parseProto<PbChunkFetchRequest>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  auto pbStreamChunkSlice = pbChunkFetchRequest->streamchunkslice();
  EXPECT_EQ(pbStreamChunkSlice.streamid(), 100);
  EXPECT_EQ(pbStreamChunkSlice.chunkindex(), 1000);
  EXPECT_EQ(pbStreamChunkSlice.offset(), 111);
  EXPECT_EQ(pbStreamChunkSlice.len(), 1111);
}

TEST(ControlMessagesTest, bufferStreamEnd) {
  BufferStreamEnd bufferStreamEnd;
  bufferStreamEnd.streamId = 111111;

  auto transportMessage = bufferStreamEnd.toTransportMessage();
  EXPECT_EQ(transportMessage.type(), BUFFER_STREAM_END);
  auto payload = transportMessage.payload();
  auto pb = utils::parseProto<PbBufferStreamEnd>(
      reinterpret_cast<const uint8_t*>(payload.c_str()), payload.size());
  EXPECT_EQ(pb->streamid(), 111111);
}
