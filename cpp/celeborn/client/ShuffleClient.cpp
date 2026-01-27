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

#include "celeborn/client/ShuffleClient.h"

#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace client {

ShuffleClientEndpoint::ShuffleClientEndpoint(
    const std::shared_ptr<const conf::CelebornConf>& conf)
    : conf_(conf),
      pushDataRetryPool_(std::make_shared<folly::IOThreadPoolExecutor>(
          conf_->clientPushRetryThreads(),
          std::make_shared<folly::NamedThreadFactory>(
              "client-pushdata-retrier"))),
      clientFactory_(std::make_shared<network::TransportClientFactory>(conf_)) {
}

std::shared_ptr<folly::IOThreadPoolExecutor>
ShuffleClientEndpoint::pushDataRetryPool() const {
  return pushDataRetryPool_;
}

std::shared_ptr<network::TransportClientFactory>
ShuffleClientEndpoint::clientFactory() const {
  return clientFactory_;
}

std::shared_ptr<ShuffleClientImpl> ShuffleClientImpl::create(
    const std::string& appUniqueId,
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const ShuffleClientEndpoint& clientEndpoint) {
  return std::shared_ptr<ShuffleClientImpl>(
      new ShuffleClientImpl(appUniqueId, conf, clientEndpoint));
}

ShuffleClientImpl::ShuffleClientImpl(
    const std::string& appUniqueId,
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const ShuffleClientEndpoint& clientEndpoint)
    : appUniqueId_(appUniqueId),
      conf_(conf),
      clientFactory_(clientEndpoint.clientFactory()),
      pushDataRetryPool_(clientEndpoint.pushDataRetryPool()),
      shuffleCompressionEnabled_(
          conf->shuffleCompressionCodec() != protocol::CompressionCodec::NONE),
      compressor_(
          shuffleCompressionEnabled_
              ? compress::Compressor::createCompressor(*conf)
              : nullptr) {
  CELEBORN_CHECK_NOT_NULL(clientFactory_);
  CELEBORN_CHECK_NOT_NULL(pushDataRetryPool_);
}

void ShuffleClientImpl::setupLifecycleManagerRef(std::string& host, int port) {
  auto managerClient = clientFactory_->createClient(host, port);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycleManagerRef_ = std::make_shared<network::NettyRpcEndpointRef>(
        "LifecycleManagerEndpoint",
        "dummy",
        0,
        host,
        port,
        managerClient,
        *conf_);

    initReviveManagerLocked();
  }
}

void ShuffleClientImpl::setupLifecycleManagerRef(
    std::shared_ptr<network::NettyRpcEndpointRef>& lifecycleManagerRef) {
  std::lock_guard<std::mutex> lock(mutex_);
  lifecycleManagerRef_ = lifecycleManagerRef;

  initReviveManagerLocked();
}

std::shared_ptr<utils::ConcurrentHashMap<
    int,
    std::shared_ptr<const protocol::PartitionLocation>>>
ShuffleClientImpl::getPartitionLocation(
    int shuffleId,
    int numMappers,
    int numPartitions) {
  auto partitionLocationOptional = partitionLocationMaps_.get(shuffleId);
  if (partitionLocationOptional.has_value()) {
    return partitionLocationOptional.value();
  }

  registerShuffle(shuffleId, numMappers, numPartitions);

  partitionLocationOptional = partitionLocationMaps_.get(shuffleId);
  CELEBORN_CHECK(
      partitionLocationOptional.has_value(),
      "partitionLocation is empty because registerShuffle failed");
  auto partitionLocationMap = partitionLocationOptional.value();
  CELEBORN_CHECK_NOT_NULL(partitionLocationMap);
  return partitionLocationMap;
}

int ShuffleClientImpl::pushData(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    const uint8_t* data,
    size_t offset,
    size_t length,
    int numMappers,
    int numPartitions) {
  const auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  if (checkMapperEnded(shuffleId, mapId, mapKey)) {
    return 0;
  }

  auto partitionLocationMap =
      getPartitionLocation(shuffleId, numMappers, numPartitions);
  CELEBORN_CHECK_NOT_NULL(partitionLocationMap);
  auto partitionLocationOptional = partitionLocationMap->get(partitionId);
  if (!partitionLocationOptional.has_value()) {
    if (!revive(
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            -1,
            nullptr,
            protocol::StatusCode::PUSH_DATA_FAIL_NON_CRITICAL_CAUSE)) {
      CELEBORN_FAIL(fmt::format(
          "Revive for shuffleId {} partitionId {} failed.",
          shuffleId,
          partitionId));
    }
    partitionLocationOptional = partitionLocationMap->get(partitionId);
  }
  if (checkMapperEnded(shuffleId, mapId, mapKey)) {
    return 0;
  }

  CELEBORN_CHECK(partitionLocationOptional.has_value());
  auto partitionLocation = partitionLocationOptional.value();
  auto pushState = getPushState(mapKey);
  const int nextBatchId = pushState->nextBatchId();

  // Compression support: compress data if compression is enabled
  const uint8_t* dataToWrite = data + offset;
  size_t lengthToWrite = length;
  std::unique_ptr<uint8_t[]> compressedBuffer;

  if (shuffleCompressionEnabled_ && compressor_) {
    // Allocate buffer for compressed data
    const size_t compressedCapacity = compressor_->getDstCapacity(length);
    compressedBuffer = std::make_unique<uint8_t[]>(compressedCapacity);

    // Compress the data
    lengthToWrite = compressor_->compress(
        dataToWrite, 0, length, compressedBuffer.get(), 0);
    dataToWrite = compressedBuffer.get();
  }

  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(kBatchHeaderSize + lengthToWrite);
  // TODO: the java side uses Platform to write the data. We simply assume
  //  littleEndian here.
  writeBuffer->writeLE<int>(mapId);
  writeBuffer->writeLE<int>(attemptId);
  writeBuffer->writeLE<int>(nextBatchId);
  writeBuffer->writeLE<int>(lengthToWrite);
  writeBuffer->writeFromBuffer(dataToWrite, 0, lengthToWrite);

  auto hostAndPushPort = partitionLocation->hostAndPushPort();
  // Check limit.
  limitMaxInFlight(mapKey, *pushState, hostAndPushPort);
  // Add inFlight requests.
  const int batchBytesSize = lengthToWrite + kBatchHeaderSize;
  pushState->addBatch(nextBatchId, batchBytesSize, hostAndPushPort);
  // Build pushData request.
  const auto shuffleKey = utils::makeShuffleKey(appUniqueId_, shuffleId);
  auto body = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  network::PushData pushData(
      network::Message::nextRequestId(),
      protocol::PartitionLocation::Mode::PRIMARY,
      shuffleKey,
      partitionLocation->uniqueId(),
      body->clone());
  // Build callback.
  auto pushDataCallback = PushDataCallback::create(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      numMappers,
      numPartitions,
      mapKey,
      nextBatchId,
      body->clone(),
      pushState,
      weak_from_this(),
      conf_->clientPushMaxReviveTimes(),
      partitionLocation);
  // Do push data.
  auto client = clientFactory_->createClient(
      partitionLocation->host, partitionLocation->pushPort, partitionId);
  client->pushDataAsync(
      pushData, conf_->clientPushDataTimeout(), pushDataCallback);
  return body->remainingSize();
}

void ShuffleClientImpl::mapperEnd(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers) {
  mapPartitionMapperEnd(shuffleId, mapId, attemptId, numMappers, -1);
}

void ShuffleClientImpl::mapPartitionMapperEnd(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int partitionId) {
  auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  auto pushState = getPushState(mapKey);

  try {
    limitZeroInFlight(mapKey, *pushState);

    auto mapperEndResponse =
        lifecycleManagerRef_
            ->askSync<protocol::MapperEnd, protocol::MapperEndResponse>(
                protocol::MapperEnd{
                    shuffleId, mapId, attemptId, numMappers, partitionId});
    if (mapperEndResponse->status != protocol::StatusCode::SUCCESS) {
      CELEBORN_FAIL(
          "MapperEnd failed. protocol::StatusCode " +
          std::to_string(mapperEndResponse->status));
    }
  } catch (std::exception& e) {
    LOG(ERROR) << "mapperEnd failed, error msg: " << e.what();
    pushStates_.erase(mapKey);
    CELEBORN_FAIL(e.what());
  }
  pushStates_.erase(mapKey);
}

void ShuffleClientImpl::cleanup(int shuffleId, int mapId, int attemptId) {
  auto mapKey = utils::makeMapKey(shuffleId, mapId, attemptId);
  auto pushStateOptional = pushStates_.erase(mapKey);
  if (pushStateOptional.has_value()) {
    auto pushState = pushStateOptional.value();
    pushState->setException(
        std::make_unique<std::runtime_error>(mapKey + "is cleaned up"));
  }
}

std::unique_ptr<CelebornInputStream> ShuffleClientImpl::readPartition(
    int shuffleId,
    int partitionId,
    int attemptNumber,
    int startMapIndex,
    int endMapIndex) {
  return ShuffleClientImpl::readPartition(
      shuffleId, partitionId, attemptNumber, startMapIndex, endMapIndex, true);
}

std::unique_ptr<CelebornInputStream> ShuffleClientImpl::readPartition(
    int shuffleId,
    int partitionId,
    int attemptNumber,
    int startMapIndex,
    int endMapIndex,
    bool needCompression) {
  const auto reducerFileGroupInfo = getReducerFileGroupInfo(shuffleId);
  CELEBORN_CHECK_NOT_NULL(reducerFileGroupInfo);
  std::string shuffleKey = utils::makeShuffleKey(appUniqueId_, shuffleId);
  std::vector<std::shared_ptr<const protocol::PartitionLocation>> locations;
  if (!reducerFileGroupInfo->fileGroups.empty() &&
      reducerFileGroupInfo->fileGroups.count(partitionId)) {
    locations = std::move(utils::toVector(
        reducerFileGroupInfo->fileGroups.find(partitionId)->second));
  }
  return std::make_unique<CelebornInputStream>(
      shuffleKey,
      conf_,
      clientFactory_,
      std::move(locations),
      reducerFileGroupInfo->attempts,
      attemptNumber,
      startMapIndex,
      endMapIndex,
      needCompression);
}

void ShuffleClientImpl::updateReducerFileGroup(int shuffleId) {
  CELEBORN_CHECK(
      lifecycleManagerRef_, "lifecycleManagerRef_ is not initialized");
  // Send the query request to lifecycleManager.
  auto reducerFileGroupInfo = lifecycleManagerRef_->askSync<
      protocol::GetReducerFileGroup,
      protocol::GetReducerFileGroupResponse>(
      protocol::GetReducerFileGroup{shuffleId},
      conf_->clientRpcGetReducerFileGroupRpcAskTimeout());

  switch (reducerFileGroupInfo->status) {
    case protocol::SUCCESS: {
      VLOG(1) << "success to get reducerFileGroupInfo, shuffleId " << shuffleId;
      reducerFileGroupInfos_.set(
          shuffleId,
          std::shared_ptr<protocol::GetReducerFileGroupResponse>(
              reducerFileGroupInfo.release()));
      return;
    }
    case protocol::SHUFFLE_NOT_REGISTERED: {
      // We cannot treat this as a failure. It indicates this is an empty
      // shuffle.
      LOG(WARNING) << "shuffleId " << shuffleId
                   << " is not registered when get reducerFileGroupInfo";
      reducerFileGroupInfos_.set(
          shuffleId,
          std::shared_ptr<protocol::GetReducerFileGroupResponse>(
              reducerFileGroupInfo.release()));
      return;
    }
    case protocol::STAGE_END_TIME_OUT:
    case protocol::SHUFFLE_DATA_LOST: {
      LOG(ERROR) << "shuffleId " << shuffleId
                 << " failed getReducerFileGroupInfo with code "
                 << reducerFileGroupInfo->status;
      CELEBORN_FAIL("failed protocol::GetReducerFileGroupResponse code");
    }
    default: {
      CELEBORN_FAIL("undefined protocol::GetReducerFileGroupResponse code");
    }
  }
}

bool ShuffleClientImpl::cleanupShuffle(int shuffleId) {
  std::lock_guard<std::mutex> lock(mutex_);
  reducerFileGroupInfos_.erase(shuffleId);
  return true;
}

std::shared_ptr<PushState> ShuffleClientImpl::getPushState(
    const std::string& mapKey) {
  return pushStates_.computeIfAbsent(
      mapKey, [&]() { return std::make_shared<PushState>(*conf_); });
}

void ShuffleClientImpl::initReviveManagerLocked() {
  if (!reviveManager_) {
    std::string uniqueName = appUniqueId_;
    uniqueName += std::to_string(utils::currentTimeNanos());
    reviveManager_ =
        ReviveManager::create(uniqueName, *conf_, weak_from_this());
  }
}

void ShuffleClientImpl::registerShuffle(
    int shuffleId,
    int numMappers,
    int numPartitions) {
  auto shuffleMutex = shuffleMutexes_.computeIfAbsent(
      shuffleId, []() { return std::make_shared<std::mutex>(); });
  // RegisterShuffle might be issued concurrently, we only allow one issue
  // for each shuffleId.
  std::lock_guard<std::mutex> lock(*shuffleMutex);
  if (partitionLocationMaps_.containsKey(shuffleId)) {
    return;
  }
  CELEBORN_CHECK(
      lifecycleManagerRef_, "lifecycleManagerRef_ is not initialized");
  const int maxRetries = conf_->clientRegisterShuffleMaxRetries();
  int numRetries = 1;
  for (; numRetries <= maxRetries; numRetries++) {
    try {
      // Send the query request to lifecycleManager.
      auto registerShuffleResponse = lifecycleManagerRef_->askSync<
          protocol::RegisterShuffle,
          protocol::RegisterShuffleResponse>(
          protocol::RegisterShuffle{shuffleId, numMappers, numPartitions},
          conf_->clientRpcRegisterShuffleRpcAskTimeout());

      switch (registerShuffleResponse->status) {
        case protocol::StatusCode::SUCCESS: {
          VLOG(1) << "success to registerShuffle, shuffleId " << shuffleId
                  << " numMappers " << numMappers << " numPartitions "
                  << numPartitions;
          auto partitionLocationMap = std::make_shared<utils::ConcurrentHashMap<
              int,
              std::shared_ptr<const protocol::PartitionLocation>>>();
          auto& partitionLocations =
              registerShuffleResponse->partitionLocations;
          for (auto i = 0; i < partitionLocations.size(); i++) {
            auto id = partitionLocations[i]->id;
            partitionLocationMap->set(id, std::move(partitionLocations[i]));
          }
          partitionLocationMaps_.set(
              shuffleId, std::move(partitionLocationMap));
          return;
        }
        default: {
          LOG(ERROR)
              << "LifecycleManager request slots return protocol::StatusCode "
              << registerShuffleResponse->status << " , shuffleId " << shuffleId
              << " numMappers " << numMappers << " numPartitions "
              << numPartitions << " , retry again, remain retry times "
              << maxRetries - numRetries;
        }
      }
    } catch (std::exception& e) {
      CELEBORN_FAIL(fmt::format(
          "registerShuffle encounters error after {} tries, "
          "shuffleId {} numMappers {} numPartitions {}, errorMsg: {}",
          numRetries,
          shuffleId,
          numMappers,
          numPartitions,
          e.what()));
      break;
    }
    std::this_thread::sleep_for(conf_->clientRegisterShuffleRetryWait());
  }
  partitionLocationMaps_.set(shuffleId, nullptr);
  CELEBORN_FAIL(fmt::format(
      "registerShuffle failed after {} tries, "
      "shuffleId {} numMappers {} numPartitions {}",
      maxRetries,
      shuffleId,
      numMappers,
      numPartitions));
}

void ShuffleClientImpl::submitRetryPushData(
    int shuffleId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> body,
    int batchId,
    std::shared_ptr<PushDataCallback> pushDataCallback,
    std::shared_ptr<PushState> pushState,
    PtrReviveRequest request,
    int remainReviveTimes,
    long dueTimeMs) {
  long reviveWaitTimeMs = dueTimeMs - utils::currentTimeMillis();
  long accumulatedTimeMs = 0;
  const long deltaMs = 50;
  while (request->reviveStatus.load() ==
             protocol::StatusCode::REVIVE_INITIALIZED &&
         accumulatedTimeMs <= reviveWaitTimeMs) {
    std::this_thread::sleep_for(utils::MS(deltaMs));
    accumulatedTimeMs += deltaMs;
  }
  if (mapperEnded(shuffleId, request->mapId)) {
    if (request->loc) {
      VLOG(1) << "Revive for push data success, but the mapper already ended "
                 "for shuffle "
              << shuffleId << " map " << request->mapId << " attempt "
              << request->attemptId << " partition " << request->partitionId
              << " batch " << batchId << " location hostAndPushPort "
              << request->loc->hostAndPushPort() << ".";
      pushState->removeBatch(batchId, request->loc->hostAndPushPort());
    } else {
      VLOG(1) << "Revive for push data success, but the mapper already ended "
                 "for shuffle "
              << shuffleId << " map " << request->mapId << " attempt "
              << request->attemptId << " partition " << request->partitionId
              << " batch " << batchId << " no location available.";
    }
    return;
  }
  if (request->reviveStatus.load() != protocol::StatusCode::SUCCESS) {
    // TODO: the exception message here should be assembled.
    pushDataCallback->onFailure(std::make_unique<std::exception>());
    return;
  }
  auto locationMapOptional = partitionLocationMaps_.get(shuffleId);
  CELEBORN_CHECK(locationMapOptional.has_value());
  auto newLocationOptional =
      locationMapOptional.value()->get(request->partitionId);
  CELEBORN_CHECK(newLocationOptional.has_value());
  auto newLocation = newLocationOptional.value();
  LOG(INFO) << "Revive for push data success, new location for shuffle "
            << shuffleId << " map " << request->mapId << " attempt "
            << request->attemptId << " partition " << request->partitionId
            << " batch " << batchId << " is location hostAndPushPort "
            << newLocation->hostAndPushPort() << ".";
  pushDataCallback->updateLatestLocation(newLocation);

  try {
    CELEBORN_CHECK_GT(remainReviveTimes, 0, "no remainReviveTime left");
    network::PushData pushData(
        network::Message::nextRequestId(),
        protocol::PartitionLocation::Mode::PRIMARY,
        utils::makeShuffleKey(appUniqueId_, shuffleId),
        newLocation->uniqueId(),
        std::move(body));
    auto client = clientFactory_->createClient(
        newLocation->host, newLocation->pushPort, request->partitionId);
    client->pushDataAsync(
        pushData, conf_->clientPushDataTimeout(), pushDataCallback);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception raised while pushing data for shuffle "
               << shuffleId << " map " << request->mapId << " attempt "
               << request->attemptId << " partition " << request->partitionId
               << " batch " << batchId << " location hostAndPushPort "
               << newLocation->hostAndPushPort() << " errorMsg " << e.what()
               << ".";
    // TODO: The failure should be treated better.
    pushDataCallback->onFailure(std::make_unique<std::exception>(e));
  }
}

bool ShuffleClientImpl::checkMapperEnded(
    int shuffleId,
    int mapId,
    const std::string& mapKey) {
  if (mapperEnded(shuffleId, mapId)) {
    VLOG(1) << "Mapper already ended for shuffle " << shuffleId << " map "
            << mapId;
    if (auto pushStateOptional = pushStates_.get(mapKey);
        pushStateOptional.has_value()) {
      auto pushState = pushStateOptional.value();
      pushState->cleanup();
    }
    return true;
  }
  return false;
}

bool ShuffleClientImpl::mapperEnded(int shuffleId, int mapId) {
  if (auto mapperEndSetOptional = mapperEndSets_.get(shuffleId);
      mapperEndSetOptional.has_value() &&
      mapperEndSetOptional.value()->contains(mapId)) {
    return true;
  }
  if (stageEnded(shuffleId)) {
    return true;
  }
  return false;
}

bool ShuffleClientImpl::stageEnded(int shuffleId) {
  return stageEndShuffleSet_.contains(shuffleId);
}

void ShuffleClientImpl::addRequestToReviveManager(
    std::shared_ptr<protocol::ReviveRequest> reviveRequest) {
  reviveManager_->addRequest(std::move(reviveRequest));
}

std::optional<std::unordered_map<int, int>> ShuffleClientImpl::reviveBatch(
    int shuffleId,
    const std::unordered_set<int>& mapIds,
    const std::unordered_map<int, PtrReviveRequest>& requests) {
  std::unordered_map<int, int> result;
  auto partitionLocationMap = partitionLocationMaps_.get(shuffleId).value();
  std::unordered_map<int, std::shared_ptr<const protocol::PartitionLocation>>
      oldLocationMap;
  protocol::Revive revive;
  revive.shuffleId = shuffleId;
  revive.mapIds.insert(mapIds.begin(), mapIds.end());
  for (auto& [partitionId, request] : requests) {
    oldLocationMap[request->partitionId] = request->loc;
    revive.reviveRequests.insert(request);
  }
  try {
    auto response =
        lifecycleManagerRef_
            ->askSync<protocol::Revive, protocol::ChangeLocationResponse>(
                revive,
                conf_->clientRpcRequestPartitionLocationRpcAskTimeout());
    auto mapperEndSet = mapperEndSets_.computeIfAbsent(shuffleId, []() {
      return std::make_shared<utils::ConcurrentHashSet<int>>();
    });
    for (auto endedMapId : response->endedMapIds) {
      mapperEndSet->insert(endedMapId);
    }
    for (auto& partitionInfo : response->partitionInfos) {
      switch (partitionInfo.status) {
        case protocol::StatusCode::SUCCESS: {
          partitionLocationMap->set(
              partitionInfo.partitionId, partitionInfo.partition);
          break;
        }
        case protocol::StatusCode::STAGE_ENDED: {
          stageEndShuffleSet_.insert(shuffleId);
          return {std::move(result)};
        }
        case protocol::StatusCode::SHUFFLE_NOT_REGISTERED: {
          LOG(ERROR) << "shuffleId " << shuffleId << " not registered!";
          return std::nullopt;
        }
        default: {
          // noop
        }
      }
      result[partitionInfo.partitionId] = partitionInfo.status;
    }
    return {std::move(result)};
  } catch (std::exception& e) {
    LOG(ERROR) << "reviveBatch failed: " << e.what();
    return std::nullopt;
  }
}

bool ShuffleClientImpl::revive(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int epoch,
    std::shared_ptr<const protocol::PartitionLocation> oldLocation,
    protocol::StatusCode cause) {
  auto request = std::make_shared<protocol::ReviveRequest>(
      shuffleId, mapId, attemptId, partitionId, epoch, oldLocation, cause);
  auto resultOptional =
      reviveBatch(shuffleId, {mapId}, {{partitionId, request}});
  if (mapperEnded(shuffleId, mapId)) {
    VLOG(1) << "Revive success, but the mapper ended for shuffle " << shuffleId
            << " map " << mapId << " attempt " << attemptId << " partition"
            << partitionId << ", just return true(Assume revive successfully).";
    return true;
  }
  if (resultOptional.has_value()) {
    auto result = resultOptional.value();
    return result.find(partitionId) != result.end() &&
        result[partitionId] == protocol::StatusCode::SUCCESS;
  }
  return false;
}

void ShuffleClientImpl::limitMaxInFlight(
    const std::string& mapKey,
    PushState& pushState,
    const std::string& hostAndPushPort) {
  bool reachLimit = pushState.limitMaxInFlight(hostAndPushPort);
  if (reachLimit) {
    auto msg = fmt::format(
        "Waiting timeout for task {} while limiting max "
        "in-flight requests to {}.",
        mapKey,
        hostAndPushPort);
    if (auto exceptionMsgOptional = pushState.getExceptionMsg();
        exceptionMsgOptional.has_value()) {
      msg += " PushState exception: " + exceptionMsgOptional.value();
    }
    CELEBORN_FAIL(msg);
  }
}

void ShuffleClientImpl::limitZeroInFlight(
    const std::string& mapKey,
    PushState& pushState) {
  bool reachLimit = pushState.limitZeroInFlight();
  if (reachLimit) {
    auto msg = fmt::format(
        "Waiting timeout for task {} while limiting zero "
        "in-flight requests.",
        mapKey);
    if (auto exceptionMsgOptional = pushState.getExceptionMsg();
        exceptionMsgOptional.has_value()) {
      msg += " PushState exception: " + exceptionMsgOptional.value();
    }
    CELEBORN_FAIL(msg);
  }
}

std::optional<ShuffleClientImpl::PtrPartitionLocationMap>
ShuffleClientImpl::getPartitionLocationMap(int shuffleId) {
  return partitionLocationMaps_.get(shuffleId);
}

utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>&
ShuffleClientImpl::mapperEndSets() {
  return mapperEndSets_;
}

void ShuffleClientImpl::addPushDataRetryTask(folly::Func&& task) {
  pushDataRetryPool_->add(std::move(task));
}

bool ShuffleClientImpl::newerPartitionLocationExists(
    std::shared_ptr<utils::ConcurrentHashMap<
        int,
        std::shared_ptr<const protocol::PartitionLocation>>> locationMap,
    int partitionId,
    int epoch) {
  auto locationOptional = locationMap->get(partitionId);
  return locationOptional.has_value() &&
      locationOptional.value()->epoch > epoch;
}

std::shared_ptr<protocol::GetReducerFileGroupResponse>
ShuffleClientImpl::getReducerFileGroupInfo(int shuffleId) {
  auto reducerFileGroupInfoOptional = reducerFileGroupInfos_.get(shuffleId);
  if (reducerFileGroupInfoOptional.has_value()) {
    return reducerFileGroupInfoOptional.value();
  }

  updateReducerFileGroup(shuffleId);

  return getReducerFileGroupInfo(shuffleId);
}
} // namespace client
} // namespace celeborn
