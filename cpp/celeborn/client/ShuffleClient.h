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

#include <functional>
#include "celeborn/client/compress/Compressor.h"
#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/client/writer/DataBatches.h"
#include "celeborn/client/writer/PushDataCallback.h"
#include "celeborn/client/writer/PushState.h"
#include "celeborn/client/writer/ReviveManager.h"
#include "celeborn/network/NettyRpcEndpointRef.h"

namespace celeborn {
namespace client {
class ShuffleClient {
 public:
  virtual void setupLifecycleManagerRef(std::string& host, int port) = 0;

  virtual void setupLifecycleManagerRef(
      std::shared_ptr<network::NettyRpcEndpointRef>& lifecycleManagerRef) = 0;

  virtual int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) = 0;

  virtual int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) = 0;

  virtual void pushMergedData(int shuffleId, int mapId, int attemptId) = 0;

  virtual void
  mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers) = 0;

  // Cleanup states of a map task.
  virtual void cleanup(int shuffleId, int mapId, int attemptId) = 0;

  virtual void updateReducerFileGroup(int shuffleId) = 0;

  using FetchExcludedWorkers = utils::ConcurrentHashMap<std::string, int64_t>;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) = 0;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) = 0;

  virtual void excludeFailedFetchLocation(
      const std::string& hostAndFetchPort,
      const std::exception& e) = 0;

  virtual bool cleanupShuffle(int shuffleId) = 0;

  virtual void shutdown() = 0;
};

class ReviveManager;
class PushDataCallback;
class PushMergedDataCallback;

/// ShuffleClientEndpoint holds all the resources of ShuffleClient, including
/// threadPools and clientFactories. The endpoint could be reused by multiple
/// ShuffleClient to avoid creating too many resources.
class ShuffleClientEndpoint {
 public:
  ShuffleClientEndpoint(const std::shared_ptr<const conf::CelebornConf>& conf);

  std::shared_ptr<folly::IOThreadPoolExecutor> pushDataRetryPool() const;

  std::shared_ptr<network::TransportClientFactory> clientFactory() const;

 private:
  const std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<folly::IOThreadPoolExecutor> pushDataRetryPool_;
  std::shared_ptr<network::TransportClientFactory> clientFactory_;
};

class ShuffleClientImpl
    : public ShuffleClient,
      public std::enable_shared_from_this<ShuffleClientImpl> {
 public:
  friend class ReviveManager;
  friend class PushDataCallback;
  friend class PushMergedDataCallback;

  using PtrReviveRequest = std::shared_ptr<protocol::ReviveRequest>;
  using PartitionLocationMap = utils::ConcurrentHashMap<
      int,
      std::shared_ptr<const protocol::PartitionLocation>>;
  using PtrPartitionLocationMap = std::shared_ptr<PartitionLocationMap>;

  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<ShuffleClientImpl> create(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const ShuffleClientEndpoint& clientEndpoint);

  void setupLifecycleManagerRef(std::string& host, int port) override;

  void setupLifecycleManagerRef(std::shared_ptr<network::NettyRpcEndpointRef>&
                                    lifecycleManagerRef) override;

  std::shared_ptr<utils::ConcurrentHashMap<
      int,
      std::shared_ptr<const protocol::PartitionLocation>>>
  getPartitionLocation(int shuffleId, int numMappers, int numPartitions);

  int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) override;

  int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions) override;

  void pushMergedData(int shuffleId, int mapId, int attemptId) override;

  void mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers)
      override;

  void mapPartitionMapperEnd(
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int partitionId);

  void cleanup(int shuffleId, int mapId, int attemptId) override;

  std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) override;

  std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) override;

  void excludeFailedFetchLocation(
      const std::string& hostAndFetchPort,
      const std::exception& e) override;

  void updateReducerFileGroup(int shuffleId) override;

  bool cleanupShuffle(int shuffleId) override;

  void shutdown() override;

 protected:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  ShuffleClientImpl(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const ShuffleClientEndpoint& clientEndpoint);

  virtual void submitRetryPushData(
      int shuffleId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body,
      int batchId,
      std::shared_ptr<PushDataCallback> pushDataCallback,
      std::shared_ptr<PushState> pushState,
      PtrReviveRequest request,
      int remainReviveTimes,
      long dueTimeMs);

  virtual bool mapperEnded(int shuffleId, int mapId);

  virtual void addRequestToReviveManager(
      std::shared_ptr<protocol::ReviveRequest> reviveRequest);

  virtual std::optional<std::unordered_map<int, int>> reviveBatch(
      int shuffleId,
      const std::unordered_set<int>& mapIds,
      const std::unordered_map<int, PtrReviveRequest>& requests);

  virtual std::optional<PtrPartitionLocationMap> getPartitionLocationMap(
      int shuffleId);

  virtual utils::
      ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>&
      mapperEndSets();

  virtual void addPushDataRetryTask(folly::Func&& task);

  virtual void submitRetryPushMergedData(
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      const std::string& mapKey,
      std::vector<DataBatch> batches,
      std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests,
      // Failure cause carried through retries so re-revive and worker
      // exclusion behave like the Java client.
      protocol::StatusCode cause,
      int oldGroupedBatchId,
      std::shared_ptr<PushState> pushState,
      int remainReviveTimes,
      long reviveResponseDueTimeMs);

  // These push-failure / worker-exclusion helpers are pure logic, kept
  // protected so a subclass can unit-test them.

  // Derive a specific push-data failure StatusCode from the transport error
  // message (mirrors the Java getPushDataFailCause). Defaults to
  // PUSH_DATA_FAIL_NON_CRITICAL_CAUSE (primary) when unknown.
  static protocol::StatusCode getPushDataFailCause(const std::string& message);

  // Shared onFailure tail for the push callbacks: classifies errorMsg into a
  // cause. With no revive attempts left, sets the terminal exception on
  // pushState ("<cause>: <errorMsg>", like Java) and returns std::nullopt;
  // otherwise returns the cause for the revive/retry.
  static std::optional<protocol::StatusCode> classifyPushFailure(
      const std::string& errorMsg,
      int remainingReviveTimes,
      PushState& pushState);

  // Exclude the worker owning oldLocation on a connection/timeout cause when
  // clientPushExcludeWorkerOnFailureEnabled is on. No-op otherwise.
  void excludeWorkerByCause(
      protocol::StatusCode cause,
      const std::shared_ptr<const protocol::PartitionLocation>& oldLocation);

  // If location (or its peer) targets an excluded worker, returns the matching
  // PUSH_DATA_*_WORKER_EXCLUDED cause, else std::nullopt (always std::nullopt
  // when clientPushExcludeWorkerOnFailureEnabled is off).
  std::optional<protocol::StatusCode> getPushTargetWorkerExcludeCause(
      const protocol::PartitionLocation& location);

 private:
  std::shared_ptr<PushState> getPushState(const std::string& mapKey);

  // Result of the shared pushData/mergeData prologue. The batch size is
  // body->remainingSize() (16-byte header + payload), validated to fit in int.
  struct PreparedBatch {
    std::string mapKey;
    std::shared_ptr<const protocol::PartitionLocation> partitionLocation;
    std::shared_ptr<PushState> pushState;
    int batchId;
    std::unique_ptr<memory::ReadOnlyByteBuffer> body;
  };

  // Runs the prologue shared by pushData and mergeData: short-circuits ended
  // mappers, resolves (reviving if needed) the partition location, allocates
  // the batchId, compresses the payload, and frames the batch body (16-byte
  // header + data). Returns std::nullopt when the mapper already ended (the
  // caller then returns 0).
  std::optional<PreparedBatch> prepareBatch(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      const uint8_t* data,
      size_t offset,
      size_t length,
      int numMappers,
      int numPartitions);

  // Shared by pushData/doPushMergedData/submitRetryPushData: if the target
  // worker (or its peer) is excluded, fails `callback` with that cause;
  // otherwise runs `doPush` under a try/catch so a synchronous failure routes
  // through the callback's failure path, not leaking the in-flight batch.
  template <typename DoPush, typename LogContext>
  void pushWithFailureRouting(
      const protocol::PartitionLocation& location,
      network::RpcResponseCallback& callback,
      DoPush&& doPush,
      LogContext&& logContext);

  void initReviveManagerLocked();

  void registerShuffle(int shuffleId, int numMappers, int numPartitions);

  bool checkMapperEnded(int shuffleId, int mapId, const std::string& mapKey);

  bool stageEnded(int shuffleId);

  bool revive(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      std::shared_ptr<const protocol::PartitionLocation> oldLocation,
      protocol::StatusCode cause);

  // Check if the pushState's ongoing package num reaches the max limit, if so,
  // block until the ongoing package num decreases below max limit.
  void limitMaxInFlight(
      const std::string& mapKey,
      PushState& pushState,
      const std::string& hostAndPushPort);

  // Check if the pushState's ongoing package num reaches zero, if not, block
  // until the ongoing package num decreases to zero.
  void limitZeroInFlight(const std::string& mapKey, PushState& pushState);

  void doPushMergedData(
      const std::string& hostAndPushPort,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      const std::string& mapKey,
      std::vector<DataBatch> batches,
      std::shared_ptr<PushState> pushState,
      int remainReviveTimes);

  static std::string genAddressPairKey(const protocol::PartitionLocation& loc);

  // TODO: no support for WAIT as it is not used.
  static bool newerPartitionLocationExists(
      std::shared_ptr<utils::ConcurrentHashMap<
          int,
          std::shared_ptr<const protocol::PartitionLocation>>> locationMap,
      int partitionId,
      int epoch);

  std::shared_ptr<protocol::GetReducerFileGroupResponse>
  getReducerFileGroupInfo(int shuffleId);

  static constexpr size_t kBatchHeaderSize = 4 * 4;

  const std::string appUniqueId_;
  const bool shuffleCompressionEnabled_;
  std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<network::NettyRpcEndpointRef> lifecycleManagerRef_;
  std::shared_ptr<network::TransportClientFactory> clientFactory_;
  std::shared_ptr<folly::IOExecutor> pushDataRetryPool_;
  std::shared_ptr<ReviveManager> reviveManager_;
  std::mutex mutex_;
  utils::ConcurrentHashMap<int, std::shared_ptr<std::mutex>> shuffleMutexes_;
  utils::ConcurrentHashMap<
      int,
      std::shared_ptr<protocol::GetReducerFileGroupResponse>>
      reducerFileGroupInfos_;
  utils::ConcurrentHashMap<int, PtrPartitionLocationMap> partitionLocationMaps_;
  utils::ConcurrentHashMap<std::string, std::shared_ptr<PushState>> pushStates_;
  utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>
      mapperEndSets_;
  utils::ConcurrentHashSet<int> stageEndShuffleSet_;

  // Factory for creating compressor instances on demand to avoid sharing a
  // single non-thread-safe compressor across concurrent operations.
  std::function<std::unique_ptr<compress::Compressor>()> compressorFactory_;
  bool pushReplicateEnabled_;
  bool fetchExcludeWorkerOnFailureEnabled_;
  std::shared_ptr<FetchExcludedWorkers> fetchExcludedWorkers_;

  // Exclude workers on push failure (connection/timeout causes) and skip them
  // on later pushes until a successful revive moves away.
  const bool pushExcludeWorkerOnFailureEnabled_;
  // Track failed batches and report them at mapperEnd for the adaptive
  // skewed-partition read optimization (Java's dataPushFailureTrackingEnabled).
  const bool dataPushFailureTrackingEnabled_;
  // hostAndPushPort of push-excluded workers. Empty when
  // pushExcludeWorkerOnFailureEnabled_ is false.
  utils::ConcurrentHashSet<std::string> pushExcludedWorkers_;
};
} // namespace client
} // namespace celeborn
