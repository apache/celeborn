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

#include <atomic>
#include <optional>

#include "celeborn/client/writer/PushStrategy.h"
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace client {

class PushStrategy;

/// Records the states of a mapKey, including the ongoing package number id, the
/// exception, etc. Besides, the congestionControl is also enforced.
class PushState {
 public:
  PushState(const conf::CelebornConf& conf);

  int nextBatchId();

  void
  addBatch(int batchId, int batchBytesSize, const std::string& hostAndPushPort);

  void onSuccess(const std::string& hostAndPushPort);

  void onCongestControl(const std::string& hostAndPushPort);

  void removeBatch(int batchId, const std::string& hostAndPushPort);

  // Check if the host's ongoing package num reaches the max limit, if so,
  // block until the ongoing package num decreases below max limit. If the
  // limit operation succeeds before timeout, return false, otherwise return
  // true.
  // When maxBytesSizeInFlight is enabled, the limit check considers both
  // request count and byte size limits. The push is allowed if either:
  // 1. Request count is within limits, or
  // 2. Byte size is within limits (when enabled)
  bool limitMaxInFlight(const std::string& hostAndPushPort);

  // Check if the pushState's ongoing package num reaches zero, if not, block
  // until the ongoing package num decreases to zero. If the limit operation
  // succeeds before timeout, return false, otherwise return true.
  bool limitZeroInFlight();

  bool exceptionExists() const;

  void setException(std::unique_ptr<std::exception> exception);

  std::optional<std::string> getExceptionMsg() const;

  void cleanup();

 private:
  void throwIfExceptionExists();

  std::atomic<int> currBatchId_{1};
  std::atomic<long> totalInflightReqs_{0};
  std::atomic<long> totalInflightBytes_{0};
  const long waitInflightTimeoutMs_;
  const long deltaMs_;
  const std::unique_ptr<PushStrategy> pushStrategy_;
  const int maxInFlightReqsTotal_;
  const bool maxInFlightBytesSizeEnabled_;
  const long maxInFlightBytesSizeTotal_;
  const long maxInFlightBytesSizePerWorker_;
  utils::ConcurrentHashMap<
      std::string,
      std::shared_ptr<utils::ConcurrentHashSet<int>>>
      inflightBatchesPerAddress_;
  std::optional<
      utils::ConcurrentHashMap<std::string, std::shared_ptr<std::atomic<long>>>>
      inflightBytesSizePerAddress_;
  std::optional<utils::ConcurrentHashMap<int, int>> inflightBatchBytesSizes_;
  folly::Synchronized<std::unique_ptr<std::exception>> exception_;
  volatile bool cleaned_{false};
};

} // namespace client
} // namespace celeborn
