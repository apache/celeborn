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

#include <folly/experimental/FunctionScheduler.h>

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/protocol/ControlMessages.h"

namespace celeborn {
namespace client {

class ShuffleClientImpl;
/// ReviveManager is responsible for buffering the ReviveRequests, and issue
/// the revive requests periodically in batches.
class ReviveManager : public std::enable_shared_from_this<ReviveManager> {
 public:
  using PtrReviveRequest = std::shared_ptr<protocol::ReviveRequest>;

  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<ReviveManager> create(
      const std::string& name,
      const conf::CelebornConf& conf,
      std::weak_ptr<ShuffleClientImpl> weakClient);

  ~ReviveManager();

  void addRequest(PtrReviveRequest request);

 private:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  ReviveManager(
      const std::string& name,
      const conf::CelebornConf& conf,
      std::weak_ptr<ShuffleClientImpl> weakClient);

  // The start() method must not be called within constructor, because for
  // std::enable_shred_from_this the shared_from_this() or weak_from_this()
  // would not work until the object construction is complete.
  void start();

  void startFunction(std::function<void()> task);

  // Scheduler for issuing the requests periodically.
  static folly::FunctionScheduler globalExecutor_;

  std::string name_;
  const int batchSize_;
  const Timeout interval_;
  std::weak_ptr<ShuffleClientImpl> weakClient_;
  folly::Synchronized<std::queue<PtrReviveRequest>, std::mutex> requestQueue_;
  std::atomic<bool> started_{false};
};

} // namespace client
} // namespace celeborn
