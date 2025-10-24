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

#include "celeborn/client/writer/PushState.h"
#include "celeborn/conf/CelebornConf.h"

namespace celeborn {
namespace client {

class PushState;

class PushStrategy {
 public:
  static std::unique_ptr<PushStrategy> create(const conf::CelebornConf& conf);

  PushStrategy() = default;

  virtual ~PushStrategy() = default;

  virtual void onSuccess(const std::string& hostAndPushPort) = 0;

  virtual void onCongestControl(const std::string& hostAndPushPort) = 0;

  virtual void clear() = 0;

  // Control the push speed to meet the requirement.
  virtual void limitPushSpeed(
      PushState& pushState,
      const std::string& hostAndPushPort) = 0;

  virtual int getCurrentMaxReqsInFlight(const std::string& hostAndPushPort) = 0;
};

class SimplePushStrategy : public PushStrategy {
 public:
  SimplePushStrategy(const conf::CelebornConf& conf)
      : maxInFlightPerWorker_(conf.clientPushMaxReqsInFlightPerWorker()) {}

  ~SimplePushStrategy() = default;

  void onSuccess(const std::string& hostAndPushPort) override {}

  void onCongestControl(const std::string& hostAndPushPort) override {}

  void clear() override {}

  void limitPushSpeed(PushState& pushState, const std::string& hostAndPushPort)
      override {}

  int getCurrentMaxReqsInFlight(const std::string& hostAndPushPort) override {
    return maxInFlightPerWorker_;
  }

 private:
  const int maxInFlightPerWorker_;
};

// TODO: support SlowStartPushStrategy

} // namespace client
} // namespace celeborn
