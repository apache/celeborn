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

#include "celeborn/client/writer/PushState.h"

using namespace celeborn;
using namespace celeborn::client;

class PushStateTest : public testing::Test {
 protected:
  void SetUp() override {
    conf::CelebornConf conf;
    conf.registerProperty(
        conf::CelebornConf::kClientPushLimitInFlightTimeoutMs,
        std::to_string(pushTimeoutMs_));
    conf.registerProperty(
        conf::CelebornConf::kClientPushLimitInFlightSleepDeltaMs,
        std::to_string(pushSleepDeltaMs_));
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxReqsInFlightTotal,
        std::to_string(maxReqsInFlight_));
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxReqsInFlightPerWorker,
        std::to_string(maxReqsInFlight_));

    pushState_ = std::make_unique<PushState>(conf);
  }

  std::unique_ptr<PushState> pushState_;
  static constexpr int pushTimeoutMs_ = 100;
  static constexpr int pushSleepDeltaMs_ = 10;
  static constexpr int maxReqsInFlight_ = 2;
};

TEST_F(PushStateTest, limitMaxInFlight) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int addBatchCalls = maxReqsInFlight_ + 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);
  std::thread addBatchThread([&]() {
    for (auto i = 0; i < addBatchCalls; i++) {
      pushState_->addBatch(i, hostAndPushPort);
      EXPECT_FALSE(pushState_->limitMaxInFlight(hostAndPushPort));
      addBatchMarks[i] = true;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  for (auto i = 0; i < maxReqsInFlight_; i++) {
    EXPECT_TRUE(addBatchMarks[i]);
  }
  EXPECT_FALSE(addBatchMarks[maxReqsInFlight_]);

  pushState_->removeBatch(0, hostAndPushPort);
  addBatchThread.join();
  EXPECT_TRUE(addBatchMarks[maxReqsInFlight_]);
}

TEST_F(PushStateTest, limitMaxInFlightTimeout) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int addBatchCalls = maxReqsInFlight_ + 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);
  std::thread addBatchThread([&]() {
    for (auto i = 0; i < addBatchCalls; i++) {
      pushState_->addBatch(i, hostAndPushPort);
      auto result = pushState_->limitMaxInFlight(hostAndPushPort);
      if (i < maxReqsInFlight_) {
        EXPECT_FALSE(result);
      } else {
        EXPECT_TRUE(result);
      }
      addBatchMarks[i] = !result;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  for (auto i = 0; i < maxReqsInFlight_; i++) {
    EXPECT_TRUE(addBatchMarks[i]);
  }
  EXPECT_FALSE(addBatchMarks[maxReqsInFlight_]);

  addBatchThread.join();
  EXPECT_FALSE(addBatchMarks[maxReqsInFlight_]);
}

TEST_F(PushStateTest, limitZeroInFlight) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int addBatchCalls = 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);
  std::thread addBatchThread([&]() {
    pushState_->addBatch(0, hostAndPushPort);
    EXPECT_FALSE(pushState_->limitZeroInFlight());
    addBatchMarks[0] = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  EXPECT_FALSE(addBatchMarks[0]);

  pushState_->removeBatch(0, hostAndPushPort);
  addBatchThread.join();
  EXPECT_TRUE(addBatchMarks[0]);
}

TEST_F(PushStateTest, limitZeroInFlightTimeout) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int addBatchCalls = 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);
  std::thread addBatchThread([&]() {
    pushState_->addBatch(0, hostAndPushPort);
    auto result = pushState_->limitZeroInFlight();
    EXPECT_TRUE(result);
    addBatchMarks[0] = !result;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  EXPECT_FALSE(addBatchMarks[0]);

  addBatchThread.join();
  EXPECT_FALSE(addBatchMarks[0]);
}

TEST_F(PushStateTest, throwException) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  pushState_->setException(std::make_unique<std::exception>());
  bool exceptionThrowed = false;
  try {
    pushState_->limitMaxInFlight(hostAndPushPort);
  } catch (...) {
    exceptionThrowed = true;
  }
  EXPECT_TRUE(exceptionThrowed);

  exceptionThrowed = false;
  try {
    pushState_->limitZeroInFlight();
  } catch (...) {
    exceptionThrowed = true;
  }
  EXPECT_TRUE(exceptionThrowed);
}
