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
  static constexpr int defaultBatchSize_ = 1024;
};

class PushStateBytesSizeTest : public testing::Test {
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
        conf::CelebornConf::kClientPushMaxReqsInFlightTotal, "2");
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxReqsInFlightPerWorker, "100");
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxBytesSizeInFlightEnabled, "true");
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxBytesSizeInFlightTotal,
        std::to_string(maxBytesSizeTotal_) + "B");
    conf.registerProperty(
        conf::CelebornConf::kClientPushMaxBytesSizeInFlightPerWorker,
        std::to_string(maxBytesSizePerWorker_) + "B");
    conf.registerProperty(
        conf::CelebornConf::kClientPushBufferMaxSize,
        std::to_string(bufferMaxSize_) + "B");

    pushState_ = std::make_unique<PushState>(conf);
  }

  std::unique_ptr<PushState> pushState_;
  static constexpr int pushTimeoutMs_ = 100;
  static constexpr int pushSleepDeltaMs_ = 10;
  static constexpr int batchSize_ = 1024;
  static constexpr long maxBytesSizeTotal_ = 3000;
  static constexpr long maxBytesSizePerWorker_ = 2500;
  static constexpr int bufferMaxSize_ = 65536;
};

TEST_F(PushStateTest, limitMaxInFlight) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int addBatchCalls = maxReqsInFlight_ + 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);
  std::thread addBatchThread([&]() {
    for (auto i = 0; i < addBatchCalls; i++) {
      pushState_->addBatch(i, defaultBatchSize_, hostAndPushPort);
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
      pushState_->addBatch(i, defaultBatchSize_, hostAndPushPort);
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
    pushState_->addBatch(0, defaultBatchSize_, hostAndPushPort);
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
    pushState_->addBatch(0, defaultBatchSize_, hostAndPushPort);
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

TEST_F(PushStateBytesSizeTest, limitMaxInFlightByBytesSize) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";
  const int expectedAllowedBatches = 2;
  const int addBatchCalls = expectedAllowedBatches + 1;
  std::vector<bool> addBatchMarks(addBatchCalls, false);

  std::thread addBatchThread([&]() {
    for (auto i = 0; i < addBatchCalls; i++) {
      pushState_->addBatch(i, batchSize_, hostAndPushPort);
      auto result = pushState_->limitMaxInFlight(hostAndPushPort);
      addBatchMarks[i] = true;
      if (i < expectedAllowedBatches) {
        EXPECT_FALSE(result) << "Batch " << i << " should be within limits";
      }
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  for (auto i = 0; i < expectedAllowedBatches; i++) {
    EXPECT_TRUE(addBatchMarks[i]) << "Batch " << i << " should have completed";
  }

  pushState_->removeBatch(0, hostAndPushPort);
  addBatchThread.join();
  EXPECT_TRUE(addBatchMarks[expectedAllowedBatches]);
}

TEST_F(PushStateBytesSizeTest, limitMaxInFlightByTotalBytesSize) {
  const std::string hostAndPushPort1 = "xx.xx.xx.xx:8080";
  const std::string hostAndPushPort2 = "yy.yy.yy.yy:8080";

  pushState_->addBatch(0, batchSize_, hostAndPushPort1);
  EXPECT_FALSE(pushState_->limitMaxInFlight(hostAndPushPort1));

  pushState_->addBatch(1, batchSize_, hostAndPushPort2);
  EXPECT_FALSE(pushState_->limitMaxInFlight(hostAndPushPort2));

  std::atomic<bool> thirdBatchCompleted{false};
  std::thread addBatchThread([&]() {
    pushState_->addBatch(2, batchSize_, hostAndPushPort1);
    pushState_->limitMaxInFlight(hostAndPushPort1);
    thirdBatchCompleted = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(pushSleepDeltaMs_));
  EXPECT_FALSE(thirdBatchCompleted.load())
      << "Third batch should be blocked due to total bytes limit";

  pushState_->removeBatch(0, hostAndPushPort1);
  addBatchThread.join();

  EXPECT_TRUE(thirdBatchCompleted.load());
}

TEST_F(PushStateBytesSizeTest, cleanupClearsBytesSizeTracking) {
  const std::string hostAndPushPort = "xx.xx.xx.xx:8080";

  pushState_->addBatch(0, batchSize_, hostAndPushPort);
  pushState_->addBatch(1, batchSize_, hostAndPushPort);
  pushState_->cleanup();

  EXPECT_FALSE(pushState_->limitMaxInFlight(hostAndPushPort));
}
