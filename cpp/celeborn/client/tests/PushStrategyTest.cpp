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

#include "celeborn/client/writer/PushStrategy.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
std::unique_ptr<SlowStartPushStrategy> createSlowStartStrategy(
    int maxReqsInFlightPerWorker,
    const std::string& maxSleepTime) {
  conf::CelebornConf conf;
  conf.registerProperty(
      conf::CelebornConf::kClientPushMaxReqsInFlightPerWorker,
      std::to_string(maxReqsInFlightPerWorker));
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitStrategy, "slowstart");
  conf.registerProperty(
      conf::CelebornConf::kClientPushSlowStartMaxSleepTime, maxSleepTime);
  auto strategy = PushStrategy::create(conf);
  EXPECT_NE(dynamic_cast<SlowStartPushStrategy*>(strategy.get()), nullptr);
  return std::unique_ptr<SlowStartPushStrategy>(
      dynamic_cast<SlowStartPushStrategy*>(strategy.release()));
}
} // namespace

TEST(PushStrategyTest, createStrategy) {
  conf::CelebornConf conf;
  // Default strategy is SIMPLE.
  EXPECT_NE(
      dynamic_cast<SimplePushStrategy*>(PushStrategy::create(conf).get()),
      nullptr);

  // The strategy name is case-insensitive, as the Java config entry
  // uppercases the configured value.
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitStrategy, "slowstart");
  EXPECT_NE(
      dynamic_cast<SlowStartPushStrategy*>(PushStrategy::create(conf).get()),
      nullptr);

  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitStrategy, "UNKNOWN");
  EXPECT_THROW(PushStrategy::create(conf), std::exception);
}

TEST(PushStrategyTest, sleepTime) {
  auto strategy = createSlowStartStrategy(32, "3s");
  const std::string dummyHostPort = "test:9087";
  auto context = strategy->getCongestControlContextByAddress(dummyHostPort);

  // If the currentReq is 1, should sleep 440 ms
  EXPECT_EQ(440, strategy->getSleepTime(*context));

  // If the currentReq is 8, should sleep 20 ms
  for (int i = 0; i < 7; i++) {
    strategy->onSuccess(dummyHostPort);
  }
  EXPECT_EQ(20, strategy->getSleepTime(*context));

  // If the currentReq is 16, should sleep 0 ms
  for (int i = 0; i < 8; i++) {
    strategy->onSuccess(dummyHostPort);
  }
  EXPECT_EQ(0, strategy->getSleepTime(*context));

  // Congest the requests, the currentReq reduced to 8, should sleep 20 ms
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(20, strategy->getSleepTime(*context));

  // Congest the requests, the currentReq reduced to 4, should sleep 260 ms
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(260, strategy->getSleepTime(*context));
  // Congest the requests, the currentReq reduced to 2, should sleep 380 ms
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(380, strategy->getSleepTime(*context));
  // Congest the requests, the currentReq reduced to 1, should sleep 440 ms
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(440, strategy->getSleepTime(*context));
  // Keep congest the requests even the currentReq reduced to 1, will increase
  // the sleep time 1s
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(1440, strategy->getSleepTime(*context));
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(2440, strategy->getSleepTime(*context));

  // Cannot exceed the max sleep time
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(3000, strategy->getSleepTime(*context));

  // If start to return success, the currentReq is increased to 2, should
  // sleep 380 ms
  strategy->onSuccess(dummyHostPort);
  EXPECT_EQ(380, strategy->getSleepTime(*context));
}

TEST(PushStrategyTest, congestStrategy) {
  auto strategy = createSlowStartStrategy(5, "4s");
  const std::string dummyHostPort = "test:9087";
  // Slow start, should exponentially increase the currentReq
  strategy->onSuccess(dummyHostPort);
  EXPECT_EQ(2, strategy->getCurrentMaxReqsInFlight(dummyHostPort));
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  EXPECT_EQ(4, strategy->getCurrentMaxReqsInFlight(dummyHostPort));
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);

  // Will linearly increase the currentReq if meet the maxReqsInFlight
  EXPECT_EQ(5, strategy->getCurrentMaxReqsInFlight(dummyHostPort));
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  strategy->onSuccess(dummyHostPort);
  EXPECT_EQ(6, strategy->getCurrentMaxReqsInFlight(dummyHostPort));

  // Congest controlled, should half the currentReq
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(3, strategy->getCurrentMaxReqsInFlight(dummyHostPort));
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(1, strategy->getCurrentMaxReqsInFlight(dummyHostPort));

  // Cannot lower than 1
  strategy->onCongestControl(dummyHostPort);
  EXPECT_EQ(1, strategy->getCurrentMaxReqsInFlight(dummyHostPort));
}

TEST(PushStrategyTest, multiHosts) {
  auto strategy = createSlowStartStrategy(3, "3s");
  const std::string dummyHostPort1 = "test1:9087";
  const std::string dummyHostPort2 = "test2:9087";
  auto context1 = strategy->getCongestControlContextByAddress(dummyHostPort1);
  auto context2 = strategy->getCongestControlContextByAddress(dummyHostPort2);

  EXPECT_EQ(440, strategy->getSleepTime(*context1));
  EXPECT_EQ(440, strategy->getSleepTime(*context2));

  // Control the dummyHostPort1, should not affect dummyHostPort2
  for (int i = 0; i < 3; i++) {
    strategy->onSuccess(dummyHostPort1);
  }
  EXPECT_EQ(0, strategy->getSleepTime(*context1));
  EXPECT_EQ(440, strategy->getSleepTime(*context2));
}
