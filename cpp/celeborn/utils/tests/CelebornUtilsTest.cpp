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
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "celeborn/utils/CelebornUtils.h"

using namespace celeborn::utils;

class CelebornUtilsTest : public testing::Test {
 protected:
  void SetUp() override {
    map_ = std::make_unique<ConcurrentHashMap<std::string, int>>();
  }

  std::unique_ptr<ConcurrentHashMap<std::string, int>> map_;
};

TEST_F(CelebornUtilsTest, mapBasicInsertAndRetrieve) {
  map_->set("apple", 10);
  auto result = map_->get("apple");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(10, *result);
}

TEST_F(CelebornUtilsTest, mapUpdateExistingKey) {
  map_->set("apple", 10);
  map_->set("apple", 20);
  auto result = map_->get("apple");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(20, *result);
}

TEST_F(CelebornUtilsTest, mapComputeIfAbsent) {
  map_->set("apple", 10);
  map_->computeIfAbsent("apple", []() { return 20; });
  auto result = map_->get("apple");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(10, *result);

  map_->computeIfAbsent("banana", []() { return 30; });
  map_->computeIfAbsent("banana", []() { return 40; });
  result = map_->get("banana");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(30, *result);
}

TEST_F(CelebornUtilsTest, mapRemoveKey) {
  map_->set("banana", 30);
  map_->erase("banana");
  auto result = map_->get("banana");
  EXPECT_FALSE(result.has_value());
}

TEST_F(CelebornUtilsTest, mapNonExistentKey) {
  auto result = map_->get("mango");
  EXPECT_FALSE(result.has_value());
}

TEST_F(CelebornUtilsTest, mapConcurrentInserts) {
  constexpr int NUM_THREADS = 8;
  constexpr int ITEMS_PER_THREAD = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_THREADS; ++i) {
    threads.emplace_back([this, i] {
      for (int j = 0; j < ITEMS_PER_THREAD; ++j) {
        std::string key =
            "thread" + std::to_string(i) + "-" + std::to_string(j);
        map_->set(key, j);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify all items were inserted
  for (int i = 0; i < NUM_THREADS; ++i) {
    for (int j = 0; j < ITEMS_PER_THREAD; ++j) {
      std::string key = "thread" + std::to_string(i) + "-" + std::to_string(j);
      auto result = map_->get(key);
      ASSERT_TRUE(result.has_value()) << "Missing key: " << key;
      EXPECT_EQ(j, *result);
    }
  }
}

TEST_F(CelebornUtilsTest, mapConcurrentUpdates) {
  constexpr int NUM_THREADS = 8;
  std::vector<std::thread> threads;
  std::atomic<bool> start{false};
  // Initial value
  map_->set("contended", 0);

  for (int i = 0; i < NUM_THREADS; ++i) {
    threads.emplace_back([this, &start, i] {
      while (!start) { /* spin */
      } // Wait for start signal

      for (int j = 0; j < 100; ++j) {
        map_->set("contended", i * 100 + j);
      }
    });
  }

  start = true;

  for (auto& t : threads) {
    t.join();
  }

  // Verify the final value is from the last writer
  auto result = map_->get("contended");
  ASSERT_TRUE(result.has_value());
  // The exact value depends on thread scheduling, but it should be
  // from one of the threads (between 0*100+99 and 7*100+99)
  EXPECT_GE(*result, 99);
  EXPECT_LE(*result, 799);
}

TEST_F(CelebornUtilsTest, mapConcurrentReadWrite) {
  constexpr int NUM_WRITERS = 4;
  constexpr int NUM_READERS = 4;
  std::atomic<bool> running{true};
  std::vector<std::thread> writers;
  std::vector<std::thread> readers;

  // Writers constantly update values
  for (int i = 0; i < NUM_WRITERS; ++i) {
    writers.emplace_back([this, i, &running] {
      while (running) {
        map_->set("key" + std::to_string(i), i);
      }
    });
  }

  // Readers constantly read values
  for (int i = 0; i < NUM_READERS; ++i) {
    readers.emplace_back([this, &running] {
      while (running) {
        for (int j = 0; j < NUM_WRITERS; ++j) {
          auto result = map_->get("key" + std::to_string(j));
        }
      }
    });
  }

  // Let them run for 500ms
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  running = false;

  for (auto& t : writers) {
    t.join();
  }

  for (auto& t : readers) {
    t.join();
  }

  // Verify final values are from writers
  for (int i = 0; i < NUM_WRITERS; ++i) {
    auto result = map_->get("key" + std::to_string(i));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(i, *result);
  }
}
