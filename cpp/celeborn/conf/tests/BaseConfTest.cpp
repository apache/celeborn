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
#include <fstream>

#include "celeborn/conf/BaseConf.h"

using namespace celeborn::conf;

using CelebornUserError = celeborn::utils::CelebornUserError;

// As the BaseConf's construction is protected, we expose it explicitly.
class TestedBaseConf : public BaseConf {};

using ValueType = folly::Optional<std::string>;

void testInsertToConf(TestedBaseConf* conf, bool isMutable = true) {
  const std::string key1 = "key1";
  const std::string val1 = "val1";
  // Before insert.
  EXPECT_THROW(conf->requiredProperty(key1), CelebornUserError);
  EXPECT_EQ(conf->optionalProperty(key1), ValueType());

  // Insert.
  EXPECT_THROW(conf->setValue(key1, val1), CelebornUserError);
  EXPECT_TRUE(conf->registerProperty(key1, ValueType(val1)));

  // After insert.
  EXPECT_FALSE(conf->registerProperty(key1, ValueType(val1)));
  EXPECT_THROW(conf->requiredProperty(key1), CelebornUserError);
  EXPECT_EQ(conf->optionalProperty(key1), ValueType(val1));

  // Update.
  const std::string val1_1 = "val1_1";
  if (isMutable) {
    EXPECT_EQ(conf->setValue(key1, val1_1), ValueType(val1));
  } else {
    EXPECT_THROW(conf->setValue(key1, val1_1), CelebornUserError);
  }
}

void testInitedConf(
    TestedBaseConf* conf,
    bool isMutable,
    const std::string& key,
    const std::string& val) {
  // Test init config.
  EXPECT_EQ(conf->requiredProperty(key), val);
  EXPECT_EQ(conf->optionalProperty(key), ValueType(val));

  // Test insert to init config.
  const std::string val_1 = "val_1_random";
  EXPECT_THROW(conf->setValue(key, val_1), CelebornUserError);
  EXPECT_TRUE(conf->registerProperty(key, val_1));
  // The init config has highest priority.
  EXPECT_EQ(conf->optionalProperty(key), val);
  EXPECT_FALSE(conf->registerProperty(key, val_1));

  if (isMutable) {
    // Insert with setValue() would work.
    EXPECT_EQ(conf->setValue(key, val_1), val);
    EXPECT_EQ(conf->requiredProperty(key), val_1);
    EXPECT_EQ(conf->optionalProperty(key), ValueType(val_1));
  } else {
    // Insert with setValue() would not work!
    EXPECT_THROW(conf->setValue(key, val_1), CelebornUserError);
  }
}

void writeToFile(
    const std::string& filename,
    const std::vector<std::string>& lines) {
  std::ofstream file;
  file.open(filename);
  for (auto& line : lines) {
    file << line << "\n";
  }
  file.close();
}

TEST(BaseConfTest, registerToEmptyConf) {
  auto conf = std::make_unique<TestedBaseConf>();
  testInsertToConf(conf.get());
}

TEST(BaseConfTest, registerToConfInitedWithMutableConfig) {
  auto conf = std::make_unique<TestedBaseConf>();
  const std::string key0 = "key0";
  const std::string val0 = "val0";
  std::unordered_map<std::string, std::string> init;
  init[key0] = val0;
  conf->initialize(std::make_unique<core::MemConfigMutable>(init));

  // Test init.
  testInitedConf(conf.get(), true, key0, val0);

  // Test insert.
  testInsertToConf(conf.get());
}

TEST(BaseConfTest, registerToConfInitedWithImmutableConfig) {
  auto conf = std::make_unique<TestedBaseConf>();
  const std::string key0 = "key0";
  const std::string val0 = "val0";
  std::unordered_map<std::string, std::string> init;
  init[key0] = val0;
  conf->initialize(std::make_unique<core::MemConfig>(init));

  // Test init.
  testInitedConf(conf.get(), false, key0, val0);

  // Test insert.
  testInsertToConf(conf.get(), false);
}

TEST(BaseConfTest, registerToConfInitedWithMutableConfigFile) {
  auto conf = std::make_unique<TestedBaseConf>();
  std::vector<std::string> lines;
  // Lines start with # would be ignored.
  const std::string annotatedKey = "annotatedKey";
  const std::string annotatedVal = "annotatedVal";
  const std::string annotatedKv = "# " + annotatedKey + " = " + annotatedVal;
  lines.push_back(annotatedKv);
  const std::string key0 = "key0";
  const std::string val0 = "val0";
  // Spaces are allowed and will be trimmed.
  const std::string kv0 = key0 + " = " + val0;
  lines.push_back(kv0);
  // Only the first k-v declaration works. The duplicate keys inserted later
  // wouldn't work.
  const std::string val0_0 = "val0_0";
  const std::string kv0_0 = key0 + " = " + val0_0;
  lines.push_back(kv0_0);
  lines.push_back("  ");

  // Explicitly declare the config as mutable. . The default is true as well.
  lines.push_back(std::string(BaseConf::kMutableConfig) + " = true");
  const std::string filename = "/tmp/test.conf";
  writeToFile(filename, lines);

  conf->initialize(filename);

  // The annotated kv would not be recorded.
  EXPECT_THROW(conf->requiredProperty(annotatedKey), CelebornUserError);
  // Test init.
  testInitedConf(conf.get(), true, key0, val0);

  // Test insert.
  testInsertToConf(conf.get());
}

TEST(BaseConfTest, registerToConfInitedWithImmutableConfigFile) {
  auto conf = std::make_unique<TestedBaseConf>();
  std::vector<std::string> lines;
  // Lines start with # would be ignored.
  const std::string annotatedKey = "annotatedKey";
  const std::string annotatedVal = "annotatedVal";
  const std::string annotatedKv = "# " + annotatedKey + " = " + annotatedVal;
  lines.push_back(annotatedKv);
  const std::string key0 = "key0";
  const std::string val0 = "val0";
  // Spaces are allowed and will be trimmed.
  const std::string kv0 = key0 + " = " + val0;
  lines.push_back(kv0);
  // Only the first k-v declaration works. The duplicate keys inserted later
  // wouldn't work.
  const std::string val0_0 = "val0_0";
  const std::string kv0_0 = key0 + " = " + val0_0;
  lines.push_back(kv0_0);
  lines.push_back("  ");

  // Explicitly declare the config as immutable.
  lines.push_back(std::string(BaseConf::kMutableConfig) + " = false");
  const std::string filename = "/tmp/test.conf";
  writeToFile(filename, lines);

  conf->initialize(filename);

  // The annotated kv would not be recorded.
  EXPECT_THROW(conf->requiredProperty(annotatedKey), CelebornUserError);
  // Test init.
  testInitedConf(conf.get(), false, key0, val0);

  // Test insert.
  testInsertToConf(conf.get(), false);
}
