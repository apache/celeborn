/*
 * Based on Config.h from Facebook Velox
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fstream>
#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

#include "celeborn/conf/BaseConf.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace core {

folly::Optional<std::string> MemConfig::get(const std::string& key) const {
  folly::Optional<std::string> val;
  auto it = values_.find(key);
  if (it != values_.end()) {
    val = it->second;
  }
  return val;
}

bool MemConfig::isValueExists(const std::string& key) const {
  return values_.find(key) != values_.end();
}

folly::Optional<std::string> MemConfigMutable::get(
    const std::string& key) const {
  auto lockedValues = values_.rlock();
  folly::Optional<std::string> val;
  auto it = lockedValues->find(key);
  if (it != lockedValues->end()) {
    val = it->second;
  }
  return val;
}

bool MemConfigMutable::isValueExists(const std::string& key) const {
  auto lockedValues = values_.rlock();
  return lockedValues->find(key) != lockedValues->end();
}

} // namespace core

namespace util {

std::unordered_map<std::string, std::string> readConfig(
    const std::string& filePath) {
  // https://teradata.github.io/presto/docs/141t/configuration/configuration.html

  std::ifstream configFile(filePath);
  if (!configFile.is_open()) {
    CELEBORN_USER_FAIL("Couldn't open config file {} for reading.", filePath);
  }

  std::unordered_map<std::string, std::string> properties;
  std::string line;
  while (getline(configFile, line)) {
    line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
    if (line[0] == '#' || line.empty()) {
      continue;
    }
    auto delimiterPos = line.find('=');
    auto name = line.substr(0, delimiterPos);
    auto value = line.substr(delimiterPos + 1);
    properties.emplace(name, value);
  }

  return properties;
}

std::string requiredProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    CELEBORN_USER_FAIL("Missing configuration property {}", name);
  }
  return it->second;
}

std::string requiredProperty(
    const Config& properties,
    const std::string& name) {
  auto value = properties.get(name);
  if (!value.hasValue()) {
    CELEBORN_USER_FAIL("Missing configuration property {}", name);
  }
  return value.value();
}

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::function<std::string()>& func) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return func();
  }
  return it->second;
}

std::string getOptionalProperty(
    const std::unordered_map<std::string, std::string>& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return defaultValue;
  }
  return it->second;
}

std::string getOptionalProperty(
    const Config& properties,
    const std::string& name,
    const std::string& defaultValue) {
  auto value = properties.get(name);
  if (!value.hasValue()) {
    return defaultValue;
  }
  return value.value();
}

} // namespace util

BaseConf::BaseConf() : config_(std::make_unique<core::MemConfigMutable>()) {}

void BaseConf::initialize(const std::string& filePath) {
  // See if we want to create a mutable config.
  auto values = util::readConfig(fs::path(filePath));
  filePath_ = filePath;
  checkRegisteredProperties(values);

  bool mutableConfig{false};
  auto it = values.find(std::string(kMutableConfig));
  if (it != values.end()) {
    mutableConfig = folly::to<bool>(it->second);
  }

  if (mutableConfig) {
    config_ = std::make_unique<core::MemConfigMutable>(values);
    CELEBORN_STARTUP_LOG(INFO) << "System Config is MemConfigMutable.";
  } else {
    config_ = std::make_unique<core::MemConfig>(values);
    CELEBORN_STARTUP_LOG(INFO) << "System Config is MemConfig.";
  }
}

bool BaseConf::registerProperty(
    const std::string& propertyName,
    const folly::Optional<std::string>& defaultValue) {
  if (registeredProps_.count(propertyName) != 0) {
    CELEBORN_STARTUP_LOG(WARNING)
        << "Property '" << propertyName
        << "' is already registered with default value '"
        << registeredProps_[propertyName].value_or("<none>") << "'.";
    return false;
  }

  registeredProps_[propertyName] = defaultValue;
  return true;
}

folly::Optional<std::string> BaseConf::setValue(
    const std::string& propertyName,
    const std::string& value) {
  CELEBORN_USER_CHECK_EQ(
      1,
      registeredProps_.count(propertyName),
      "Property '{}' is not registered in the config.",
      propertyName);
  if (auto* memConfig = dynamic_cast<core::MemConfigMutable*>(config_.get())) {
    auto oldValue = config_->get(propertyName);
    memConfig->setValue(propertyName, value);
    if (oldValue.hasValue()) {
      return oldValue;
    }
    return registeredProps_[propertyName];
  }
  CELEBORN_USER_FAIL(
      "Config is not mutable. Consider setting '{}' to 'true'.",
      kMutableConfig);
}

void BaseConf::checkRegisteredProperties(
    const std::unordered_map<std::string, std::string>& values) {
  std::stringstream supported;
  std::stringstream unsupported;
  for (const auto& pair : values) {
    ((registeredProps_.count(pair.first) != 0) ? supported : unsupported)
        << "  " << pair.first << "=" << pair.second << "\n";
  }
  auto str = supported.str();
  if (!str.empty()) {
    CELEBORN_STARTUP_LOG(INFO)
        << "Registered properties from '" << filePath_ << "':\n"
        << str;
  }
  str = unsupported.str();
  if (!str.empty()) {
    CELEBORN_STARTUP_LOG(WARNING)
        << "Unregistered properties from '" << filePath_ << "':\n"
        << str;
  }
}
} // namespace celeborn
