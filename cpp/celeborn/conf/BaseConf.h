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

#pragma once

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/SocketAddress.h>
#include <folly/Synchronized.h>
#include <set>
#include <string>
#include <typeindex>
#include <unordered_map>

#include "celeborn/utils/Exceptions.h"

namespace celeborn {

class Config {
 public:
  virtual ~Config() = default;

  virtual folly::Optional<std::string> get(const std::string& key) const = 0;
  // virtual const string operator[](const std::string& key) = 0;
  // overload and disable not supported cases.

  template <typename T>
  folly::Optional<T> get(const std::string& key) const {
    auto val = get(key);
    if (val.hasValue()) {
      return folly::to<T>(val.value());
    } else {
      return folly::none;
    }
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    auto val = get(key);
    if (val.hasValue()) {
      return folly::to<T>(val.value());
    } else {
      return defaultValue;
    }
  }

  virtual bool isValueExists(const std::string& key) const = 0;

  virtual const std::unordered_map<std::string, std::string>& values() const {
    CELEBORN_UNSUPPORTED("method values() is not supported by this config");
  }

  virtual std::unordered_map<std::string, std::string> valuesCopy() const {
    CELEBORN_UNSUPPORTED("method valuesCopy() is not supported by this config");
  }
};

namespace core {

class MemConfig : public Config {
 public:
  explicit MemConfig(const std::unordered_map<std::string, std::string>& values)
      : values_(values) {}

  explicit MemConfig() : values_{} {}

  explicit MemConfig(std::unordered_map<std::string, std::string>&& values)
      : values_(std::move(values)) {}

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  const std::unordered_map<std::string, std::string>& values() const override {
    return values_;
  }

  std::unordered_map<std::string, std::string> valuesCopy() const override {
    return values_;
  }

 private:
  std::unordered_map<std::string, std::string> values_;
};

/// In-memory config allowing changing properties at runtime.
class MemConfigMutable : public Config {
 public:
  explicit MemConfigMutable(
      const std::unordered_map<std::string, std::string>& values)
      : values_(values) {}

  explicit MemConfigMutable() : values_{} {}

  explicit MemConfigMutable(
      std::unordered_map<std::string, std::string>&& values)
      : values_(std::move(values)) {}

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  const std::unordered_map<std::string, std::string>& values() const override {
    CELEBORN_UNSUPPORTED(
        "Mutable config cannot return unprotected reference to values.");
    return *values_.rlock();
  }

  std::unordered_map<std::string, std::string> valuesCopy() const override {
    return *values_.rlock();
  }

  /// Adds or replaces value at the given key. Can be used by debugging or
  /// testing code.
  void setValue(const std::string& key, const std::string& value) {
    (*values_.wlock())[key] = value;
  }

 private:
  folly::Synchronized<std::unordered_map<std::string, std::string>> values_;
};

} // namespace core

class BaseConf {
 public:
  // Setting this to 'true' makes configs modifiable via server operations.
  static constexpr std::string_view kMutableConfig{"mutable-config"};

  /// Reads configuration properties from the specified file. Must be called
  /// before calling any of the getters below.
  /// @param filePath Path to configuration file.
  void initialize(const std::string& filePath);

  /// Uses a config object already materialized.
  void initialize(std::unique_ptr<Config>&& config) {
    config_ = std::move(config);
  }

  /// Registers an extra property in the config.
  /// Returns true if succeeded, false if failed (due to the property already
  /// registered).
  bool registerProperty(
      const std::string& propertyName,
      const folly::Optional<std::string>& defaultValue = {});

  /// Adds or replaces value at the given key. Can be used by debugging or
  /// testing code.
  /// Returns previous value if there was any.
  folly::Optional<std::string> setValue(
      const std::string& propertyName,
      const std::string& value);

  /// Returns a required value of the requested type. Fails if no value found.
  template <typename T>
  T requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get<T>(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      CELEBORN_USER_FAIL(
          "{} is required in the {} file.", propertyName, filePath_);
    }
  }

  /// Returns a required value of the requested type. Fails if no value found.
  template <typename T>
  T requiredProperty(std::string_view propertyName) const {
    return requiredProperty<T>(std::string{propertyName});
  }

  /// Returns a required value of the string type. Fails if no value found.
  std::string requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      CELEBORN_USER_FAIL(
          "{} is required in the {} file.", propertyName, filePath_);
    }
  }

  /// Returns a required value of the requested type. Fails if no value found.
  std::string requiredProperty(std::string_view propertyName) const {
    return requiredProperty(std::string{propertyName});
  }

  /// Returns optional value of the requested type. Can return folly::none.
  template <typename T>
  folly::Optional<T> optionalProperty(const std::string& propertyName) const {
    auto val = config_->get(propertyName);
    if (!val.hasValue()) {
      const auto it = registeredProps_.find(propertyName);
      if (it != registeredProps_.end()) {
        val = it->second;
      }
    }
    if (val.hasValue()) {
      return folly::to<T>(val.value());
    }
    return folly::none;
  }

  /// Returns optional value of the requested type. Can return folly::none.
  template <typename T>
  folly::Optional<T> optionalProperty(std::string_view propertyName) const {
    return optionalProperty<T>(std::string{propertyName});
  }

  /// Returns optional value of the string type. Can return folly::none.
  folly::Optional<std::string> optionalProperty(
      const std::string& propertyName) const {
    auto val = config_->get(propertyName);
    if (!val.hasValue()) {
      const auto it = registeredProps_.find(propertyName);
      if (it != registeredProps_.end()) {
        return it->second;
      }
    }
    return val;
  }

  /// Returns optional value of the string type. Can return folly::none.
  folly::Optional<std::string> optionalProperty(
      std::string_view propertyName) const {
    return optionalProperty(std::string{propertyName});
  }

  /// Returns copy of the config values map.
  std::unordered_map<std::string, std::string> values() const {
    return config_->valuesCopy();
  }

 protected:
  BaseConf();

  // Check if all properties are registered.
  void checkRegisteredProperties(
      const std::unordered_map<std::string, std::string>& values);

  std::unique_ptr<Config> config_;
  std::string filePath_;
  // Map of registered properties with their default values.
  std::unordered_map<std::string, folly::Optional<std::string>>
      registeredProps_;
};

} // namespace celeborn
