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

#include <re2/re2.h>

#include "celeborn/conf/CelebornConf.h"

namespace celeborn {
namespace conf {
using Duration = utils::Duration;
namespace {

// folly::to<> does not generate 'true' and 'false', so we do it ourselves.
std::string bool2String(bool value) {
  return value ? "true" : "false";
}

#define STR_PROP(_key_, _val_) \
  { std::string(_key_), std::string(_val_) }
#define NUM_PROP(_key_, _val_) \
  { std::string(_key_), folly::to<std::string>(_val_) }
#define BOOL_PROP(_key_, _val_) \
  { std::string(_key_), bool2String(_val_) }
#define NONE_PROP(_key_) \
  { std::string(_key_), folly::none }

enum class CapacityUnit {
  BYTE,
  KILOBYTE,
  MEGABYTE,
  GIGABYTE,
  TERABYTE,
  PETABYTE
};

double toBytesPerCapacityUnit(CapacityUnit unit) {
  switch (unit) {
    case CapacityUnit::BYTE:
      return 1;
    case CapacityUnit::KILOBYTE:
      return exp2(10);
    case CapacityUnit::MEGABYTE:
      return exp2(20);
    case CapacityUnit::GIGABYTE:
      return exp2(30);
    case CapacityUnit::TERABYTE:
      return exp2(40);
    case CapacityUnit::PETABYTE:
      return exp2(50);
    default:
      CELEBORN_USER_FAIL("Invalid capacity unit '{}'", (int)unit);
  }
}

CapacityUnit valueOfCapacityUnit(const std::string& unitStr) {
  if (unitStr == "B") {
    return CapacityUnit::BYTE;
  }
  if (unitStr == "kB") {
    return CapacityUnit::KILOBYTE;
  }
  if (unitStr == "MB") {
    return CapacityUnit::MEGABYTE;
  }
  if (unitStr == "GB") {
    return CapacityUnit::GIGABYTE;
  }
  if (unitStr == "TB") {
    return CapacityUnit::TERABYTE;
  }
  if (unitStr == "PB") {
    return CapacityUnit::PETABYTE;
  }
  CELEBORN_USER_FAIL("Invalid capacity unit '{}'", unitStr);
}

// Convert capacity string with unit to the capacity number in the specified
// units
uint64_t toCapacity(const std::string& from, CapacityUnit to) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$)");
  double value;
  std::string unit;
  if (!RE2::FullMatch(from, kPattern, &value, &unit)) {
    CELEBORN_USER_FAIL("Invalid capacity string '{}'", from);
  }

  return value *
      (toBytesPerCapacityUnit(valueOfCapacityUnit(unit)) /
       toBytesPerCapacityUnit(to));
}

Duration toDuration(const std::string& str) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*)");

  double value;
  std::string unit;
  if (!RE2::FullMatch(str, kPattern, &value, &unit)) {
    CELEBORN_USER_FAIL("Invalid duration {}", str);
  }
  if (unit == "ns") {
    return std::chrono::duration<double, std::nano>(value);
  } else if (unit == "us") {
    return std::chrono::duration<double, std::micro>(value);
  } else if (unit == "ms") {
    return std::chrono::duration<double, std::milli>(value);
  } else if (unit == "s") {
    return Duration(value);
  } else if (unit == "m") {
    return std::chrono::duration<double, std::ratio<60>>(value);
  } else if (unit == "h") {
    return std::chrono::duration<double, std::ratio<60 * 60>>(value);
  } else if (unit == "d") {
    return std::chrono::duration<double, std::ratio<60 * 60 * 24>>(value);
  }
  CELEBORN_USER_FAIL("Invalid duration {}", str);
}

} // namespace

const std::unordered_map<std::string, folly::Optional<std::string>>
    CelebornConf::kDefaultProperties = {
        STR_PROP(kRpcLookupTimeout, "30s"),
        STR_PROP(kClientRpcGetReducerFileGroupRpcAskTimeout, "60s"),
        STR_PROP(kNetworkConnectTimeout, "10s"),
        STR_PROP(kClientFetchTimeout, "600s"),
        NUM_PROP(kNetworkIoNumConnectionsPerPeer, "1"),
        NUM_PROP(kNetworkIoClientThreads, 0),
        NUM_PROP(kClientFetchMaxReqsInFlight, 3),
        // NUM_PROP(kNumExample, 50'000),
        // BOOL_PROP(kBoolExample, false),
};

CelebornConf::CelebornConf() {
  registeredProps_ = kDefaultProperties;
}

CelebornConf::CelebornConf(const std::string& filename) {
  initialize(filename);
  registeredProps_ = kDefaultProperties;
}

CelebornConf::CelebornConf(const CelebornConf& other) {
  if (auto* memConfig =
          dynamic_cast<core::MemConfigMutable*>(other.config_.get())) {
    config_ =
        std::make_unique<core::MemConfigMutable>(other.config_->valuesCopy());
  } else {
    config_ = std::make_unique<core::MemConfig>(other.config_->valuesCopy());
  }
  registeredProps_ = other.registeredProps_;
  filePath_ = other.filePath_;
}

void CelebornConf::registerProperty(
    const std::string_view& key,
    const std::string& value) {
  setValue(static_cast<std::string>(key), value);
}

Timeout CelebornConf::rpcLookupTimeout() const {
  return utils::toTimeout(
      toDuration(optionalProperty(kRpcLookupTimeout).value()));
}

Timeout CelebornConf::clientRpcGetReducerFileGroupRpcAskTimeout() const {
  return utils::toTimeout(toDuration(
      optionalProperty(kClientRpcGetReducerFileGroupRpcAskTimeout).value()));
}

Timeout CelebornConf::networkConnectTimeout() const {
  return utils::toTimeout(
      toDuration(optionalProperty(kNetworkConnectTimeout).value()));
}

Timeout CelebornConf::clientFetchTimeout() const {
  return utils::toTimeout(
      toDuration(optionalProperty(kClientFetchTimeout).value()));
}

int CelebornConf::networkIoNumConnectionsPerPeer() const {
  return std::stoi(optionalProperty(kNetworkIoNumConnectionsPerPeer).value());
}

int CelebornConf::networkIoClientThreads() const {
  return std::stoi(optionalProperty(kNetworkIoClientThreads).value());
}

int CelebornConf::clientFetchMaxReqsInFlight() const {
  return std::stoi(optionalProperty(kClientFetchMaxReqsInFlight).value());
}
} // namespace conf
} // namespace celeborn
