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

#include <google/protobuf/io/coded_stream.h>
#include <charconv>
#include <chrono>
#include <vector>

#include "celeborn/memory/ByteBuffer.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace utils {
#define CELEBORN_STARTUP_LOG_PREFIX "[CELEBORN_STARTUP] "
#define CELEBORN_STARTUP_LOG(severity) \
  LOG(severity) << CELEBORN_STARTUP_LOG_PREFIX

#define CELEBORN_SHUTDOWN_LOG_PREFIX "[CELEBORN_SHUTDOWN] "
#define CELEBORN_SHUTDOWN_LOG(severity) \
  LOG(severity) << CELEBORN_SHUTDOWN_LOG_PREFIX

template <typename T>
std::vector<T> toVector(const std::set<T>& in) {
  std::vector<T> out{};
  out.reserve(in.size());
  for (const auto& i : in) {
    out.emplace_back(i);
  }
  return std::move(out);
}

std::string makeShuffleKey(const std::string& appId, int shuffleId);

void writeUTF(memory::WriteOnlyByteBuffer& buffer, const std::string& msg);

void writeRpcAddress(
    memory::WriteOnlyByteBuffer& buffer,
    const std::string& host,
    int port);

using Duration = std::chrono::duration<double>;
using Timeout = std::chrono::milliseconds;
inline Timeout toTimeout(Duration duration) {
  return std::chrono::duration_cast<Timeout>(duration);
}

/// parse string like "Any-Host-Str:Port#1:Port#2:...:Port#num", split into
/// {"Any-Host-Str", "Port#1", "Port#2", ..., "Port#num"}. Note that the
/// "Any-Host-Str" might contain ':' in IPV6 address.
std::vector<std::string_view> parseColonSeparatedHostPorts(
    const std::string_view& s,
    int num);

std::vector<std::string_view> explode(const std::string_view& s, char delim);

std::tuple<std::string_view, std::string_view> split(
    const std::string_view& s,
    char delim);

template <class T>
T strv2val(const std::string_view& s) {
  T t;
  const char* first = s.data();
  const char* last = s.data() + s.size();
  std::from_chars_result res = std::from_chars(first, last, t);

  // These two exceptions reflect the behavior of std::stoi.
  if (res.ec == std::errc::invalid_argument) {
    CELEBORN_FAIL("Invalid argument when parsing");
  } else if (res.ec == std::errc::result_out_of_range) {
    CELEBORN_FAIL("Out of range when parsing");
  }
  return t;
}

template <typename T>
std::unique_ptr<T> parseProto(const uint8_t* bytes, int len) {
  CELEBORN_CHECK_NOT_NULL(
      bytes, "Data for {} must be non-null", typeid(T).name());

  auto pbObj = std::make_unique<T>();

  google::protobuf::io::CodedInputStream cis(bytes, len);

  // The default recursion depth is 100, which causes some test cases to fail
  // during regression testing. By setting the recursion depth limit to 2000,
  // it means that during the parsing process, if the recursion depth exceeds
  // 2000 layers, the parsing process will be terminated and an error will be
  // returned.
  cis.SetRecursionLimit(2000);
  bool parseSuccess = (pbObj.get())->ParseFromCodedStream(&cis);

  if (!parseSuccess) {
    std::cerr << "Unable to parse " << typeid(T).name() << " protobuf";
    exit(1);
  }
  return pbObj;
}

} // namespace utils
} // namespace celeborn
