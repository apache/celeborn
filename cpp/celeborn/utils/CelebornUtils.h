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

#include <chrono>

#include "celeborn/utils/Exceptions.h"

namespace celeborn {
#define CELEBORN_STARTUP_LOG_PREFIX "[CELEBORN_STARTUP] "
#define CELEBORN_STARTUP_LOG(severity) \
  LOG(severity) << CELEBORN_STARTUP_LOG_PREFIX

#define CELEBORN_SHUTDOWN_LOG_PREFIX "[CELEBORN_SHUTDOWN] "
#define CELEBORN_SHUTDOWN_LOG(severity) \
  LOG(severity) << CELEBORN_SHUTDOWN_LOG_PREFIX


using Duration = std::chrono::duration<double>;
using Timeout = std::chrono::milliseconds;
inline Timeout toTimeout(Duration duration) {
  return std::chrono::duration_cast<Timeout>(duration);

}
} // namespace celeborn

