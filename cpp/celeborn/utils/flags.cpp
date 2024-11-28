/*
 * Based on flags.cpp from Facebook Velox
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

#include <gflags/gflags.h>

/* Used in utils/CelebornException.cpp */

DEFINE_bool(
    celeborn_exception_user_stacktrace_enabled,
    false,
    "Enable the stacktrace for user type of CelebornException");

DEFINE_bool(
    celeborn_exception_system_stacktrace_enabled,
    true,
    "Enable the stacktrace for system type of CelebornException");

DEFINE_int32(
    celeborn_exception_user_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " user type of CelebornException; off when set to 0 (the default)");

DEFINE_int32(
    celeborn_exception_system_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " system type of CelebornException; off when set to 0 (the default)");

/* Used in utils/ProcessBase.cpp */

DEFINE_bool(celeborn_avx2, true, "Enables use of AVX2 when available");

DEFINE_bool(celeborn_bmi2, true, "Enables use of BMI2 when available");
