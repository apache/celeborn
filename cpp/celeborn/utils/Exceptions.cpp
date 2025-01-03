/*
 * Based on Exceptions.cpp from Facebook Velox
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

#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace utils {
namespace detail {

CELEBORN_DEFINE_CHECK_FAIL_TEMPLATES(::celeborn::utils::CelebornRuntimeError);
CELEBORN_DEFINE_CHECK_FAIL_TEMPLATES(::celeborn::utils::CelebornUserError);

} // namespace detail
} // namespace utils
} // namespace celeborn
