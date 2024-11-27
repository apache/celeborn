/*
 * Based on VeloxException.cpp from Facebook Velox
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

#include "celeborn/utils/CelebornException.h"

#include <folly/synchronization/AtomicStruct.h>
#include <google/protobuf/message.h>

#include <exception>

namespace celeborn {

std::exception_ptr toCelebornException(const std::exception_ptr& exceptionPtr) {
  try {
    std::rethrow_exception(exceptionPtr);
  } catch (const CelebornException&) {
    return exceptionPtr;
  } catch (const std::exception& e) {
    return std::make_exception_ptr(
        CelebornUserError(std::current_exception(), e.what(), false));
  }
}

int64_t& threadNumCelebornThrow() {
  thread_local int64_t numThrow;
  return numThrow;
}

ExceptionContext& getExceptionContext() {
  thread_local ExceptionContext context;
  return context;
}

// Retrieves the message of the top-level ancestor of the current exception
// context. If the top-level context message is not empty and is the same as the
// current one, returns a string indicating they are the same.
std::string getTopLevelExceptionContextString(
    CelebornException::Type exceptionType,
    const std::string& currentMessage) {
  auto* context = &getExceptionContext();
  if (context->parent && context->parent->parent) {
    while (context->parent && context->parent->parent) {
      context = context->parent;
    }
    auto topLevelMessage = context->message(exceptionType);
    if (!topLevelMessage.empty() && topLevelMessage == currentMessage) {
      return "Same as context.";
    } else {
      return topLevelMessage;
    }
  }

  if (!currentMessage.empty()) {
    return "Same as context.";
  }
  return "";
}

CelebornException::CelebornException(
    const char* file,
    size_t line,
    const char* function,
    std::string_view failingExpression,
    std::string_view message,
    std::string_view errorSource,
    std::string_view errorCode,
    bool isRetriable,
    Type exceptionType,
    std::string_view exceptionName)
    : CelebornException(State::make(exceptionType, [&](auto& state) {
        state.exceptionType = exceptionType;
        state.exceptionName = exceptionName;
        state.file = file;
        state.line = line;
        state.function = function;
        state.failingExpression = failingExpression;
        state.message = message;
        state.errorSource = errorSource;
        state.errorCode = errorCode;
        state.context = getExceptionContext().message(exceptionType);
        state.topLevelContext =
            getTopLevelExceptionContextString(exceptionType, state.context);
        state.isRetriable = isRetriable;
      })) {}

CelebornException::CelebornException(
    const std::exception_ptr& e,
    std::string_view message,
    std::string_view errorSource,
    std::string_view errorCode,
    bool isRetriable,
    Type exceptionType,
    std::string_view exceptionName)
    : CelebornException(State::make([&](auto& state) {
        state.exceptionType = exceptionType;
        state.exceptionName = exceptionName;
        state.file = "UNKNOWN";
        state.line = 0;
        state.function = "";
        state.failingExpression = "";
        state.message = message;
        state.errorSource = errorSource;
        state.errorCode = errorCode;
        state.context = getExceptionContext().message(exceptionType);
        state.topLevelContext =
            getTopLevelExceptionContextString(exceptionType, state.context);
        state.isRetriable = isRetriable;
        state.wrappedException = e;
      })) {}

namespace {

/// returns whether CelebornException stacktraces are enabled and whether, if
/// they are rate-limited, whether the rate-limit check passes
bool isStackTraceEnabled(CelebornException::Type type) {
  using namespace std::literals::chrono_literals;
  const bool isSysException = type == CelebornException::Type::kSystem;
  if ((isSysException && !FLAGS_celeborn_exception_system_stacktrace_enabled) ||
      (!isSysException && !FLAGS_celeborn_exception_user_stacktrace_enabled)) {
    // CelebornException stacktraces are disabled.
    return false;
  }

  const int32_t rateLimitMs = isSysException
      ? FLAGS_celeborn_exception_system_stacktrace_rate_limit_ms
      : FLAGS_celeborn_exception_user_stacktrace_rate_limit_ms;
  // not static so the gflag can be manipulated at runtime
  if (0 == rateLimitMs) {
    // CelebornException stacktraces are not rate-limited
    return true;
  }
  static folly::AtomicStruct<std::chrono::steady_clock::time_point> systemLast;
  static folly::AtomicStruct<std::chrono::steady_clock::time_point> userLast;
  auto* last = isSysException ? &systemLast : &userLast;

  auto const now = std::chrono::steady_clock::now();
  auto latest = last->load(std::memory_order_relaxed);
  if (now < latest + std::chrono::milliseconds(rateLimitMs)) {
    // CelebornException stacktraces are rate-limited and the rate-limit check
    // failed
    return false;
  }

  // CelebornException stacktraces are rate-limited and the rate-limit check
  // passed
  //
  // the cas happens only here, so the rate-limit check in effect gates not
  // only computation of the stacktrace but also contention on this atomic
  // variable
  return last->compare_exchange_strong(latest, now, std::memory_order_relaxed);
}

} // namespace

template <typename F>
std::shared_ptr<const CelebornException::State> CelebornException::State::make(
    CelebornException::Type exceptionType,
    F f) {
  auto state = std::make_shared<CelebornException::State>();
  if (isStackTraceEnabled(exceptionType)) {
    // new v.s. make_unique to avoid any extra frames from make_unique
    state->stackTrace.reset(new utils::StackTrace());
  }
  f(*state);
  return state;
}

/*
Not much to say. Constructs the elaborate message from the available
pieces of information.
 */
void CelebornException::State::finalize() const {
  assert(elaborateMessage.empty());

  // Fill elaborateMessage_
  if (!exceptionName.empty()) {
    elaborateMessage += "Exception: ";
    elaborateMessage += exceptionName;
    elaborateMessage += '\n';
  }

  if (!errorSource.empty()) {
    elaborateMessage += "Error Source: ";
    elaborateMessage += errorSource;
    elaborateMessage += '\n';
  }

  if (!errorCode.empty()) {
    elaborateMessage += "Error Code: ";
    elaborateMessage += errorCode;
    elaborateMessage += '\n';
  }

  if (!message.empty()) {
    elaborateMessage += "Reason: ";
    elaborateMessage += message;
    elaborateMessage += '\n';
  }

  elaborateMessage += "Retriable: ";
  elaborateMessage += isRetriable ? "True" : "False";
  elaborateMessage += '\n';

  if (!failingExpression.empty()) {
    elaborateMessage += "Expression: ";
    elaborateMessage += failingExpression;
    elaborateMessage += '\n';
  }

  if (!context.empty()) {
    elaborateMessage += "Context: " + context + "\n";
  }

  if (!topLevelContext.empty()) {
    elaborateMessage += "Top-Level Context: " + topLevelContext + "\n";
  }

  if (function) {
    elaborateMessage += "Function: ";
    elaborateMessage += function;
    elaborateMessage += '\n';
  }

  if (file) {
    elaborateMessage += "File: ";
    elaborateMessage += file;
    elaborateMessage += '\n';
  }

  if (line) {
    elaborateMessage += "Line: ";
    auto len = elaborateMessage.size();
    size_t t = line;
    do {
      elaborateMessage += static_cast<char>('0' + t % 10);
      t /= 10;
    } while (t);
    reverse(elaborateMessage.begin() + len, elaborateMessage.end());
    elaborateMessage += '\n';
  }

  elaborateMessage += "Stack trace:\n";
  if (stackTrace) {
    elaborateMessage += stackTrace->toString();
  } else {
    elaborateMessage += "Stack trace has been disabled.";
    if (exceptionType == CelebornException::Type::kSystem) {
      elaborateMessage +=
          " Use --celeborn_exception_system_stacktrace_enabled=true to enable it.\n";
    } else {
      elaborateMessage +=
          " Use --celeborn_exception_user_stacktrace_enabled=true to enable it.\n";
    }
  }
}

const char* CelebornException::State::what() const noexcept {
  try {
    folly::call_once(once, [&] { finalize(); });
    return elaborateMessage.c_str();
  } catch (...) {
    return "<unknown failure in CelebornException::what>";
  }
}

} // namespace celeborn
