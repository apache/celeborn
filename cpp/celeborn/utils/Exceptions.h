/*
 * Based on Exceptions.h from Facebook Velox
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

#include <memory>
#include <sstream>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <glog/logging.h>

#include <folly/Conv.h>
#include <folly/Exception.h>
#include <folly/Preprocessor.h>

#include "celeborn/utils/CelebornException.h"
// TODO: maybe remove the file permanently...
// #include "celeborn/utils/FmtStdFormatters.h"

namespace celeborn {
namespace utils {
namespace detail {

struct CelebornCheckFailArgs {
  const char* file;
  size_t line;
  const char* function;
  const char* expression;
  const char* errorSource;
  const char* errorCode;
  bool isRetriable;
};

struct CompileTimeEmptyString {
  CompileTimeEmptyString() = default;
  constexpr operator const char*() const {
    return "";
  }
  constexpr operator std::string_view() const {
    return {};
  }
  operator std::string() const {
    return {};
  }
};

// celebornCheckFail is defined as a separate helper function rather than
// a macro or inline `throw` expression to allow the compiler *not* to
// inline it when it is large. Having an out-of-line error path helps
// otherwise-small functions that call error-checking macros stay
// small and thus stay eligible for inlining.
template <typename Exception, typename StringType>
[[noreturn]] void celebornCheckFail(
    const CelebornCheckFailArgs& args,
    StringType s) {
  static_assert(
      !std::is_same_v<StringType, std::string>,
      "BUG: we should not pass std::string by value to celebornCheckFail");
  if constexpr (!std::is_same_v<Exception, CelebornUserError>) {
    LOG(ERROR) << "Line: " << args.file << ":" << args.line
               << ", Function:" << args.function
               << ", Expression: " << args.expression << " " << s
               << ", Source: " << args.errorSource
               << ", ErrorCode: " << args.errorCode;
  }

  ++threadNumCelebornThrow();
  throw Exception(
      args.file,
      args.line,
      args.function,
      args.expression,
      s,
      args.errorSource,
      args.errorCode,
      args.isRetriable);
}

// CelebornCheckFailStringType helps us pass by reference to
// celebornCheckFail exactly when the string type is std::string.
template <typename T>
struct CelebornCheckFailStringType;

template <>
struct CelebornCheckFailStringType<CompileTimeEmptyString> {
  using type = CompileTimeEmptyString;
};

template <>
struct CelebornCheckFailStringType<const char*> {
  using type = const char*;
};

template <>
struct CelebornCheckFailStringType<std::string> {
  using type = const std::string&;
};

// Declare explicit instantiations of celebornCheckFail for the given
// exceptionType. Just like normal function declarations (prototypes),
// this allows the compiler to assume that they are defined elsewhere
// and simply insert a function call for the linker to fix up, rather
// than emitting a definition of these templates into every
// translation unit they are used in.
#define CELEBORN_DECLARE_CHECK_FAIL_TEMPLATES(exception_type)                 \
  namespace detail {                                                          \
  extern template void                                                        \
  celebornCheckFail<exception_type, CompileTimeEmptyString>(                  \
      const CelebornCheckFailArgs& args,                                      \
      CompileTimeEmptyString);                                                \
  extern template void celebornCheckFail<exception_type, const char*>(        \
      const CelebornCheckFailArgs& args,                                      \
      const char*);                                                           \
  extern template void celebornCheckFail<exception_type, const std::string&>( \
      const CelebornCheckFailArgs& args,                                      \
      const std::string&);                                                    \
  } // namespace detail

// Definitions corresponding to CELEBORN_DECLARE_CHECK_FAIL_TEMPLATES. Should
// only be used in Exceptions.cpp.
#define CELEBORN_DEFINE_CHECK_FAIL_TEMPLATES(exception_type)               \
  template void celebornCheckFail<exception_type, CompileTimeEmptyString>( \
      const CelebornCheckFailArgs& args, CompileTimeEmptyString);          \
  template void celebornCheckFail<exception_type, const char*>(            \
      const CelebornCheckFailArgs& args, const char*);                     \
  template void celebornCheckFail<exception_type, const std::string&>(     \
      const CelebornCheckFailArgs& args, const std::string&);

// When there is no message passed, we can statically detect this case
// and avoid passing even a single unnecessary argument pointer,
// minimizing size and thus maximizing eligibility for inlining.
inline CompileTimeEmptyString errorMessage() {
  return {};
}

inline const char* errorMessage(const char* s) {
  return s;
}

inline std::string errorMessage(const std::string& str) {
  return str;
}

template <typename... Args>
std::string errorMessage(fmt::string_view fmt, const Args&... args) {
  return fmt::vformat(fmt, fmt::make_format_args(args...));
}

} // namespace detail

#define _CELEBORN_THROW_IMPL(                                            \
    exception, exprStr, errorSource, errorCode, isRetriable, ...)        \
  {                                                                      \
    /* GCC 9.2.1 doesn't accept this code with constexpr. */             \
    static const ::celeborn::utils::detail::CelebornCheckFailArgs        \
        celebornCheckFailArgs = {                                        \
            __FILE__,                                                    \
            __LINE__,                                                    \
            __FUNCTION__,                                                \
            exprStr,                                                     \
            errorSource,                                                 \
            errorCode,                                                   \
            isRetriable};                                                \
    auto message = ::celeborn::utils::detail::errorMessage(__VA_ARGS__); \
    ::celeborn::utils::detail::celebornCheckFail<                        \
        exception,                                                       \
        typename ::celeborn::utils::detail::CelebornCheckFailStringType< \
            decltype(message)>::type>(celebornCheckFailArgs, message);   \
  }

#define _CELEBORN_CHECK_AND_THROW_IMPL(                                        \
    expr, exprStr, exception, errorSource, errorCode, isRetriable, ...)        \
  if (UNLIKELY(!(expr))) {                                                     \
    _CELEBORN_THROW_IMPL(                                                      \
        exception, exprStr, errorSource, errorCode, isRetriable, __VA_ARGS__); \
  }

#define _CELEBORN_THROW(exception, ...) \
  _CELEBORN_THROW_IMPL(exception, "", ##__VA_ARGS__)

CELEBORN_DECLARE_CHECK_FAIL_TEMPLATES(::celeborn::utils::CelebornRuntimeError);

#define _CELEBORN_CHECK_IMPL(expr, exprStr, ...)                    \
  _CELEBORN_CHECK_AND_THROW_IMPL(                                   \
      expr,                                                         \
      exprStr,                                                      \
      ::celeborn::utils::CelebornRuntimeError,                      \
      ::celeborn::utils::error_source::kErrorSourceRuntime.c_str(), \
      ::celeborn::utils::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

// If the caller passes a custom message (4 *or more* arguments), we
// have to construct a format string from ours ("({} vs. {})") plus
// theirs by adding a space and shuffling arguments. If they don't (exactly 3
// arguments), we can just pass our own format string and arguments straight
// through.

#define _CELEBORN_CHECK_OP_WITH_USER_FMT_HELPER( \
    implmacro, expr1, expr2, op, user_fmt, ...)  \
  implmacro(                                     \
      (expr1)op(expr2),                          \
      #expr1 " " #op " " #expr2,                 \
      "({} vs. {}) " user_fmt,                   \
      expr1,                                     \
      expr2,                                     \
      ##__VA_ARGS__)

#define _CELEBORN_CHECK_OP_HELPER(implmacro, expr1, expr2, op, ...) \
  if constexpr (FOLLY_PP_DETAIL_NARGS(__VA_ARGS__) > 0) {           \
    _CELEBORN_CHECK_OP_WITH_USER_FMT_HELPER(                        \
        implmacro, expr1, expr2, op, __VA_ARGS__);                  \
  } else {                                                          \
    implmacro(                                                      \
        (expr1)op(expr2),                                           \
        #expr1 " " #op " " #expr2,                                  \
        "({} vs. {})",                                              \
        expr1,                                                      \
        expr2);                                                     \
  }

#define _CELEBORN_CHECK_OP(expr1, expr2, op, ...) \
  _CELEBORN_CHECK_OP_HELPER(                      \
      _CELEBORN_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

#define _CELEBORN_USER_CHECK_IMPL(expr, exprStr, ...)            \
  _CELEBORN_CHECK_AND_THROW_IMPL(                                \
      expr,                                                      \
      exprStr,                                                   \
      ::celeborn::utils::CelebornUserError,                      \
      ::celeborn::utils::error_source::kErrorSourceUser.c_str(), \
      ::celeborn::utils::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define _CELEBORN_USER_CHECK_OP(expr1, expr2, op, ...) \
  _CELEBORN_CHECK_OP_HELPER(                           \
      _CELEBORN_USER_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define CELEBORN_CHECK(expr, ...) \
  _CELEBORN_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define CELEBORN_CHECK_GT(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define CELEBORN_CHECK_GE(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define CELEBORN_CHECK_LT(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define CELEBORN_CHECK_LE(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define CELEBORN_CHECK_EQ(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define CELEBORN_CHECK_NE(e1, e2, ...) \
  _CELEBORN_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define CELEBORN_CHECK_NULL(e, ...) CELEBORN_CHECK(e == nullptr, ##__VA_ARGS__)
#define CELEBORN_CHECK_NOT_NULL(e, ...) \
  CELEBORN_CHECK(e != nullptr, ##__VA_ARGS__)

#define CELEBORN_CHECK_OK(expr)                          \
  do {                                                   \
    ::celeborn::utils::Status _s = (expr);               \
    _CELEBORN_CHECK_IMPL(_s.ok(), #expr, _s.toString()); \
  } while (false)

#define CELEBORN_UNSUPPORTED(...)                                \
  _CELEBORN_THROW(                                               \
      ::celeborn::utils::CelebornUserError,                      \
      ::celeborn::utils::error_source::kErrorSourceUser.c_str(), \
      ::celeborn::utils::error_code::kUnsupported.c_str(),       \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define CELEBORN_ARITHMETIC_ERROR(...)                           \
  _CELEBORN_THROW(                                               \
      ::celeborn::utils::CelebornUserError,                      \
      ::celeborn::utils::error_source::kErrorSourceUser.c_str(), \
      ::celeborn::utils::error_code::kArithmeticError.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define CELEBORN_SCHEMA_MISMATCH_ERROR(...)                      \
  _CELEBORN_THROW(                                               \
      ::celeborn::utils::CelebornUserError,                      \
      ::celeborn::utils::error_source::kErrorSourceUser.c_str(), \
      ::celeborn::utils::error_code::kSchemaMismatch.c_str(),    \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define CELEBORN_FILE_NOT_FOUND_ERROR(...)                          \
  _CELEBORN_THROW(                                                  \
      ::celeborn::utils::CelebornRuntimeError,                      \
      ::celeborn::utils::error_source::kErrorSourceRuntime.c_str(), \
      ::celeborn::utils::error_code::kFileNotFound.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

#define CELEBORN_UNREACHABLE(...)                                   \
  _CELEBORN_THROW(                                                  \
      ::celeborn::utils::CelebornRuntimeError,                      \
      ::celeborn::utils::error_source::kErrorSourceRuntime.c_str(), \
      ::celeborn::utils::error_code::kUnreachableCode.c_str(),      \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

#ifndef NDEBUG
#define CELEBORN_DCHECK(expr, ...) CELEBORN_CHECK(expr, ##__VA_ARGS__)
#define CELEBORN_DCHECK_GT(e1, e2, ...) CELEBORN_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_GE(e1, e2, ...) CELEBORN_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_LT(e1, e2, ...) CELEBORN_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_LE(e1, e2, ...) CELEBORN_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_EQ(e1, e2, ...) CELEBORN_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_NE(e1, e2, ...) CELEBORN_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_DCHECK_NULL(e, ...) CELEBORN_CHECK_NULL(e, ##__VA_ARGS__)
#define CELEBORN_DCHECK_NOT_NULL(e, ...) \
  CELEBORN_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#else
#define CELEBORN_DCHECK(expr, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_GT(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_GE(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_LT(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_LE(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_EQ(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_NE(e1, e2, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_NULL(e, ...) CELEBORN_CHECK(true)
#define CELEBORN_DCHECK_NOT_NULL(e, ...) CELEBORN_CHECK(true)
#endif

#define CELEBORN_FAIL(...)                                          \
  _CELEBORN_THROW(                                                  \
      ::celeborn::utils::CelebornRuntimeError,                      \
      ::celeborn::utils::error_source::kErrorSourceRuntime.c_str(), \
      ::celeborn::utils::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

CELEBORN_DECLARE_CHECK_FAIL_TEMPLATES(::celeborn::utils::CelebornUserError);

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define CELEBORN_USER_CHECK(expr, ...) \
  _CELEBORN_USER_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_GT(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_GE(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_LT(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_LE(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_EQ(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_NE(e1, e2, ...) \
  _CELEBORN_USER_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_NULL(e, ...) \
  CELEBORN_USER_CHECK(e == nullptr, ##__VA_ARGS__)
#define CELEBORN_USER_CHECK_NOT_NULL(e, ...) \
  CELEBORN_USER_CHECK(e != nullptr, ##__VA_ARGS__)

#ifndef NDEBUG
#define CELEBORN_USER_DCHECK(expr, ...) CELEBORN_USER_CHECK(expr, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_GT(e1, e2, ...) \
  CELEBORN_USER_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_GE(e1, e2, ...) \
  CELEBORN_USER_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_LT(e1, e2, ...) \
  CELEBORN_USER_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_LE(e1, e2, ...) \
  CELEBORN_USER_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_EQ(e1, e2, ...) \
  CELEBORN_USER_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_NE(e1, e2, ...) \
  CELEBORN_USER_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_NOT_NULL(e, ...) \
  CELEBORN_USER_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#define CELEBORN_USER_DCHECK_NULL(e, ...) \
  CELEBORN_USER_CHECK_NULL(e, ##__VA_ARGS__)
#else
#define CELEBORN_USER_DCHECK(expr, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_GT(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_GE(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_LT(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_LE(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_EQ(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_NE(e1, e2, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_NULL(e, ...) CELEBORN_USER_CHECK(true)
#define CELEBORN_USER_DCHECK_NOT_NULL(e, ...) CELEBORN_USER_CHECK(true)
#endif

#define CELEBORN_USER_FAIL(...)                                  \
  _CELEBORN_THROW(                                               \
      ::celeborn::utils::CelebornUserError,                      \
      ::celeborn::utils::error_source::kErrorSourceUser.c_str(), \
      ::celeborn::utils::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define CELEBORN_NYI(...)                                           \
  _CELEBORN_THROW(                                                  \
      ::celeborn::utils::CelebornRuntimeError,                      \
      ::celeborn::utils::error_source::kErrorSourceRuntime.c_str(), \
      ::celeborn::utils::error_code::kNotImplemented.c_str(),       \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

} // namespace utils
} // namespace celeborn
