# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.24)

if (NOT DEFINED PACKAGE_VERSION)
    set(PACKAGE_VERSION "0.0.1")
endif ()

project("celeborn" VERSION ${PACKAGE_VERSION} LANGUAGES CXX C)
enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# All internal static libs are whole-archived into the shared library
# `celeborn_client`, which on Linux/ELF requires every contained object
# to be position-independent. Without this, `ld` complains:
#   relocation R_X86_64_TPOFF32 against hidden symbol ... can not be used
#   when making a shared object
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
if ("${TREAT_WARNINGS_AS_ERRORS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif ()

# Avoid folly::f14::detail::F14LinkCheck problem on x86-64 platform.
# -msse4.2 is an x86-only flag; emitting it on aarch64 (Apple Silicon or
# aarch64 Linux) makes the compiler error out, so guard it by architecture.
if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
endif ()

# Resolve the Homebrew prefix on macOS instead of hard-coding the
# Apple-Silicon path. Intel Macs install under /usr/local while Apple
# Silicon uses /opt/homebrew; `brew --prefix` reports whichever applies.
if (APPLE)
    if (DEFINED ENV{HOMEBREW_PREFIX})
        set(HOMEBREW_PREFIX "$ENV{HOMEBREW_PREFIX}")
    else ()
        find_program(BREW_EXECUTABLE brew)
        if (BREW_EXECUTABLE)
            execute_process(
                COMMAND ${BREW_EXECUTABLE} --prefix
                OUTPUT_VARIABLE HOMEBREW_PREFIX
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        elseif (EXISTS /opt/homebrew)
            set(HOMEBREW_PREFIX "/opt/homebrew")
        else ()
            set(HOMEBREW_PREFIX "/usr/local")
        endif ()
    endif ()
    message(STATUS "Using HOMEBREW_PREFIX=${HOMEBREW_PREFIX}")
endif ()

# Set CMAKE_BUILD_TYPE to 'Release' if it is not specified.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()
message(STATUS "CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")

# Known warnings that are benign can be disabled.
set(DISABLED_WARNINGS
        "-Wno-nullability-completeness -Wno-deprecated-declarations")

# Important warnings that must be explicitly enabled.
set(ENABLE_WARNINGS "-Wreorder")

# The CMAKE_PREFIX_PATH should be set to the thirdparty's install path
# (thirdparty/installed by default), to find all the dependencies.
message(STATUS "Using CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")
# Currently, we use the "-lgtest" to link the gtest library, and gtest library
# is in the "thirdparty/installed/lib64" directory in the linux environment, so
# the lib search path needs to be specified here.
if (EXISTS ${CMAKE_PREFIX_PATH}/lib64)
    link_directories(${CMAKE_PREFIX_PATH}/lib64)
    # thrift.a is installed in the directory thirdparty/installed/lib by
    # default in the Linux environment.
    link_directories(${CMAKE_PREFIX_PATH}/lib)
elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
    message(
            WARNING
            "You did not use the recommended way "
            "(using 'thirdparty/build-thirdparty.sh') to build & install "
            "thirdparty libraries.")
endif ()

#if (NOT APPLE)
#    set(Boost_USE_STATIC_RUNTIME ON)
#    set(Boost_USE_STATIC_LIBS ON)
#endif ()
set(Boost_USE_MULTITHREADED TRUE)
find_package(
        Boost
	#1.75.0
        1.84.0
        REQUIRED
        program_options
        context
        filesystem
        regex
        thread
        system
        date_time
        atomic)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

# GFlags
#
# NOTE: The name here must be exactly "gflags", that is, use all lowercase.
# Otherwise, e.g. "GFLAGS" or "GFlags", the generated `GFLAGS_LIBRARIES` will
# point to the shared library instead of the static library, even if we
# explicitly specify to link against the static library (via "COMPONENTS
# static"). This may be a problem that the cmake script of GFlags does not
# consider comprehensively (for the case of words).
#
# See [1] for the recommended `find_package` commands to use to find GFlags, in
# which the "@PACKAGE_NAME@" will be replaced with "gflags" when installed.
#
# [1] https://github.com/gflags/gflags/blob/v2.2.2/cmake/config.cmake.in#L50-L56
if (APPLE)
    # Use the shared library of gflags on MacOS because it is installed via
    # Homebrew and only shared library is installed.
    find_package(gflags REQUIRED COMPONENTS shared)
else ()
    # We build celeborn_client as a SHARED library (whole-archives every
    # internal .a). Ubuntu's libgflags.a from apt is not compiled with
    # -fPIC, so it cannot be linked into a .so. The shared gflags
    # (libgflags.so) is PIC and works for both standalone executables and
    # the aggregated shared library.
    find_package(gflags REQUIRED COMPONENTS shared)
endif ()

find_package(glog REQUIRED)
find_library(FMT fmt)

find_package(folly CONFIG REQUIRED)
set(FOLLY_WITH_DEPENDENCIES
        ${FOLLY_LIBRARIES}
        Boost::context
        dl
)

# Include third party header files
find_path(OPT_OPENSSL_DIR NAMES opt/openssl@1.1)
set(OPENSSL_ROOT_DIR "${OPT_OPENSSL_DIR}/opt/openssl@1.1")
find_package(OpenSSL REQUIRED)

find_package(Protobuf REQUIRED)

set(CMAKE_MODULE_PATH
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
        ${CMAKE_MODULE_PATH})
find_package(Sodium REQUIRED)
find_library(FIZZ fizz REQUIRED)
find_library(WANGLE wangle REQUIRED)

find_package(LZ4 REQUIRED)
set(LZ4_WITH_DEPENDENCIES
        ${LZ4_LIBRARY}
        xxhash
)

find_library(RE2 re2)

find_package(fizz CONFIG REQUIRED)
find_package(wangle CONFIG REQUIRED)

set(WANGLE_LIBRARIES ${WANGLE} ${FIZZ})

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

include_directories(SYSTEM celeborn)
include_directories(.)

## TODO: to avoid the file location dependency problem caused by protobuf_cpp_generate,
## we hardcode the protoc procedure here. Maybe we could find a more elegant way to fix
## this later...
set(ProtoFile "${CMAKE_CURRENT_SOURCE_DIR}/celeborn/proto/TransportMessagesCpp.proto")
set(ProtoGenHeader "${CMAKE_BINARY_DIR}/celeborn/proto/TransportMessagesCpp.pb.h")
set(ProtoGenSource "${CMAKE_BINARY_DIR}/celeborn/proto/TransportMessagesCpp.pb.cc")
add_custom_command(
        OUTPUT ${ProtoGenSource} ${ProtoGenHeader}
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/celeborn/proto
        COMMAND protobuf::protoc --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/celeborn/proto --cpp_out=${CMAKE_BINARY_DIR}/celeborn/proto ${ProtoFile}
        DEPENDS ${ProtoFile}
        VERBATIM)
add_custom_target(
        generate_proto_source
        DEPENDS
        ${ProtoGenSource} ${ProtoGenHeader})

option(CELEBORN_BUILD_TESTS "CELEBORN_BUILD_TESTS" ON)
if(CELEBORN_BUILD_TESTS)
    ### TODO: we use prebuilt gtest prebuilt package here. A better way is
    ###  to use source package, but the setting would be more complicated.
    ###  Maybe we could change the method later.
    find_package(GTest CONFIG REQUIRED)
    # Include after project() but before add_subdirectory().
    include(CTest)
endif()

add_subdirectory(celeborn)

# ---------------------------------------------------------------------------
# Aggregate SHARED library — bundles all internal static libs (including
# the C ABI shim `celeborn_ffi`) into a single libceleborn_client.{so,dylib}.
# Downstream language bindings (Rust / ctypes / etc.) link only this file
# and never see folly / protobuf / glog / abseil.
# ---------------------------------------------------------------------------
add_library(celeborn_client SHARED dummy.cc)

# Internal targets propagate plain library names like "xxhash" / "fmt" via
# their link interface. Those need an actual search path on macOS where the
# Homebrew prefix is not on ld's default path.
if(APPLE)
  target_link_directories(celeborn_client PRIVATE
    ${HOMEBREW_PREFIX}/lib
    ${HOMEBREW_PREFIX}/opt/openssl@3/lib)
endif()

set(_CELEBORN_INTERNAL_LIBS
        celeborn_ffi client protocol network proto memory conf utils)

if(APPLE)
  # ld64: -force_load pulls every object out of each .a so no symbol gets
  # stripped by the linker before it ships in the .dylib. Wrapping the
  # target file path in a generator expression does not register a
  # build-order dependency on those targets, so add it explicitly.
  add_dependencies(celeborn_client ${_CELEBORN_INTERNAL_LIBS})
  foreach(_lib IN LISTS _CELEBORN_INTERNAL_LIBS)
    target_link_libraries(celeborn_client PRIVATE
      "-Wl,-force_load" "$<TARGET_FILE:${_lib}>")
  endforeach()
else()
  target_link_libraries(celeborn_client PRIVATE
    -Wl,--whole-archive
    ${_CELEBORN_INTERNAL_LIBS}
    -Wl,--no-whole-archive)
endif()

# Third-party deps remain NEEDED entries inside the produced .so/.dylib and
# resolve at runtime from the system loader path.
target_link_libraries(celeborn_client PRIVATE
  ${WANGLE} ${FIZZ}
  ${LIBSODIUM_LIBRARY}
  ${FOLLY_WITH_DEPENDENCIES}
  ${LZ4_WITH_DEPENDENCIES}
  ${GLOG} ${GFLAGS_LIBRARIES}
  ${RE2}
  protobuf::libprotobuf
  OpenSSL::SSL OpenSSL::Crypto)

# protobuf v28+ pulls in dozens of absl_* sub-libraries. Prefer the CMake
# package config / pkg-config exported by abseil, which provides the correct
# imported targets regardless of where the distribution installs them. This
# is portable across Apple Silicon, Intel macOS, x86_64 Linux and aarch64
# Linux (whose multiarch dir is /usr/lib/aarch64-linux-gnu, not covered by
# any hard-coded path).
find_package(absl CONFIG QUIET)
if(absl_FOUND)
  # Link the umbrella target if present, otherwise the common building blocks
  # that protobuf actually depends on.
  if(TARGET absl::absl)
    target_link_libraries(celeborn_client PRIVATE absl::absl)
  else()
    target_link_libraries(celeborn_client PRIVATE
      absl::base absl::strings absl::status absl::statusor
      absl::synchronization absl::time absl::log absl::log_internal_check_op)
  endif()
else()
  # No CMake config — fall back to globbing the platform library directory.
  if(APPLE)
    set(_ABSEIL_LIB_DIR "${HOMEBREW_PREFIX}/lib")
    set(_ABSEIL_LIB_EXT "dylib")
  else()
    set(_ABSEIL_LIB_EXT "so")
    # Derive the GNU multiarch library directory from the toolchain triplet
    # (e.g. x86_64-linux-gnu, aarch64-linux-gnu) so non-x86_64 Linux targets
    # advertised by the README actually resolve absl.
    execute_process(
      COMMAND ${CMAKE_CXX_COMPILER} -dumpmachine
      OUTPUT_VARIABLE _MULTIARCH_TRIPLET
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET)
    set(_ABSEIL_SEARCH_DIRS
      "/usr/lib/${_MULTIARCH_TRIPLET}"
      "/usr/local/lib"
      "/usr/lib64"
      "/usr/lib")
    set(_ABSEIL_LIB_DIR "")
    foreach(_dir IN LISTS _ABSEIL_SEARCH_DIRS)
      file(GLOB _probe "${_dir}/libabsl_*.${_ABSEIL_LIB_EXT}")
      if(_probe)
        set(_ABSEIL_LIB_DIR "${_dir}")
        break()
      endif()
    endforeach()
  endif()

  file(GLOB _ABSEIL_LIBS "${_ABSEIL_LIB_DIR}/libabsl_*.${_ABSEIL_LIB_EXT}")
  if(_ABSEIL_LIBS)
    message(STATUS "Found abseil libraries in ${_ABSEIL_LIB_DIR}")
    target_link_libraries(celeborn_client PRIVATE ${_ABSEIL_LIBS})
  else()
    # protobuf almost always depends on absl; a missing absl is a hard link
    # failure rather than a warning to silently ignore.
    message(FATAL_ERROR
      "Could not locate libabsl_*.${_ABSEIL_LIB_EXT}. protobuf depends on "
      "abseil; install abseil-cpp (providing absl CMake config or "
      "libabsl_*.${_ABSEIL_LIB_EXT}) or point CMAKE_PREFIX_PATH at it.")
  endif()
endif()

# Hide every non-celeborn symbol so the dylib does not collide with the
# host process's own protobuf / abseil / folly runtimes.
set_target_properties(celeborn_client PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON)

if(APPLE)
  target_link_options(celeborn_client PRIVATE
    "-Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/exports.txt")
  set_target_properties(celeborn_client PROPERTIES
    INSTALL_RPATH "@loader_path;${HOMEBREW_PREFIX}/lib;${HOMEBREW_PREFIX}/opt/openssl@3/lib"
    BUILD_WITH_INSTALL_RPATH TRUE)
else()
  target_link_options(celeborn_client PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map"
    "-Wl,--exclude-libs,ALL")
  set_target_properties(celeborn_client PROPERTIES
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE)
endif()

# ---------------------------------------------------------------------------
# Install rules — headers + the aggregated shared library.
# Static libraries are still installed to keep the legacy build path working.
# ---------------------------------------------------------------------------
install(
  DIRECTORY celeborn/
  DESTINATION include/celeborn
  FILES_MATCHING PATTERN "*.h"
  PATTERN "tests" EXCLUDE
)

install(
  FILES ${ProtoGenHeader}
  DESTINATION include/celeborn/proto
)

install(
  TARGETS celeborn_client celeborn_ffi client conf memory network proto protocol utils
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
