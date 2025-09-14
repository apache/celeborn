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
message("Appending CMAKE_CXX_FLAGS with ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
if ("${TREAT_WARNINGS_AS_ERRORS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif ()

# Avoid folly::f14::detail::F14LinkCheck problem on x86-64 platform.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")

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
    find_package(gflags REQUIRED COMPONENTS static)
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
        COMMAND protoc --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/celeborn/proto --cpp_out=${CMAKE_BINARY_DIR}/celeborn/proto ${ProtoFile}
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
