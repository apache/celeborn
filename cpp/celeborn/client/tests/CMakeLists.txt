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

add_executable(
        celeborn_client_test
        WorkerPartitionReaderTest.cpp
        PushDataCallbackTest.cpp
        PushStateTest.cpp
        ReviveManagerTest.cpp
        Lz4DecompressorTest.cpp
        ZstdDecompressorTest.cpp
        Lz4CompressorTest.cpp
        ZstdCompressorTest.cpp
        CompressorFactoryTest.cpp)

add_test(NAME celeborn_client_test COMMAND celeborn_client_test)

target_include_directories(client PUBLIC ${CMAKE_BINARY_DIR})

target_link_libraries(
        celeborn_client_test
        PRIVATE
        memory
        conf
        proto
        protocol
        utils
        network
        client
        ${WANGLE}
        ${FIZZ}
        ${LIBSODIUM_LIBRARY}
        ${FOLLY_WITH_DEPENDENCIES}
        ${GLOG}
        ${GFLAGS_LIBRARIES}
        GTest::gtest
        GTest::gmock
        GTest::gtest_main)