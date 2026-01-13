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

 #include <gtest/gtest.h>
 #include <memory>
 #include <vector>
 
 #include "celeborn/client/ShuffleClient.h"
 #include "celeborn/client/compress/Lz4Compressor.h"
 #include "celeborn/client/compress/Lz4Decompressor.h"
 #include "celeborn/client/compress/ZstdCompressor.h"
 #include "celeborn/client/compress/ZstdDecompressor.h"
 #include "celeborn/network/TransportClient.h"
 #include "celeborn/protocol/PartitionLocation.h"
 #include "celeborn/proto/TransportMessagesCpp.pb.h"
 
 using namespace celeborn;
 using namespace celeborn::client;
 using namespace celeborn::protocol;
 
 namespace {
 
 class MockTransportClient : public network::TransportClient {
  public:
   MockTransportClient()
       : network::TransportClient(nullptr, nullptr, utils::MS(10000)) {}
 
   void pushDataAsync(
       const network::PushData& pushData,
       utils::Timeout timeout,
       std::shared_ptr<network::RpcResponseCallback> callback) override {
     capturedBody_ = pushData.body()->clone();
     // Simulate success
     auto response = memory::ReadOnlyByteBuffer::createEmptyBuffer();
     callback->onSuccess(std::move(response));
   }
 
   std::unique_ptr<memory::ReadOnlyByteBuffer> getCapturedBody() {
     return std::move(capturedBody_);
   }
 
  private:
   std::unique_ptr<memory::ReadOnlyByteBuffer> capturedBody_;
 };
 
 class MockTransportClientFactory : public network::TransportClientFactory {
  public:
   MockTransportClientFactory()
       : network::TransportClientFactory(
             std::make_shared<conf::CelebornConf>()) {}
 
   std::shared_ptr<network::TransportClient> createClient(
       const std::string& host,
       uint16_t port,
       int32_t partitionId) override {
     auto client = std::make_shared<MockTransportClient>();
     lastClient_ = client;
     return client;
   }
 
   std::shared_ptr<network::TransportClient> createClient(
       const std::string& host,
       uint16_t port) override {
     return createClient(host, port, 0);
   }
 
   std::shared_ptr<MockTransportClient> getLastClient() {
     return std::dynamic_pointer_cast<MockTransportClient>(lastClient_);
   }
 
  private:
   std::shared_ptr<network::TransportClient> lastClient_;
 };
 
 class MockShuffleClient : public ShuffleClientImpl {
  public:
   static std::shared_ptr<MockShuffleClient> create(
       const conf::CelebornConf& conf, int shuffleId = 0) {
     auto endpoint = std::make_shared<ShuffleClientEndpoint>(
         std::make_shared<conf::CelebornConf>(conf));
     auto client = std::shared_ptr<MockShuffleClient>(
         new MockShuffleClient("test-app", conf, *endpoint));
     client->clientFactory_ = std::make_shared<MockTransportClientFactory>();
     client->setupMockPartitionLocation(shuffleId, 0);
     return client;
   }
 
   bool mapperEnded(int shuffleId, int mapId) override { return false; }
 
   void addRequestToReviveManager(
       std::shared_ptr<protocol::ReviveRequest> reviveRequest) override {}
 
   void addPushDataRetryTask(folly::Func&& task) override { task(); }
 
   std::shared_ptr<MockTransportClientFactory> getMockClientFactory() {
     return std::dynamic_pointer_cast<MockTransportClientFactory>(clientFactory_);
   }
 
   void setupMockPartitionLocation(int shuffleId, int partitionId) {
     auto locationMap = std::make_shared<PartitionLocationMap>();
     auto partitionLocation = createMockPartitionLocation();
     locationMap->set(partitionId, partitionLocation);
     partitionLocationMaps_.set(shuffleId, locationMap);
   }
 
  private:
   MockShuffleClient(
       const std::string& appUniqueId,
       const conf::CelebornConf& conf,
       const ShuffleClientEndpoint& clientEndpoint)
       : ShuffleClientImpl(appUniqueId,
                           std::make_shared<conf::CelebornConf>(conf),
                           clientEndpoint) {}
 
   std::shared_ptr<const PartitionLocation> createMockPartitionLocation() {
     auto location = std::make_unique<PartitionLocation>();
     location->id = 0;
     location->epoch = 0;
     location->host = "test-host";
     location->rpcPort = 12345;
     location->pushPort = 12346;
     location->fetchPort = 12347;
     location->replicatePort = 12348;
     location->mode = PartitionLocation::PRIMARY;
     location->replicaPeer = nullptr;
     // Create a simple StorageInfo
     location->storageInfo = std::make_unique<StorageInfo>();
     location->storageInfo->type = StorageInfo::MEMORY;
     return std::shared_ptr<const PartitionLocation>(location.release());
   }
 };
 
 } // namespace
 
 class ShuffleClientCompressionTest : public testing::Test {
  protected:
   void SetUp() override {
     testData_ = std::vector<uint8_t>(1024);
     // Fill with compressible data
     for (size_t i = 0; i < testData_.size(); i++) {
       testData_[i] = static_cast<uint8_t>(i % 256);
     }
   }
 
   conf::CelebornConf createConfWithCompression(
       protocol::CompressionCodec codec) {
     conf::CelebornConf conf;
     conf.registerProperty(
         conf::CelebornConf::kShuffleCompressionCodec,
         std::string(protocol::toString(codec)));
     conf.registerProperty(
         conf::CelebornConf::kClientPushMaxReqsInFlightTotal, "100");
     conf.registerProperty(
         conf::CelebornConf::kClientPushMaxReqsInFlightPerWorker, "100");
     return conf;
   }
 
   std::vector<uint8_t> testData_;
   static constexpr int kBatchHeaderSize = 4 * 4;
   static constexpr int kTestShuffleId = 1000;
   static constexpr int kTestMapId = 1001;
   static constexpr int kTestAttemptId = 1002;
   static constexpr int kTestPartitionId = 0;
   static constexpr int kTestNumMappers = 1;
   static constexpr int kTestNumPartitions = 1;
 };
 
 // Test 1: Verify no compression when disabled
 TEST_F(ShuffleClientCompressionTest, pushDataWithNoCompression) {
   auto conf = createConfWithCompression(protocol::CompressionCodec::NONE);
   auto client = MockShuffleClient::create(conf, kTestShuffleId);
 
   int pushDataLen = client->pushData(
       kTestShuffleId,
       kTestMapId,
       kTestAttemptId,
       kTestPartitionId,
       testData_.data(),
       0,
       testData_.size(),
       kTestNumMappers,
       kTestNumPartitions);
 
   EXPECT_EQ(pushDataLen, static_cast<int>(testData_.size()) + kBatchHeaderSize);
 
   auto mockFactory = client->getMockClientFactory();
   auto mockClient = mockFactory->getLastClient();
   ASSERT_NE(mockClient, nullptr);
   auto body = mockClient->getCapturedBody();
   ASSERT_NE(body, nullptr);
   EXPECT_EQ(body->remainingSize(), static_cast<size_t>(pushDataLen));
 }
 
 // Test 2: Verify LZ4 compression is applied and compressed size is used
 TEST_F(ShuffleClientCompressionTest, pushDataWithLz4Compression) {
   auto conf = createConfWithCompression(protocol::CompressionCodec::LZ4);
   auto client = MockShuffleClient::create(conf, kTestShuffleId);
 
   compress::Lz4Compressor compressor;
   const size_t dstCapacity = compressor.getDstCapacity(testData_.size());
   std::vector<uint8_t> compressedBuffer(dstCapacity);
   const size_t compressedSize = compressor.compress(
       testData_.data(), 0, testData_.size(), compressedBuffer.data(), 0);
 
   int pushDataLen = client->pushData(
       kTestShuffleId,
       kTestMapId,
       kTestAttemptId,
       kTestPartitionId,
       testData_.data(),
       0,
       testData_.size(),
       kTestNumMappers,
       kTestNumPartitions);
 
   EXPECT_EQ(
       pushDataLen, static_cast<int>(compressedSize) + kBatchHeaderSize);
   EXPECT_LT(pushDataLen, static_cast<int>(testData_.size()) + kBatchHeaderSize);
 
   auto mockFactory = client->getMockClientFactory();
   auto mockClient = mockFactory->getLastClient();
   ASSERT_NE(mockClient, nullptr);
   auto body = mockClient->getCapturedBody();
   ASSERT_NE(body, nullptr);
   EXPECT_EQ(body->remainingSize(), static_cast<size_t>(pushDataLen));
 
   body->skip(12);
   int lengthInHeader = body->readLE<int>();
   EXPECT_EQ(lengthInHeader, static_cast<int>(compressedSize));
 
   body->retreat(16);
   body->skip(kBatchHeaderSize);
   std::vector<uint8_t> compressedDataBuffer(lengthInHeader);
   body->readToBuffer(compressedDataBuffer.data(), lengthInHeader);
   const uint8_t* compressedDataPtr = compressedDataBuffer.data();
 
   compress::Lz4Decompressor decompressor;
   const int originalLen = decompressor.getOriginalLen(compressedDataPtr);
   EXPECT_EQ(originalLen, static_cast<int>(testData_.size()));
 
   std::vector<uint8_t> decompressedData(originalLen);
   const bool success = decompressor.decompress(
       compressedDataPtr, decompressedData.data(), 0);
   EXPECT_TRUE(success);
   EXPECT_EQ(
       std::vector<uint8_t>(testData_.begin(), testData_.end()),
       decompressedData);
 }
 
 // Test 3: Verify ZSTD compression is applied and compressed size is used
 TEST_F(ShuffleClientCompressionTest, pushDataWithZstdCompression) {
   auto conf = createConfWithCompression(protocol::CompressionCodec::ZSTD);
   auto client = MockShuffleClient::create(conf, kTestShuffleId);
 
   int pushDataLen = client->pushData(
       kTestShuffleId,
       kTestMapId,
       kTestAttemptId,
       kTestPartitionId,
       testData_.data(),
       0,
       testData_.size(),
       kTestNumMappers,
       kTestNumPartitions);
 
   EXPECT_LT(pushDataLen, static_cast<int>(testData_.size()) + kBatchHeaderSize);
 
   auto mockFactory = client->getMockClientFactory();
   auto mockClient = mockFactory->getLastClient();
   ASSERT_NE(mockClient, nullptr);
   auto body = mockClient->getCapturedBody();
   ASSERT_NE(body, nullptr);
   EXPECT_EQ(body->remainingSize(), static_cast<size_t>(pushDataLen));
 
   body->skip(12);
   int lengthInHeader = body->readLE<int>();
   EXPECT_LT(lengthInHeader, static_cast<int>(testData_.size()));
   EXPECT_EQ(pushDataLen, lengthInHeader + kBatchHeaderSize);
 
   body->retreat(16);
   body->skip(kBatchHeaderSize);
   std::vector<uint8_t> compressedDataBuffer(lengthInHeader);
   const size_t bytesRead = body->readToBuffer(compressedDataBuffer.data(), lengthInHeader);
   EXPECT_EQ(bytesRead, static_cast<size_t>(lengthInHeader));
   const uint8_t* compressedDataPtr = compressedDataBuffer.data();
 
   if (lengthInHeader >= 4) {
     EXPECT_EQ(compressedDataPtr[0], 'Z');
     EXPECT_EQ(compressedDataPtr[1], 'S');
     EXPECT_EQ(compressedDataPtr[2], 'T');
     EXPECT_EQ(compressedDataPtr[3], 'D');
   }
 
   compress::ZstdDecompressor decompressor;
   const int originalLen = decompressor.getOriginalLen(compressedDataPtr);
   EXPECT_GT(originalLen, 0); // Sanity check
   EXPECT_EQ(originalLen, static_cast<int>(testData_.size()));
 
   std::vector<uint8_t> decompressedData(originalLen);
   const bool success = decompressor.decompress(
       compressedDataPtr, decompressedData.data(), 0);
   EXPECT_TRUE(success);
   EXPECT_EQ(
       std::vector<uint8_t>(testData_.begin(), testData_.end()),
       decompressedData);
 }
 
 // Test 4: Verify batchBytesSize uses compressed size (indirectly via pushDataLen)
 TEST_F(ShuffleClientCompressionTest, batchBytesSizeUsesCompressedSize) {
   auto conf = createConfWithCompression(protocol::CompressionCodec::LZ4);
   auto client = MockShuffleClient::create(conf, kTestShuffleId);
 
   compress::Lz4Compressor compressor;
   const size_t dstCapacity = compressor.getDstCapacity(testData_.size());
   std::vector<uint8_t> compressedBuffer(dstCapacity);
   const size_t compressedSize = compressor.compress(
       testData_.data(), 0, testData_.size(), compressedBuffer.data(), 0);
 
   int pushDataLen = client->pushData(
       kTestShuffleId,
       kTestMapId,
       kTestAttemptId,
       kTestPartitionId,
       testData_.data(),
       0,
       testData_.size(),
       kTestNumMappers,
       kTestNumPartitions);
 
   EXPECT_EQ(
       pushDataLen, static_cast<int>(compressedSize) + kBatchHeaderSize);
   EXPECT_LT(pushDataLen, static_cast<int>(testData_.size()) + kBatchHeaderSize);
 }
 