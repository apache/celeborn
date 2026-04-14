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

#include <folly/init/Init.h>
#include <cstdio>
#include <fstream>
#include <iostream>

#include <celeborn/client/ShuffleClient.h>

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);
  // Read the configs.
  assert(argc == 10);
  std::string lifecycleManagerHost = argv[1];
  int lifecycleManagerPort = std::atoi(argv[2]);
  std::string appUniqueId = argv[3];
  int shuffleId = std::atoi(argv[4]);
  int attemptId = std::atoi(argv[5]);
  int numMappers = std::atoi(argv[6]);
  int numPartitions = std::atoi(argv[7]);
  std::string resultFile = argv[8];
  std::string compressCodec = argv[9];
  std::cout << "lifecycleManagerHost = " << lifecycleManagerHost
            << ", lifecycleManagerPort = " << lifecycleManagerPort
            << ", appUniqueId = " << appUniqueId
            << ", shuffleId = " << shuffleId << ", attemptId = " << attemptId
            << ", numMappers = " << numMappers
            << ", numPartitions = " << numPartitions
            << ", resultFile = " << resultFile
            << ", compressCodec = " << compressCodec << std::endl;

  // Create shuffleClient and setup.
  auto conf = std::make_shared<celeborn::conf::CelebornConf>();
  conf->registerProperty(
      celeborn::conf::CelebornConf::kShuffleCompressionCodec, compressCodec);
  auto clientEndpoint =
      std::make_shared<celeborn::client::ShuffleClientEndpoint>(conf);
  auto shuffleClient = celeborn::client::ShuffleClientImpl::create(
      appUniqueId, conf, *clientEndpoint);
  shuffleClient->setupLifecycleManagerRef(
      lifecycleManagerHost, lifecycleManagerPort);

  long maxData = 1000000;
  size_t numData = 1000;
  // Generate data, sum up and pushData.
  std::vector<long> result(numPartitions, 0);
  std::vector<size_t> dataCnt(numPartitions, 0);
  for (int mapId = 0; mapId < numMappers; mapId++) {
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      std::string partitionData;
      for (size_t i = 0; i < numData; i++) {
        int data = std::rand() % maxData;
        result[partitionId] += data;
        dataCnt[partitionId]++;
        partitionData += "-" + std::to_string(data);
      }
      shuffleClient->pushData(
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          reinterpret_cast<const uint8_t*>(partitionData.c_str()),
          0,
          partitionData.size(),
          numMappers,
          numPartitions);
    }
    shuffleClient->mapperEnd(shuffleId, mapId, attemptId, numMappers);
  }
  for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
    std::cout << "partition " << partitionId
              << " sum result = " << result[partitionId]
              << ", dataCnt = " << dataCnt[partitionId] << std::endl;
  }

  // Write result to resultFile.
  remove(resultFile.c_str());
  std::ofstream of(resultFile);
  for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
    of << result[partitionId] << std::endl;
  }
  of.close();

  return 0;
}
