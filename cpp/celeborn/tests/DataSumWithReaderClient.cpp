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

#include <cstdio>
#include <fstream>
#include <iostream>

#include <celeborn/client/ShuffleClient.h>

int main(int argc, char** argv) {
  // Read the configs.
  assert(argc == 8);
  std::string lifecycleManagerHost = argv[1];
  int lifecycleManagerPort = std::atoi(argv[2]);
  std::string appUniqueId = argv[3];
  int shuffleId = std::atoi(argv[4]);
  int attemptId = std::atoi(argv[5]);
  int numPartitions = std::atoi(argv[6]);
  std::string resultFile = argv[7];
  std::cout << "lifecycleManagerHost = " << lifecycleManagerHost
            << ", lifecycleManagerPort = " << lifecycleManagerPort
            << ", appUniqueId = " << appUniqueId
            << ", shuffleId = " << shuffleId << ", attemptId = " << attemptId
            << ", numPartitions = " << numPartitions
            << ", resultFile = " << resultFile << std::endl;

  // Create shuffleClient and setup.
  auto conf = std::make_shared<celeborn::conf::CelebornConf>();
  auto clientFactory =
      std::make_shared<celeborn::network::TransportClientFactory>(conf);
  auto shuffleClient = std::make_unique<celeborn::client::ShuffleClientImpl>(
      appUniqueId, conf, clientFactory);
  shuffleClient->setupLifecycleManagerRef(
      lifecycleManagerHost, lifecycleManagerPort);

  // Read data, parse data and sum up.
  std::vector<long> result(numPartitions, 0);
  shuffleClient->updateReducerFileGroup(shuffleId);
  for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
    auto inputStream = shuffleClient->readPartition(
        shuffleId, partitionId, attemptId, 0, INT_MAX);
    char c;
    long data = 0;
    int dataCnt = 0;
    while (inputStream->read((uint8_t*)&c, 0, 1) > 0) {
      if (c == '-') {
        result[partitionId] += data;
        data = 0;
        dataCnt++;
        continue;
      }
      assert(c >= '0' && c <= '9');
      data *= 10;
      data += c - '0';
    }
    result[partitionId] += data;
    std::cout << "partition " << partitionId
              << " sum result = " << result[partitionId]
              << ", dataCnt = " << dataCnt << std::endl;
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
