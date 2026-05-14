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

package org.apache.celeborn.service.deploy.master

import java.util
import java.util.{HashMap => JHashMap, Random}
import java.util.stream.{Collectors, IntStream}

import org.apache.celeborn.benchmark.{Benchmark, BenchmarkBase}
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.StorageInfo

/**
 * SlotsAllocator roundRobin benchmark.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "celeborn-master/test:runMain
 *      org.apache.celeborn.service.deploy.master.SlotsAllocatorBenchmark"
 *   2. generate result:
 *      CELEBORN_GENERATE_BENCHMARK_FILES=1 build/sbt "celeborn-master/test:runMain
 *      org.apache.celeborn.service.deploy.master.SlotsAllocatorBenchmark"
 *      Results will be written to "benchmarks/SlotsAllocatorBenchmark-results.txt".
 * }}}
 */
object SlotsAllocatorBenchmark extends BenchmarkBase {

  private val PARTITION_SIZE = 64 * 1024 * 1024L
  private val DISK_PATH = "/mnt/disk"
  private val DISK_SPACE = 1024L * 1024 * 1024 * 1024
  private val NUM_NETWORK_LOCATIONS = 20
  private val random = new Random(42)

  private def prepareWorkers(numWorkers: Int): util.List[WorkerInfo] = {
    val diskPartitionToSize = new JHashMap[String, java.lang.Long]()
    diskPartitionToSize.put(DISK_PATH, java.lang.Long.valueOf(DISK_SPACE))
    SlotsAllocatorSuiteJ.basePrepareWorkers(
      numWorkers,
      true,
      diskPartitionToSize,
      PARTITION_SIZE,
      NUM_NETWORK_LOCATIONS,
      false,
      random)
  }

  private def preparePartitionIds(numPartitions: Int): util.List[Integer] = {
    java.util.Collections.unmodifiableList(
      IntStream.range(0, numPartitions).boxed().collect(Collectors.toList()))
  }

  private def benchmarkRoundRobin(
      name: String,
      numWorkers: Int,
      numPartitions: Int,
      shouldReplicate: Boolean): Unit = {
    runBenchmark(name) {
      val benchmark = new Benchmark(name, numPartitions, output = output)

      benchmark.addTimerCase("offerSlotsRoundRobin") { timer =>
        val workers = prepareWorkers(numWorkers)
        val partitionIds = preparePartitionIds(numPartitions)
        timer.startTiming()
        SlotsAllocator.offerSlotsRoundRobin(
          workers,
          partitionIds,
          shouldReplicate,
          false,
          StorageInfo.ALL_TYPES_AVAILABLE_MASK,
          false,
          0)
        timer.stopTiming()
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkRoundRobin(
      "200 workers, 10K partitions, no replication",
      200,
      10000,
      shouldReplicate = false)
    benchmarkRoundRobin(
      "200 workers, 100K partitions, no replication",
      200,
      100000,
      shouldReplicate = false)
    benchmarkRoundRobin(
      "500 workers, 100K partitions, with replication",
      500,
      100000,
      shouldReplicate = true)
    benchmarkRoundRobin(
      "500 workers, 2M partitions, no replication",
      500,
      2000000,
      shouldReplicate = false)
    benchmarkRoundRobin(
      "1000 workers, 500K partitions, with replication",
      1000,
      500000,
      shouldReplicate = true)
  }
}
