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

package org.apache.celeborn.service.deploy.master;

import static org.apache.celeborn.service.deploy.master.SlotsAllocatorSuiteJ.*;
import static org.openjdk.jmh.annotations.Mode.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.StorageInfo;

public class SlotsAllocatorJmhBenchmark {

  private static final int NUM_WORKERS = 1500;
  private static final long PARTITION_SIZE = 64 * 1024 * 1024;
  private static final String DISK_PATH = "/mnt/disk";
  private static final long DISK_SPACE = 512 * 1024 * 1024L;
  private static final boolean HAS_DISKS = true;
  private static final int NUM_NETWORK_LOCATIONS = 20;
  private static final int NUM_PARTITIONS = 100000;

  @State(Scope.Thread)
  public static class BenchmarkState {
    List<WorkerInfo> workers;
    List<Integer> partitionIds =
        Collections.unmodifiableList(
            IntStream.range(0, NUM_PARTITIONS).boxed().collect(Collectors.toList()));

    @Setup
    public void initialize() {
      Map<String, Long> diskPartitionToSize = new HashMap<>();
      diskPartitionToSize.put(DISK_PATH, DISK_SPACE);
      workers =
          basePrepareWorkers(
              NUM_WORKERS,
              HAS_DISKS,
              diskPartitionToSize,
              PARTITION_SIZE,
              NUM_NETWORK_LOCATIONS,
              new Random());
    }
  }

  @Benchmark
  @Fork(1)
  @Threads(Threads.MAX)
  @BenchmarkMode(AverageTime)
  @Warmup(iterations = 5, time = 5)
  @Measurement(time = 60, timeUnit = TimeUnit.SECONDS, iterations = 5)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmarkSlotSelection(Blackhole blackhole, BenchmarkState state) {

    blackhole.consume(
        SlotsAllocator.offerSlotsRoundRobin(
            state.workers, state.partitionIds, true, true, StorageInfo.ALL_TYPES_AVAILABLE_MASK));
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
