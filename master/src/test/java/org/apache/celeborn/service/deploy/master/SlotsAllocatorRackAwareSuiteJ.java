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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.TableMapping;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.master.network.CelebornRackResolver;

public class SlotsAllocatorRackAwareSuiteJ {

  private static final int NUM_ATTEMPTS =
      Integer.getInteger("SlotsAllocatorRackAwareSuiteJ.NUM_ATTEMPTS", 100);

  @Test
  public void offerSlotsRoundRobinWithRackAware() throws IOException {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_RESERVE_SLOTS_RACKAWARE_ENABLED().key(), "true");

    File mapFile = File.createTempFile("testResolve1", ".txt");
    Writer mapFileWriter = Files.newBufferedWriter(mapFile.toPath(), StandardCharsets.UTF_8);
    mapFileWriter.write(
        "host1 /default/rack1\nhost2 /default/rack1\nhost3 /default/rack1\n"
            + "host4 /default/rack2\nhost5 /default/rack2\nhost6 /default/rack2\n");
    mapFileWriter.flush();
    mapFileWriter.close();
    mapFile.deleteOnExit();

    conf.set(
        "celeborn.hadoop." + NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        TableMapping.class.getName());
    conf.set("celeborn.hadoop." + NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath());
    CelebornRackResolver resolver = new CelebornRackResolver(conf);

    List<Integer> partitionIds = new ArrayList<Integer>();
    partitionIds.add(0);
    partitionIds.add(1);
    partitionIds.add(2);

    List<WorkerInfo> workers = prepareWorkers(resolver);

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        SlotsAllocator.offerSlotsRoundRobin(
            workers, partitionIds, true, true, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    Consumer<PartitionLocation> assertCustomer =
        new Consumer<PartitionLocation>() {
          @Override
          public void accept(PartitionLocation location) {
            Assert.assertNotEquals(
                resolver.resolve(location.getHost()).getNetworkLocation(),
                resolver.resolve(location.getPeer().getHost()).getNetworkLocation());
          }
        };
    slots.values().stream().map(Tuple2::_1).flatMap(Collection::stream).forEach(assertCustomer);
  }

  @Test
  public void offerSlotsRoundRobinWithRackAwareWithoutMappingFile() throws IOException {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_RESERVE_SLOTS_RACKAWARE_ENABLED().key(), "true");

    File mapFile = File.createTempFile("testResolve1", ".txt");
    mapFile.delete();

    conf.set(
        "celeborn.hadoop." + NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        TableMapping.class.getName());
    conf.set("celeborn.hadoop." + NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath());
    CelebornRackResolver resolver = new CelebornRackResolver(conf);

    List<Integer> partitionIds = new ArrayList<Integer>();
    partitionIds.add(0);
    partitionIds.add(1);
    partitionIds.add(2);

    List<WorkerInfo> workers = prepareWorkers(resolver);

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        SlotsAllocator.offerSlotsRoundRobin(
            workers, partitionIds, true, true, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

    Consumer<PartitionLocation> assertConsumer =
        new Consumer<PartitionLocation>() {
          @Override
          public void accept(PartitionLocation location) {
            Assert.assertEquals(
                NetworkTopology.DEFAULT_RACK,
                resolver.resolve(location.getHost()).getNetworkLocation());
            Assert.assertEquals(
                NetworkTopology.DEFAULT_RACK,
                resolver.resolve(location.getPeer().getHost()).getNetworkLocation());
          }
        };
    slots.values().stream().map(Tuple2::_1).flatMap(Collection::stream).forEach(assertConsumer);
  }

  private List<WorkerInfo> prepareWorkers(CelebornRackResolver resolver) {
    ArrayList<WorkerInfo> workers = new ArrayList<>(3);
    workers.add(new WorkerInfo("host1", 9, 10, 110, 113, new HashMap<>(), null));
    workers.add(new WorkerInfo("host2", 9, 11, 111, 114, new HashMap<>(), null));
    workers.add(new WorkerInfo("host3", 9, 12, 112, 115, new HashMap<>(), null));
    workers.add(new WorkerInfo("host4", 9, 10, 110, 113, new HashMap<>(), null));
    workers.add(new WorkerInfo("host5", 9, 11, 111, 114, new HashMap<>(), null));
    workers.add(new WorkerInfo("host6", 9, 12, 112, 115, new HashMap<>(), null));

    workers.forEach(
        new Consumer<WorkerInfo>() {
          @Override
          public void accept(WorkerInfo workerInfo) {
            workerInfo.networkLocation_$eq(
                resolver.resolve(workerInfo.host()).getNetworkLocation());
          }
        });
    return workers;
  }

  @Test
  public void testRackAwareRoundRobinReplicaPatterns() {
    for (Tuple2<Integer, List<WorkerInfo>> tuple :
        Arrays.asList(
            // This is a specific case we analyzed which was resulting in degenerate
            // behavior for replicas
            Tuple2.apply(
                100,
                Arrays.asList(
                    // h1r1, h2r1, h3r1, h4r2, h5r2, h6r2
                    WorkersSupplier.initWorker("h1r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h2r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h3r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h4r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h5r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h6r2", "/rack/r2"))),
            // This is a specific case we observed in production which triggered
            // suboptimal replica selection
            Tuple2.apply(
                20,
                Arrays.asList(
                    // h1r2, h4r2, h3r1, h1r1, h4r1, h2r1, h3r2, h2r2, h5r2, h5r1
                    WorkersSupplier.initWorker("h1r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h4r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h3r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h1r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h4r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h2r1", "/rack/r1"),
                    WorkersSupplier.initWorker("h3r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h2r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h5r2", "/rack/r2"),
                    WorkersSupplier.initWorker("h5r1", "/rack/r1"))))) {

      int numPartitions = tuple._1();
      List<WorkerInfo> workers = tuple._2();
      int maxReplicaCount = (numPartitions / workers.size()) + 1;
      List<Integer> partitionIds =
          IntStream.range(0, numPartitions).boxed().collect(Collectors.toList());

      Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
          SlotsAllocator.offerSlotsRoundRobin(
              workers, partitionIds, true, true, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

      Map<String, Long> numReplicaPerHost =
          slots.entrySet().stream()
              .flatMap(v -> v.getValue()._2.stream().map(PartitionLocation::getHost))
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

      for (String host : workers.stream().map(WorkerInfo::host).collect(Collectors.toList())) {
        long numReplicas = numReplicaPerHost.getOrDefault(host, 0L);
        Assert.assertTrue(
            "host = " + host + ", numReplicaPerHost = " + numReplicaPerHost + ", slots = " + slots,
            numReplicas <= maxReplicaCount);
      }
    }
  }

  @Test
  public void testRackAwareRoundRobinReplicaDistribution() {

    List<SlotReplicaAllocatorTestCase> allTests = getSlotReplicaAllocatorTestCases();

    for (final SlotReplicaAllocatorTestCase test : allTests) {

      final int numPartitions = test.getNumPartitions();
      long maxValue = Long.MIN_VALUE;
      List<WorkerInfo> maxValueWorkers = null;
      Map<String, Long> maxValueNumReplicaPerHost = null;
      List<Integer> partitionIds =
          Collections.unmodifiableList(
              IntStream.range(0, numPartitions).boxed().collect(Collectors.toList()));

      // For each test, run NUM_ATTEMPTS times and pick the worst case.
      for (int attempt = 0; attempt < NUM_ATTEMPTS; attempt++) {

        // workers will be randomized
        List<WorkerInfo> workers = test.generateWorkers();

        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
            SlotsAllocator.offerSlotsRoundRobin(
                workers, partitionIds, true, true, StorageInfo.ALL_TYPES_AVAILABLE_MASK);

        Map<String, Long> numReplicaPerHost =
            slots.entrySet().stream()
                .flatMap(v -> v.getValue()._2.stream().map(PartitionLocation::getHost))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Comparator<Map.Entry<String, Long>> entryComparator =
            Comparator.comparingLong(Map.Entry::getValue);

        Map.Entry<String, Long> maxEntry =
            Collections.max(numReplicaPerHost.entrySet(), entryComparator);

        if (maxEntry.getValue() > maxValue) {
          maxValue = maxEntry.getValue();
          maxValueWorkers = workers;
          maxValueNumReplicaPerHost = numReplicaPerHost;
        }
      }

      Assert.assertNotNull(maxValueWorkers);

      for (String host :
          maxValueWorkers.stream().map(WorkerInfo::host).collect(Collectors.toList())) {
        long numReplicas = maxValueNumReplicaPerHost.getOrDefault(host, 0L);
        Assert.assertTrue(
            "host = "
                + host
                + ", workerHosts = "
                + maxValueWorkers.stream().map(WorkerInfo::host).collect(Collectors.toList())
                + ", replicaHosts = "
                + SlotsAllocator.generateRackAwareWorkers(maxValueWorkers).stream()
                    .map(WorkerInfo::host)
                    .collect(Collectors.toList())
                + ", numReplicaPerHost = "
                + maxValueNumReplicaPerHost
                + ", test = "
                + test,
            numReplicas <= test.getExpectedMaxSlotsPerHost());
      }
    }
  }

  private static List<SlotReplicaAllocatorTestCase> getSlotReplicaAllocatorTestCases() {
    List<SlotReplicaAllocatorTestCase> equalHostsPerRackTests =
        // equal number of hosts per rack
        Arrays.asList(
            new SlotReplicaAllocatorTestCase(5, 2, 50),
            new SlotReplicaAllocatorTestCase(5, 2, 100),
            new SlotReplicaAllocatorTestCase(5, 2, 200),
            new SlotReplicaAllocatorTestCase(5, 2, 1000),
            new SlotReplicaAllocatorTestCase(5, 2, 10000),
            new SlotReplicaAllocatorTestCase(5, 2, 50000),
            new SlotReplicaAllocatorTestCase(5, 3, 50),
            new SlotReplicaAllocatorTestCase(5, 3, 100),
            new SlotReplicaAllocatorTestCase(5, 3, 200),
            new SlotReplicaAllocatorTestCase(5, 3, 1000),
            new SlotReplicaAllocatorTestCase(5, 3, 10000),
            new SlotReplicaAllocatorTestCase(5, 3, 50000),
            new SlotReplicaAllocatorTestCase(10, 5, 200),
            new SlotReplicaAllocatorTestCase(10, 5, 1000),
            new SlotReplicaAllocatorTestCase(10, 5, 10000),
            new SlotReplicaAllocatorTestCase(10, 5, 50000),
            new SlotReplicaAllocatorTestCase(20, 10, 1000),
            new SlotReplicaAllocatorTestCase(20, 10, 10000),
            new SlotReplicaAllocatorTestCase(20, 10, 50000));

    List<SlotReplicaAllocatorTestCase> unequalHostsPerRackTests =
        Arrays.asList(
            // specified number of hosts per rack
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 50),
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 100),
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 200),
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 1000),
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 10000),
            new SlotReplicaAllocatorTestCase(Arrays.asList(3, 7), 50000),
            new SlotReplicaAllocatorTestCase(Arrays.asList(2, 3, 5), 100),
            new SlotReplicaAllocatorTestCase(Arrays.asList(2, 3, 5), 50000),
            new SlotReplicaAllocatorTestCase(Arrays.asList(2, 3, 5, 5, 10), 100),
            new SlotReplicaAllocatorTestCase(Arrays.asList(2, 3, 5, 5, 10), 50000));

    List<SlotReplicaAllocatorTestCase> allTests = new ArrayList<>();
    allTests.addAll(equalHostsPerRackTests);
    allTests.addAll(unequalHostsPerRackTests);
    return allTests;
  }

  private static class WorkersSupplier implements Supplier<List<WorkerInfo>> {
    private static final Random RAND = new Random(42);

    private final String message;
    private final List<WorkerInfo> workerList;

    WorkersSupplier(int numHostsPerRack, int numRacks) {
      this(
          String.format("Equal hosts(%d) per rack(%d)", numHostsPerRack, numRacks),
          equalHostsPerRackWorkers(numHostsPerRack, numRacks));
    }

    WorkersSupplier(List<Integer> hostsPerRack) {
      this("Hosts per rack = " + hostsPerRack, specifiedHostsPerRackWorkers(hostsPerRack));
    }

    WorkersSupplier(String message, List<WorkerInfo> workerList) {
      this.message = message;
      this.workerList = Collections.unmodifiableList(workerList);
    }

    private static List<WorkerInfo> equalHostsPerRackWorkers(int numHostsPerRack, int numRacks) {
      List<WorkerInfo> workers = new ArrayList<>(numRacks * numHostsPerRack);
      for (int rack = 1; rack <= numRacks; rack++) {
        final String rackName = "/rack/r" + rack;
        for (int host = 1; host <= numHostsPerRack; host++) {
          final String hostName = "h" + host + "r" + rack;
          workers.add(initWorker(hostName, rackName));
        }
      }
      return workers;
    }

    private static List<WorkerInfo> specifiedHostsPerRackWorkers(List<Integer> hostsPerRack) {
      List<WorkerInfo> workers = new ArrayList<>();

      int rack = 0;
      for (int numHosts : hostsPerRack) {
        rack++;
        final String rackName = "/rack/r" + rack;
        for (int host = 1; host <= numHosts; host++) {
          final String hostName = "h" + host + "r" + rack;
          workers.add(initWorker(hostName, rackName));
        }
      }

      return workers;
    }

    @Override
    public List<WorkerInfo> get() {
      List<WorkerInfo> result = new ArrayList<>(workerList);
      Collections.shuffle(result, RAND);
      return result;
    }

    @Override
    public String toString() {
      return "WorkersSupplier{" + message + "}";
    }

    private static WorkerInfo initWorker(String host, String rack) {
      long assumedPartitionSize = 64 * 1024 * 1024;
      DiskInfo diskInfo = new DiskInfo("/mnt/a", 1024L * 1024L * 1024L * 1024L, 1, 1, 0);
      diskInfo.maxSlots_$eq(diskInfo.actualUsableSpace() / assumedPartitionSize);

      Map<String, DiskInfo> diskInfoMap = new HashMap<>(2);
      diskInfoMap.put(diskInfo.mountPoint(), diskInfo);

      WorkerInfo workerInfo =
          new WorkerInfo(host, 1, 2, 3, 4, Collections.unmodifiableMap(diskInfoMap), null);
      workerInfo.networkLocation_$eq(rack);
      return workerInfo;
    }
  }

  private static class SlotReplicaAllocatorTestCase {

    private final Supplier<List<WorkerInfo>> workersSupplier;
    // Total number of partitions
    private final int numPartitions;

    // The maximum number of slots per replica we are expecting
    private final int expectedMaxSlotsPerHost;

    SlotReplicaAllocatorTestCase(int numHostsPerRack, int numRacks, int numPartitions) {
      this(
          new WorkersSupplier(numHostsPerRack, numRacks),
          numPartitions,
          (int) Math.ceil(((double) numPartitions) / (numHostsPerRack * numRacks)));
    }

    SlotReplicaAllocatorTestCase(List<Integer> hostsPerRack, int numPartitions) {
      Collections.sort(hostsPerRack);
      int maxHostsPerRack = hostsPerRack.get(hostsPerRack.size() - 1);
      int secondMaxHostsPerRack = hostsPerRack.get(hostsPerRack.size() - 2);
      int totalHosts = hostsPerRack.stream().mapToInt(Integer::intValue).sum();
      int expected =
          (int)
              Math.ceil(
                  ((double) numPartitions
                      / totalHosts
                      * (1
                          + ((double) (maxHostsPerRack - secondMaxHostsPerRack + 1))
                              / (totalHosts - maxHostsPerRack))));
      this.workersSupplier = new WorkersSupplier(hostsPerRack);
      this.numPartitions = numPartitions;
      this.expectedMaxSlotsPerHost = expected;
    }

    SlotReplicaAllocatorTestCase(
        Supplier<List<WorkerInfo>> workersSupplier,
        int numPartitions,
        int expectedMaxSlotsPerHost) {
      this.workersSupplier = workersSupplier;
      this.numPartitions = numPartitions;
      this.expectedMaxSlotsPerHost = expectedMaxSlotsPerHost;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    public int getExpectedMaxSlotsPerHost() {
      return expectedMaxSlotsPerHost;
    }

    public List<WorkerInfo> generateWorkers() {
      return workersSupplier.get();
    }

    @Override
    public String toString() {
      return "SlotReplicaAllocatorTestCase{"
          + "workersSupplier="
          + workersSupplier
          + ", numPartitions="
          + numPartitions
          + ", expectedMaxSlotsPerHost="
          + expectedMaxSlotsPerHost
          + '}';
    }
  }
}
