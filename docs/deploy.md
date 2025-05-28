---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Deploy Celeborn

1. Unzip the tarball to `$CELEBORN_HOME`.
2. Modify environment variables in `$CELEBORN_HOME/conf/celeborn-env.sh`.

EXAMPLE:
```properties
#!/usr/bin/env bash
CELEBORN_MASTER_MEMORY=4g
CELEBORN_WORKER_MEMORY=2g
CELEBORN_WORKER_OFFHEAP_MEMORY=4g
```
3. Modify configurations in `$CELEBORN_HOME/conf/celeborn-defaults.conf`.

EXAMPLE: single master cluster
```properties
# used by client and worker to connect to master
celeborn.master.endpoints clb-master:9097

# used by master to bootstrap
celeborn.master.host clb-master
celeborn.master.port 9097

celeborn.metrics.enabled true
celeborn.worker.flusher.buffer.size 256k

# If Celeborn workers have local disks and HDFS. Following configs should be added.
# If Celeborn workers have local disks, use following config.
# Disk type is HDD by default.
celeborn.worker.storage.dirs /mnt/disk1:disktype=SSD,/mnt/disk2:disktype=SSD

# If Celeborn workers don't have local disks. You can use HDFS.
# Do not set `celeborn.worker.storage.dirs` and use following configs.
celeborn.storage.availableTypes HDFS
celeborn.worker.sortPartition.threads 64
celeborn.worker.commitFiles.timeout 240s
celeborn.worker.commitFiles.threads 128
celeborn.master.slot.assign.policy roundrobin
celeborn.rpc.askTimeout 240s
celeborn.worker.flusher.hdfs.buffer.size 4m
celeborn.storage.hdfs.dir hdfs://<namenode>/celeborn
celeborn.worker.replicate.fastFail.duration 240s
# Either principal/keytab or valid TGT cache is required to access kerberized HDFS
celeborn.storage.hdfs.kerberos.principal user@REALM
celeborn.storage.hdfs.kerberos.keytab /path/to/user.keytab

# If your hosts have disk raid or use lvm, set `celeborn.worker.monitor.disk.enabled` to false
celeborn.worker.monitor.disk.enabled false
```   

EXAMPLE: HA cluster
```properties
# used by client and worker to connect to master
celeborn.master.endpoints clb-1:9097,clb-2:9097,clb-3:9097

# used by master nodes to bootstrap, every node should know the topology of whole cluster, for each node,
# `celeborn.master.ha.node.id` should be unique, and `celeborn.master.ha.node.<id>.host` is required.
celeborn.master.ha.enabled true
celeborn.master.ha.node.1.host clb-1
celeborn.master.ha.node.1.port 9097
celeborn.master.ha.node.1.ratis.port 9872
celeborn.master.ha.node.2.host clb-2
celeborn.master.ha.node.2.port 9097
celeborn.master.ha.node.2.ratis.port 9872
celeborn.master.ha.node.3.host clb-3
celeborn.master.ha.node.3.port 9097
celeborn.master.ha.node.3.ratis.port 9872
celeborn.master.ha.ratis.raft.server.storage.dir /mnt/disk1/celeborn_ratis/

celeborn.metrics.enabled true
# If you want to use HDFS as shuffle storage, make sure that flush buffer size is at least 4MB or larger.
celeborn.worker.flusher.buffer.size 256k

# If Celeborn workers have local disks and HDFS. Following configs should be added.
# Celeborn will use local disks until local disk become unavailable to gain the best performance.
# Increase Celeborn's off-heap memory if Celeborn write to HDFS.
# If Celeborn workers have local disks, use following config.
# Disk type is HDD by default.
celeborn.worker.storage.dirs /mnt/disk1:disktype=SSD,/mnt/disk2:disktype=SSD

# If Celeborn workers don't have local disks. You can use HDFS.
# Do not set `celeborn.worker.storage.dirs` and use following configs.
celeborn.storage.availableTypes HDFS
celeborn.worker.sortPartition.threads 64
celeborn.worker.commitFiles.timeout 240s
celeborn.worker.commitFiles.threads 128
celeborn.master.slot.assign.policy roundrobin
celeborn.rpc.askTimeout 240s
celeborn.worker.flusher.hdfs.buffer.size 4m
celeborn.storage.hdfs.dir hdfs://<namenode>/celeborn
celeborn.worker.replicate.fastFail.duration 240s

# If your hosts have disk raid or use lvm, set `celeborn.worker.monitor.disk.enabled` to false
celeborn.worker.monitor.disk.enabled false
```

Flink engine related configurations:
```properties
# If you are using Celeborn for flink, these settings will be needed.
celeborn.worker.directMemoryRatioForReadBuffer 0.4
celeborn.worker.directMemoryRatioToResume 0.5
# These setting will affect performance. 
# If there is enough off-heap memory, you can try to increase read buffers.
# Read buffer max memory usage for a data partition is `taskmanager.memory.segment-size * readBuffersMax`
celeborn.worker.partition.initial.readBuffersMin 512
celeborn.worker.partition.initial.readBuffersMax 1024
celeborn.worker.readBuffer.allocationWait 10ms
```

4. Copy Celeborn and configurations to all nodes.
5. Start all services. If you install Celeborn distribution in the same path on every node and your
   cluster can perform SSH login then you can fill `$CELEBORN_HOME/conf/hosts` and
   use `$CELEBORN_HOME/sbin/start-all.sh` to start all
   services. If the installation paths are not identical, you will need to start service manually.  
   Start Celeborn master  
   `$CELEBORN_HOME/sbin/start-master.sh`  
   Start Celeborn worker  
   `$CELEBORN_HOME/sbin/start-worker.sh`
6. If Celeborn starts success, the output of the Master's log should be like this:
```
22/10/08 19:29:11,805 INFO [main] Dispatcher: Dispatcher numThreads: 64
22/10/08 19:29:11,875 INFO [main] TransportClientFactory: mode NIO threads 64
22/10/08 19:29:12,057 INFO [main] Utils: Successfully started service 'Master' on port 9097.
22/10/08 19:29:12,113 INFO [main] Master: Metrics system enabled.
22/10/08 19:29:12,125 INFO [main] HttpServer: master: HttpServer started on port 9098.
22/10/08 19:29:12,126 INFO [main] Master: Master started.
22/10/08 19:29:57,842 INFO [dispatcher-event-loop-19] Master: Registered worker
Host: 192.168.15.140
RpcPort: 37359
PushPort: 38303
FetchPort: 37569
ReplicatePort: 37093
SlotsUsed: 0()
LastHeartbeat: 0
Disks: {/mnt/disk1=DiskInfo(maxSlots: 6679, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: /mnt/disk1, usableSpace: 448284381184, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk3=DiskInfo(maxSlots: 6716, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: /mnt/disk3, usableSpace: 450755608576, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk2=DiskInfo(maxSlots: 6713, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: /mnt/disk2, usableSpace: 450532900864, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk4=DiskInfo(maxSlots: 6712, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: /mnt/disk4, usableSpace: 450456805376, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs }
WorkerRef: null
```

## Deploy Spark client
Celeborn release binary contains clients for Spark 2.x and Spark 3.x, copy the corresponding client jar into Spark's
`jars/` directory:

Copy `$CELEBORN_HOME/spark/celeborn-client-spark-<spark.major.version>-shaded_<scala.binary.version>-<celeborn.version>.jar` to `$SPARK_HOME/jars/`.

### Spark Configuration
To use Celeborn, the following spark configurations should be added.
```properties
# Shuffle manager class name changed in 0.3.0:
#    before 0.3.0: `org.apache.spark.shuffle.celeborn.RssShuffleManager`
#    since 0.3.0: `org.apache.spark.shuffle.celeborn.SparkShuffleManager`
spark.shuffle.manager org.apache.spark.shuffle.celeborn.SparkShuffleManager
# must use kryo serializer because java serializer do not support relocation
spark.serializer org.apache.spark.serializer.KryoSerializer

# celeborn master
spark.celeborn.master.endpoints clb-1:9097,clb-2:9097,clb-3:9097
# This is not necessary if your Spark external shuffle service is Spark 3.1 or newer
spark.shuffle.service.enabled false

# options: hash, sort
# Hash shuffle writer use (partition count) * (celeborn.push.buffer.max.size) * (spark.executor.cores) memory.
# Sort shuffle writer uses less memory than hash shuffle writer, if your shuffle partition count is large, try to use sort hash writer.  
spark.celeborn.client.spark.shuffle.writer hash

# We recommend setting `spark.celeborn.client.push.replicate.enabled` to true to enable server-side data replication
# If you have only one worker, this setting must be false 
# If your Celeborn is using HDFS, it's recommended to set this setting to false
spark.celeborn.client.push.replicate.enabled true

# Support for Spark AQE only tested under Spark 3
# we recommend setting localShuffleReader to false for getting better performance of Celeborn
spark.sql.adaptive.localShuffleReader.enabled false

# If Celeborn is using HDFS
spark.celeborn.storage.availableTypes HDFS
spark.celeborn.storage.hdfs.dir hdfs://<namenode>/celeborn

# we recommend enabling aqe support to gain better performance
spark.sql.adaptive.enabled true
spark.sql.adaptive.skewJoin.enabled true

# Support Spark Dynamic Resource Allocation
# Required Spark version >= 3.5.0
spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO
# Required Spark version >= 3.4.0, highly recommended to disable
spark.dynamicAllocation.shuffleTracking.enabled false

# Support ShuffleManager when defined in user jars
# Required Spark version < 4.0.0 or without SPARK-45762, highly recommended to false for ShuffleManager in user-defined jar specified by --jars or spark.jars
spark.executor.userClassPathFirst false
```

## Deploy Flink client

**Important: Only Flink batch jobs are supported for now. Due to the Shuffle Service in Flink is cluster-granularity, if you want to use Celeborn in a session cluster, it will not be able to submit both streaming and batch job to the same cluster. We plan to get rid of this restriction for Hybrid Shuffle mode in a future release.**

Celeborn release binary contains clients for Flink 1.16.x, Flink 1.17.x, Flink 1.18.x, Flink 1.19.x, Flink 1.20.x and Flink 2.0.x, copy the corresponding client jar into Flink's
`lib/` directory:

Copy `$CELEBORN_HOME/flink/celeborn-client-flink-<flink.version>-shaded_<scala.binary.version>-<celeborn.version>.jar` to `$FLINK_HOME/lib/`.

### Flink Configuration
Celeborn supports two Flink integration strategies: remote shuffle service (since Flink 1.16) and [hybrid shuffle](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/batch/batch_shuffle/#hybrid-shuffle) (since Flink 1.20).

To use Celeborn, you can choose one of them and add the following Flink configurations.

#### Flink Remote Shuffle Service Configuration
```properties
shuffle-service-factory.class: org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory
execution.batch-shuffle-mode: ALL_EXCHANGES_BLOCKING
celeborn.master.endpoints: clb-1:9097,clb-2:9097,clb-3:9097

celeborn.client.shuffle.batchHandleReleasePartition.enabled: true
celeborn.client.push.maxReqsInFlight: 128

# Network connections between peers
celeborn.data.io.numConnectionsPerPeer: 16
# threads number may vary according to your cluster but do not set to 1
celeborn.data.io.threads: 32
celeborn.client.shuffle.batchHandleCommitPartition.threads: 32
celeborn.rpc.dispatcher.numThreads: 32

# Floating buffers may need to change `taskmanager.network.memory.fraction` and `taskmanager.network.memory.max`
taskmanager.network.memory.floating-buffers-per-gate: 4096
taskmanager.network.memory.buffers-per-channel: 0
taskmanager.memory.task.off-heap.size: 512m
```
**Note**: The config option `execution.batch-shuffle-mode` should configure as `ALL_EXCHANGES_BLOCKING`.

##### Flink Hybrid Shuffle Configuration
```properties
shuffle-service-factory.class: org.apache.flink.runtime.io.network.NettyShuffleServiceFactory
taskmanager.network.hybrid-shuffle.external-remote-tier-factory.class: org.apache.celeborn.plugin.flink.tiered.CelebornTierFactory
execution.batch-shuffle-mode: ALL_EXCHANGES_HYBRID_FULL
jobmanager.partition.hybrid.partition-data-consume-constraint: ALL_PRODUCERS_FINISHED

celeborn.master.endpoints: clb-1:9097,clb-2:9097,clb-3:9097
celeborn.client.shuffle.batchHandleReleasePartition.enabled: true
celeborn.client.push.maxReqsInFlight: 128
# Network connections between peers
celeborn.data.io.numConnectionsPerPeer: 16
# threads number may vary according to your cluster but do not set to 1
celeborn.data.io.threads: 32
celeborn.client.shuffle.batchHandleCommitPartition.threads: 32
celeborn.rpc.dispatcher.numThreads: 32
```
**Note**: The config option `execution.batch-shuffle-mode` should configure as `ALL_EXCHANGES_HYBRID_FULL`.

## Deploy MapReduce client
Copy `$CELEBORN_HOME/mr/celeborn-client-mr-shaded_<scala.binary.version>-<celeborn.version>.jar` into `mapreduce.application.classpath` and `yarn.application.classpath`.

Meanwhile, configure the following settings in YARN and MapReduce config.
```bash
-Dyarn.app.mapreduce.am.job.recovery.enable=false
-Dmapreduce.job.reduce.slowstart.completedmaps=1
-Dmapreduce.celeborn.master.endpoints=<master-1-1>:9097
-Dyarn.app.mapreduce.am.command-opts=org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn
-Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.CelebornMapOutputCollector
-Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer
```
**Note**: `MRAppMasterWithCeleborn` supports setting `mapreduce.celeborn.master.endpoints` via environment variable `CELEBORN_MASTER_ENDPOINTS`.
Meanwhile, `MRAppMasterWithCeleborn` disables `yarn.app.mapreduce.am.job.recovery.enable` and sets `mapreduce.job.reduce.slowstart.completedmaps` to 1 by default.
