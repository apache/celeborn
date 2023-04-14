---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

## Deploy Celeborn

1. Unzip the tarball to `$CELEBORN_HOME`
2. Modify environment variables in `$CELEBORN_HOME/conf/celeborn-env.sh`

EXAMPLE:
```properties
#!/usr/bin/env bash
CELEBORN_MASTER_MEMORY=4g
CELEBORN_WORKER_MEMORY=2g
CELEBORN_WORKER_OFFHEAP_MEMORY=4g
```
3. Modify configurations in `$CELEBORN_HOME/conf/celeborn-defaults.conf`

EXAMPLE: single master cluster
```properties
# used by client and worker to connect to master
celeborn.master.endpoints clb-master:9097

# used by master to bootstrap
celeborn.master.host clb-master
celeborn.master.port 9097

celeborn.metrics.enabled true
celeborn.worker.flush.buffer.size 256k
celeborn.worker.storage.dirs /mnt/disk1/,/mnt/disk2
# If your hosts have disk raid or use lvm, set celeborn.worker.monitor.disk.enabled to false
celeborn.worker.monitor.disk.enabled false
```   

EXAMPLE: HA cluster
```properties
# used by client and worker to connect to master
celeborn.master.endpoints clb-1:9097,clb-2:9098,clb-3:9099

# used by master nodes to bootstrap, every node should know the topology of whole cluster, for each node,
# `celeborn.ha.master.node.id` should be unique, and `celeborn.ha.master.node.<id>.host` is required
celeborn.ha.enabled true
celeborn.ha.master.node.id 1
celeborn.ha.master.node.1.host clb-1
celeborn.ha.master.node.1.port 9097
celeborn.ha.master.node.1.ratis.port 9872
celeborn.ha.master.node.2.host clb-2
celeborn.ha.master.node.2.port 9098
celeborn.ha.master.node.2.ratis.port 9873
celeborn.ha.master.node.3.host clb-3
celeborn.ha.master.node.3.port 9099
celeborn.ha.master.node.3.ratis.port 9874
celeborn.ha.master.ratis.raft.server.storage.dir /mnt/disk1/rss_ratis/

celeborn.metrics.enabled true
# If you want to use HDFS as shuffle storage, make sure that flush buffer size is at least 4MB or larger.
celeborn.worker.flush.buffer.size 256k
celeborn.worker.storage.dirs /mnt/disk1/,/mnt/disk2
# If your hosts have disk raid or use lvm, set celeborn.worker.monitor.disk.enabled to false
celeborn.worker.monitor.disk.enabled false
```

4. Copy Celeborn and configurations to all nodes
5. Start all services. If you install Celeborn distribution in same path on every node and your
   cluster can perform SSH login then you can fill `$CELEBORN_HOME/conf/hosts` and
   use `$CELEBORN_HOME/sbin/start-all.sh` to start all
   services. If the installation paths are not identical, you will need to start service manually.  
   Start Celeborn master  
   `$CELEBORN_HOME/sbin/start-master.sh`  
   Start Celeborn worker  
   `$CELEBORN_HOME/sbin/start-worker.sh`
6. If Celeborn start success, the output of Master's log should be like this:
```angular2html
22/10/08 19:29:11,805 INFO [main] Dispatcher: Dispatcher numThreads: 64
22/10/08 19:29:11,875 INFO [main] TransportClientFactory: mode NIO threads 64
22/10/08 19:29:12,057 INFO [main] Utils: Successfully started service 'MasterSys' on port 9097.
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
Disks: {/mnt/disk1=DiskInfo(maxSlots: 6679, committed shuffles 0 shuffleAllocations: Map(), mountPoint: /mnt/disk1, usableSpace: 448284381184, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk3=DiskInfo(maxSlots: 6716, committed shuffles 0 shuffleAllocations: Map(), mountPoint: /mnt/disk3, usableSpace: 450755608576, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk2=DiskInfo(maxSlots: 6713, committed shuffles 0 shuffleAllocations: Map(), mountPoint: /mnt/disk2, usableSpace: 450532900864, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs , /mnt/disk4=DiskInfo(maxSlots: 6712, committed shuffles 0 shuffleAllocations: Map(), mountPoint: /mnt/disk4, usableSpace: 450456805376, avgFlushTime: 0, activeSlots: 0) status: HEALTHY dirs }
WorkerRef: null
```

## Deploy Spark client
Copy $CELEBORN_HOME/spark/*.jar to $SPARK_HOME/jars/

## Spark Configuration
To use Celeborn, following spark configurations should be added.
```properties
spark.shuffle.manager org.apache.spark.shuffle.celeborn.RssShuffleManager
# must use kryo serializer because java serializer do not support relocation
spark.serializer org.apache.spark.serializer.KryoSerializer

# celeborn master
spark.celeborn.master.endpoints clb-1:9097,clb-2:9098,clb-3:9099
spark.shuffle.service.enabled false

# options: hash, sort
# Hash shuffle writer use (partition count) * (celeborn.push.buffer.size) * (spark.executor.cores) memory.
# Sort shuffle writer use less memory than hash shuffle writer, if your shuffle partition count is large, try to use sort hash writer.  
spark.celeborn.shuffle.writer hash

# we recommend set spark.celeborn.push.replicate.enabled to true to enable server-side data replication
# If you have only one worker, this setting must be false 
spark.celeborn.push.replicate.enabled true

# Support for Spark AQE only tested under Spark 3
# we recommend set localShuffleReader to false to get better performance of Celeborn
spark.sql.adaptive.localShuffleReader.enabled false

# we recommend enabling aqe support to gain better performance
spark.sql.adaptive.enabled true
spark.sql.adaptive.skewJoin.enabled true
```
