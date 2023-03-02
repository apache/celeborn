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

<!--begin-include-->
| Key | Default | Description | Since |
| --- | ------- | ----------- | ----- |
| celeborn.application.heartbeat.timeout | 120s | Application heartbeat timeout. | 0.2.0 | 
| celeborn.ha.enabled | false | When true, master nodes run as Raft cluster mode. | 0.2.0 | 
| celeborn.ha.master.node.&lt;id&gt;.host | &lt;required&gt; | Host to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.node.&lt;id&gt;.port | 9097 | Port to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.node.&lt;id&gt;.ratis.port | 9872 | Ratis port to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.ratis.raft.rpc.type | netty | RPC type for Ratis, available options: netty, grpc. | 0.2.0 | 
| celeborn.ha.master.ratis.raft.server.storage.dir | /tmp/ratis |  | 0.2.0 | 
| celeborn.master.host | &lt;localhost&gt; | Hostname for master to bind. | 0.2.0 | 
| celeborn.master.metrics.prometheus.host | 0.0.0.0 | Master's Prometheus host. | 0.2.0 | 
| celeborn.master.metrics.prometheus.port | 9098 | Master's Prometheus port. | 0.2.0 | 
| celeborn.master.port | 9097 | Port for master to bind. | 0.2.0 | 
| celeborn.metrics.app.topDiskUsage.count | 50 | Size for top items about top disk usage applications list. | 0.2.0 | 
| celeborn.metrics.app.topDiskUsage.interval | 10min | Time length for a window about top disk usage application list. | 0.2.0 | 
| celeborn.metrics.app.topDiskUsage.windowSize | 24 | Window size about top disk usage application list. | 0.2.0 | 
| celeborn.metrics.capacity | 4096 | The maximum number of metrics which a source can use to generate output strings. | 0.2.0 | 
| celeborn.metrics.collectPerfCritical.enabled | false | It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them. | 0.2.0 | 
| celeborn.metrics.enabled | true | When true, enable metrics system. | 0.2.0 | 
| celeborn.metrics.sample.rate | 1.0 | It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0]. | 0.2.0 | 
| celeborn.metrics.timer.slidingWindow.size | 4096 | The sliding window size of timer metric. | 0.2.0 | 
| celeborn.shuffle.estimatedPartitionSize.update.initialDelay | 5min | Initial delay time before start updating partition size for estimation. | 0.2.0 | 
| celeborn.shuffle.estimatedPartitionSize.update.interval | 10min | Interval of updating partition size for estimation. | 0.2.0 | 
| celeborn.shuffle.initialEstimatedPartitionSize | 64mb | Initial partition size for estimation, it will change according to runtime stats. | 0.2.0 | 
| celeborn.slots.assign.extraSlots | 2 | Extra slots number when master assign slots. | 0.2.0 | 
| celeborn.slots.assign.loadAware.diskGroupGradient | 0.1 | This value means how many more workload will be placed into a faster disk group than a slower group. | 0.2.0 | 
| celeborn.slots.assign.loadAware.fetchTimeWeight | 1.0 | Weight of average fetch time when calculating ordering in load-aware assignment strategy | 0.2.1 | 
| celeborn.slots.assign.loadAware.flushTimeWeight | 0.0 | Weight of average flush time when calculating ordering in load-aware assignment strategy | 0.2.1 | 
| celeborn.slots.assign.loadAware.numDiskGroups | 5 | This configuration is a guidance for load-aware slot allocation algorithm. This value is control how many disk groups will be created. | 0.2.0 | 
| celeborn.slots.assign.policy | ROUNDROBIN | Policy for master to assign slots, Celeborn supports two types of policy: roundrobin and loadaware. | 0.2.0 | 
| celeborn.worker.heartbeat.timeout | 120s | Worker heartbeat timeout. | 0.2.0 | 
<!--end-include-->
