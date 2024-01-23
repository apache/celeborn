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
| Key | Default | Description | Since | Deprecated |
| --- | ------- | ----------- | ----- | ---------- |
| celeborn.dynamicConfig.refresh.interval | 120s | Interval for refreshing the corresponding dynamic config periodically. | 0.4.0 |  | 
| celeborn.dynamicConfig.store.backend | NONE | Store backend for dynamic config. Available options: NONE, FS. Note: NONE means disabling dynamic config store. | 0.4.0 |  | 
| celeborn.master.estimatedPartitionSize.initialSize | 64mb | Initial partition size for estimation, it will change according to runtime stats. | 0.3.0 | celeborn.shuffle.initialEstimatedPartitionSize | 
| celeborn.master.estimatedPartitionSize.update.initialDelay | 5min | Initial delay time before start updating partition size for estimation. | 0.3.0 | celeborn.shuffle.estimatedPartitionSize.update.initialDelay | 
| celeborn.master.estimatedPartitionSize.update.interval | 10min | Interval of updating partition size for estimation. | 0.3.0 | celeborn.shuffle.estimatedPartitionSize.update.interval | 
| celeborn.master.hdfs.expireDirs.timeout | 1h | The timeout for a expire dirs to be deleted on HDFS. | 0.3.0 |  | 
| celeborn.master.heartbeat.application.timeout | 300s | Application heartbeat timeout. | 0.3.0 | celeborn.application.heartbeat.timeout | 
| celeborn.master.heartbeat.worker.timeout | 120s | Worker heartbeat timeout. | 0.3.0 | celeborn.worker.heartbeat.timeout | 
| celeborn.master.host | &lt;localhost&gt; | Hostname for master to bind. | 0.2.0 |  | 
| celeborn.master.http.host | &lt;localhost&gt; | Master's http host. | 0.4.0 | celeborn.metrics.master.prometheus.host,celeborn.master.metrics.prometheus.host | 
| celeborn.master.http.port | 9098 | Master's http port. | 0.4.0 | celeborn.metrics.master.prometheus.port,celeborn.master.metrics.prometheus.port | 
| celeborn.master.port | 9097 | Port for master to bind. | 0.2.0 |  | 
| celeborn.master.slot.assign.extraSlots | 2 | Extra slots number when master assign slots. | 0.3.0 | celeborn.slots.assign.extraSlots | 
| celeborn.master.slot.assign.loadAware.diskGroupGradient | 0.1 | This value means how many more workload will be placed into a faster disk group than a slower group. | 0.3.0 | celeborn.slots.assign.loadAware.diskGroupGradient | 
| celeborn.master.slot.assign.loadAware.fetchTimeWeight | 1.0 | Weight of average fetch time when calculating ordering in load-aware assignment strategy | 0.3.0 | celeborn.slots.assign.loadAware.fetchTimeWeight | 
| celeborn.master.slot.assign.loadAware.flushTimeWeight | 0.0 | Weight of average flush time when calculating ordering in load-aware assignment strategy | 0.3.0 | celeborn.slots.assign.loadAware.flushTimeWeight | 
| celeborn.master.slot.assign.loadAware.numDiskGroups | 5 | This configuration is a guidance for load-aware slot allocation algorithm. This value is control how many disk groups will be created. | 0.3.0 | celeborn.slots.assign.loadAware.numDiskGroups | 
| celeborn.master.slot.assign.maxWorkers | 10000 | Max workers that slots of one shuffle can be allocated on. Will choose the smaller positive one from Master side and Client side, see `celeborn.client.slot.assign.maxWorkers`. | 0.3.1 |  | 
| celeborn.master.slot.assign.policy | ROUNDROBIN | Policy for master to assign slots, Celeborn supports two types of policy: roundrobin and loadaware. Loadaware policy will be ignored when `HDFS` is enabled in `celeborn.storage.activeTypes` | 0.3.0 | celeborn.slots.assign.policy | 
| celeborn.master.userResourceConsumption.update.interval | 30s | Time length for a window about compute user resource consumption. | 0.3.0 |  | 
| celeborn.master.workerUnavailableInfo.expireTimeout | 1800s | Worker unavailable info would be cleared when the retention period is expired | 0.3.1 |  | 
| celeborn.storage.availableTypes | HDD | Enabled storages. Available options: MEMORY,HDD,SSD,HDFS. Note: HDD and SSD would be treated as identical. | 0.3.0 | celeborn.storage.activeTypes | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | HDFS base directory for Celeborn to store shuffle data. | 0.2.0 |  | 
| celeborn.storage.hdfs.kerberos.keytab | &lt;undefined&gt; | Kerberos keytab file path for HDFS storage connection. | 0.3.2 |  | 
| celeborn.storage.hdfs.kerberos.principal | &lt;undefined&gt; | Kerberos principal for HDFS storage connection. | 0.3.2 |  | 
<!--end-include-->
