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
| celeborn.master.estimatedPartitionSize.initialSize | 64mb | Initial partition size for estimation, it will change according to runtime stats. | 0.3.0 | 
| celeborn.master.estimatedPartitionSize.update.initialDelay | 5min | Initial delay time before start updating partition size for estimation. | 0.3.0 | 
| celeborn.master.estimatedPartitionSize.update.interval | 10min | Interval of updating partition size for estimation. | 0.3.0 | 
| celeborn.master.heartbeat.application.timeout | 300s | Application heartbeat timeout. | 0.3.0 | 
| celeborn.master.heartbeat.worker.timeout | 120s | Worker heartbeat timeout. | 0.3.0 | 
| celeborn.master.host | &lt;localhost&gt; | Hostname for master to bind. | 0.2.0 | 
| celeborn.master.port | 9097 | Port for master to bind. | 0.2.0 | 
| celeborn.master.slot.assign.extraSlots | 2 | Extra slots number when master assign slots. | 0.3.0 | 
| celeborn.master.slot.assign.loadAware.diskGroupGradient | 0.1 | This value means how many more workload will be placed into a faster disk group than a slower group. | 0.3.0 | 
| celeborn.master.slot.assign.loadAware.fetchTimeWeight | 1.0 | Weight of average fetch time when calculating ordering in load-aware assignment strategy | 0.3.0 | 
| celeborn.master.slot.assign.loadAware.flushTimeWeight | 0.0 | Weight of average flush time when calculating ordering in load-aware assignment strategy | 0.3.0 | 
| celeborn.master.slot.assign.loadAware.numDiskGroups | 5 | This configuration is a guidance for load-aware slot allocation algorithm. This value is control how many disk groups will be created. | 0.3.0 | 
| celeborn.master.slot.assign.policy | ROUNDROBIN | Policy for master to assign slots, Celeborn supports two types of policy: roundrobin and loadaware. | 0.3.0 | 
| celeborn.master.userResourceConsumption.update.interval | 30s | Time length for a window about compute user resource consumption. | 0.3.0 | 
<!--end-include-->
