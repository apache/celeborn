---
license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
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
| celeborn.application.heartbeat.timeout | 120s | Application heartbeat timeout. |  | 
| celeborn.ha.enabled | false | When true, master nodes run as Raft cluster mode. | 0.1.0 | 
| celeborn.ha.master.node.<id>.host | <required> | Host to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.node.<id>.port | 9097 | Port to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.node.<id>.ratis.port | 9872 | Ratis port to bind of master node <id> in HA mode. | 0.2.0 | 
| celeborn.ha.master.ratis.raft.rpc.type | netty | RPC type for Ratis, available options: netty, grpc. | 0.2.0 | 
| celeborn.ha.master.ratis.raft.server.storage.dir | /tmp/ratis |  | 0.2.0 | 
| celeborn.master.host | <localhost> | Hostname for master to bind. | 0.2.0 | 
| celeborn.master.metrics.prometheus.host | 0.0.0.0 |  |  | 
| celeborn.master.metrics.prometheus.port | 9098 |  |  | 
| celeborn.master.port | 9097 | Port for master to bind. | 0.2.0 | 
| celeborn.metrics.enabled | true | When true, enable metrics system. |  | 
| celeborn.metrics.sample.rate | 1.0 |  |  | 
| celeborn.metrics.timer.sliding.size | 4000 |  |  | 
| celeborn.metrics.timer.sliding.window.size | 4096 |  |  | 
| celeborn.worker.heartbeat.timeout | 120s | Worker heartbeat timeout. |  | 
<!--end-include-->
