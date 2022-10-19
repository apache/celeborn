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
| celeborn.application.heartbeat.interval | `10s` | Interval for client to send heartbeat message to master. |  | 
| celeborn.fetch.maxReqsInFlight | `3` | Amount of in-flight chunk fetch request. |  | 
| celeborn.fetch.timeout | `120s` | Timeout for a task to fetch chunk. |  | 
| celeborn.master.endpoints | `<localhost>:9097` | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.push.buffer.initial.size | `8k` |  |  | 
| celeborn.push.buffer.max.size | `64k` | Max size of reducer partition buffer memory for shuffle hash writer. The pushed data will be buffered in memory before sending to Celeborn worker. For performance consideration keep this buffer size higher than 32K. Example: If reducer amount is 2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` heap memory. |  | 
| celeborn.push.maxReqsInFlight | `32` | Amount of Netty in-flight requests. The maximum memory is `celeborn.push.maxReqsInFlight` * `celeborn.push.buffer.max.size` * compression ratio(1 in worst case), default: 64Kib * 32 = 2Mib |  | 
| celeborn.push.queue.capacity | `512` | Push buffer queue size for a task. The maximum memory is `celeborn.push.buffer.max.size` * `celeborn.push.queue.capacity`, default: 64KiB * 512 = 32MiB |  | 
| celeborn.push.replicate.enabled | `true` | When true, Celeborn worker will replicate shuffle data to another Celeborn worker asynchronously to ensure the pushed shuffle data won't be lost after the node failure. | 0.2.0 | 
| celeborn.rpc.maxParallelism | `1024` | Max parallelism of client on sending RPC requests. |  | 
| celeborn.shuffle.chuck.size | `8m` | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. |  | 
| celeborn.shuffle.expired.interval | `60s` | Interval for client to check expired shuffles. |  | 
| celeborn.shuffle.register.maxRetries | `3` | Max retry times for client to register shuffle. |  | 
| celeborn.shuffle.register.retryWait | `3s` | Wait time before next retry if register shuffle failed. |  | 
| celeborn.shuffle.writer.mode | `hash` | Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine when memory pressure is high or shuffle partition count it huge. | 0.2.0 | 
| celeborn.slot.reserve.maxRetries | `3` | Max retry times for client to reserve slots. |  | 
| celeborn.slot.reserve.retry.timeout | `3s` | Wait time before next retry if reserve slots failed. |  | 
| celeborn.storage.hdfs.dir | `<undefined>` | HDFS dir configuration for Celeborn to access HDFS. |  | 
| celeborn.worker.excluded.interval | `30s` | Interval for client to refresh excluded worker list. |  | 
<!--end-include-->
