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
| celeborn.master.endpoints | `<localhost>:9097` | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.metrics.enabled | `true` | When true, enable metrics system. |  | 
| celeborn.metrics.sample.rate | `1.0` |  |  | 
| celeborn.metrics.timer.sliding.size | `4000` |  |  | 
| celeborn.metrics.timer.sliding.window.size | `4096` |  |  | 
| celeborn.shuffle.chuck.size | `8m` | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. |  | 
| celeborn.worker.checkSlotsFinishedInterval | `1s` | The wait interval of checking whether all released slots to be committed or destroyed during worker graceful shutdown |  | 
| celeborn.worker.checkSlotsFinishedTimeout | `480s` | The wait time of waiting for the released slots to be committed or destroyed during worker graceful shutdown. |  | 
| celeborn.worker.commit.threads | `32` | Thread number of worker to commit shuffle data files asynchronously. |  | 
| celeborn.worker.deviceMonitor.checklist | `readwrite,diskusage` | Select what the device needs to detect, available items are: iohang, readwrite and diskusage. |  | 
| celeborn.worker.deviceMonitor.enabled | `true` | When true, worker will monitor device and report to master. |  | 
| celeborn.worker.diskFlusherShutdownTimeout | `3s` | The timeout to wait for diskOperators to execute remaining jobs before being shutdown immediately. |  | 
| celeborn.worker.flush.buffer.size | `256k` | Size of buffer used by a single flusher. |  | 
| celeborn.worker.gracefulShutdown.enabled | `false` | When true, during worker shutdown, the worker will wait for all released slots to be committed or destroyed in time of `rss.worker.checkSlots.timeout` and wait sorting partition files in time of `rss.worker.partitionSorterCloseAwaitTime`. |  | 
| celeborn.worker.gracefulShutdownTimeout | `600s` | The worker's graceful shutdown time. |  | 
| celeborn.worker.heartbeat.timeout | `120s` | Worker heartbeat timeout. |  | 
| celeborn.worker.metrics.prometheus.host | `0.0.0.0` |  |  | 
| celeborn.worker.metrics.prometheus.port | `9096` |  |  | 
| celeborn.worker.partitionSorterCloseAwaitTime | `120s` | The wait time of waiting for sorting partition files during worker graceful shutdown. |  | 
| celeborn.worker.recoverPath | `/tmp/recover` | The path to store levelDB. |  | 
| celeborn.worker.replicate.threads | `64` | Thread number of worker to replicate shuffle data. |  | 
| celeborn.worker.storage.dirs | `<undefined>` | Directory list to store shuffle data. It's recommended to configure one directory on each disk. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 flush threads on the same disk partition if you are using HDD, and should be 8 or more flush threads on the same disk partition if you are using SSD. For example: dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=] |  | 
<!--end-include-->
