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
| celeborn.hdfs.dir | `<undefined>` | Hdfs dir configuration for Celeborn to access HDFS. |  | 
| celeborn.master.endpoints | `<localhost>:9097` | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.metrics.enabled | `true` | When true, enable metrics system. |  | 
| celeborn.metrics.sample.rate | `1.0` |  |  | 
| celeborn.metrics.timer.sliding.size | `4000` |  |  | 
| celeborn.metrics.timer.sliding.window.size | `4096` |  |  | 
| celeborn.shuffle.chuck.size | `8m` | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. |  | 
| celeborn.worker.base.dir.number | `16` | Base directory count for Celeborn worker to write. |  | 
| celeborn.worker.base.dir.prefix | `/mnt/disk` | Base directory for Celeborn worker to write. |  | 
| celeborn.worker.commit.files.timeout | `120s` | Timeout for commit file to complete. | 0.2.0 | 
| celeborn.worker.commit.threads | `32` | Thread number of worker to commit shuffle data files asynchronously. |  | 
| celeborn.worker.deviceMonitor.checklist | `readwrite,diskusage` | Select what the device needs to detect, available items are: iohang, readwrite and diskusage. |  | 
| celeborn.worker.deviceMonitor.enabled | `true` | When true, worker will monitor device and report to master. |  | 
| celeborn.worker.disk.reservation | `5G` | Celeborn worker reserved space for each disk. | 0.2.0 | 
| celeborn.worker.diskmonitor.check.interval | `60s` |  |  | 
| celeborn.worker.diskmonitor.sys.block.dir | `/sys/block` | The directory where linux file block information is stored. |  | 
| celeborn.worker.filewriter.close.timeout | `120s` | Timeout for a file writer to close | 0.2.0 | 
| celeborn.worker.filewriter.creation.retry | `3` | Retry count for a file writer to create if its creation was failed. |  | 
| celeborn.worker.flusher.avg.window.count | `20` | The count of windows used for calculate statistics about flushed time and count. | 0.2.0 | 
| celeborn.worker.flusher.buffer.size | `256k` | Size of buffer used by a single flusher. |  | 
| celeborn.worker.flusher.graceful.shutdown.timeout | `3s` | Timeout for a flusher to shutdown gracefully. | 0.2.0 | 
| celeborn.worker.flusher.hdd.threads | `1` | Flusher's thread count used for write data to HDD disks. | 0.2.0 | 
| celeborn.worker.flusher.hdfs.threads | `4` | Flusher's thread count used for write data to HDFS. | 0.2.0 | 
| celeborn.worker.flusher.ssd.threads | `8` | Flusher's thread count used for write data to SSD disks. | 0.2.0 | 
| celeborn.worker.flusher.window.minimum.flush.count | `1000` | Minimum flush data count for a valid window. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.interval | `1s` | The wait interval of checking whether all released slots to be committed or destroyed during worker graceful shutdown | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout | `480s` | The wait time of waiting for the released slots to be committed or destroyed during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.enabled | `false` | When true, during worker shutdown, the worker will wait for all released slots to be committed or destroyed. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout | `120s` | The wait time of waiting for sorting partition files during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.recoverPath | `<tmp>/recover` | The path to store levelDB. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.timeout | `600s` | The worker's graceful shutdown timeout time. | 0.2.0 | 
| celeborn.worker.heartbeat.timeout | `120s` | Worker heartbeat timeout. |  | 
| celeborn.worker.metrics.prometheus.host | `0.0.0.0` |  |  | 
| celeborn.worker.metrics.prometheus.port | `9096` |  |  | 
| celeborn.worker.replicate.threads | `64` | Thread number of worker to replicate shuffle data. |  | 
| celeborn.worker.storage.dirs | `<undefined>` | Directory list to store shuffle data. It's recommended to configure one directory on each disk. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 flush threads on the same disk partition if you are using HDD, and should be 8 or more flush threads on the same disk partition if you are using SSD. For example: dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=] |  | 
<!--end-include-->
