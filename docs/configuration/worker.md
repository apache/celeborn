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
| celeborn.client.maxRetries | 15 | Max retry times for client to connect master endpoint | 0.2.0 | 
| celeborn.master.endpoints | &lt;localhost&gt;:9097 | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.metrics.capacity | 4096 | The maximum number of metrics which a source can use to generate output strings. | 0.2.0 | 
| celeborn.metrics.collectPerfCritical.enabled | false | It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them. | 0.2.0 | 
| celeborn.metrics.enabled | true | When true, enable metrics system. | 0.2.0 | 
| celeborn.metrics.sample.rate | 1.0 | It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0]. | 0.2.0 | 
| celeborn.metrics.timer.slidingWindow.size | 4096 | The sliding window size of timer metric. | 0.2.0 | 
| celeborn.shuffle.chuck.size | 8m | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. | 0.2.0 | 
| celeborn.shuffle.minPartitionSizeToEstimate | 8mb | Ignore partition size smaller than this configuration of partition size for estimation. | 0.2.0 | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | HDFS dir configuration for Celeborn to access HDFS. | 0.2.0 | 
| celeborn.worker.closeIdleConnections | false | Whether worker will close idle connections. | 0.2.0 | 
| celeborn.worker.commit.threads | 32 | Thread number of worker to commit shuffle data files asynchronously. | 0.2.0 | 
| celeborn.worker.directMemoryRatioToPauseReceive | 0.85 | If direct memory usage reaches this limit, the worker will stop to receive data from Celeborn shuffle clients. | 0.2.0 | 
| celeborn.worker.directMemoryRatioToPauseReplicate | 0.95 | If direct memory usage reaches this limit, the worker will stop to receive replication data from other workers. | 0.2.0 | 
| celeborn.worker.directMemoryRatioToResume | 0.5 | If direct memory usage is less than this limit, worker will resume. | 0.2.0 | 
| celeborn.worker.disk.check.timeout | 30s | Timeout time for worker check device status. | 0.2.0 | 
| celeborn.worker.disk.checkFileClean.maxRetries | 3 | The number of retries for a worker to check if the working directory is cleaned up before registering with the master. | 0.2.0 | 
| celeborn.worker.disk.checkFileClean.timeout | 1000ms | The wait time per retry for a worker to check if the working directory is cleaned up before registering with the master. | 0.2.0 | 
| celeborn.worker.disk.reserve.size | 5G | Celeborn worker reserved space for each disk. | 0.2.0 | 
| celeborn.worker.fetch.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to handle client fetch data. The default threads number is `size(celeborn.worker.storage.dirs)*2`. | 0.2.0 | 
| celeborn.worker.fetch.port | 0 | Server port for Worker to receive fetch data request from ShuffleClient. | 0.2.0 | 
| celeborn.worker.flusher.avgFlushTime.slidingWindow.size | 20 | The size of sliding windows used to calculate statistics about flushed time and count. | 0.2.0 | 
| celeborn.worker.flusher.buffer.size | 256k | Size of buffer used by a single flusher. | 0.2.0 | 
| celeborn.worker.flusher.hdd.threads | 1 | Flusher's thread count per disk used for write data to HDD disks. | 0.2.0 | 
| celeborn.worker.flusher.hdfs.threads | 4 | Flusher's thread count used for write data to HDFS. | 0.2.0 | 
| celeborn.worker.flusher.shutdownTimeout | 3s | Timeout for a flusher to shutdown. | 0.2.0 | 
| celeborn.worker.flusher.ssd.threads | 8 | Flusher's thread count per disk used for write data to SSD disks. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.interval | 1s | The wait interval of checking whether all released slots to be committed or destroyed during worker graceful shutdown | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout | 480s | The wait time of waiting for the released slots to be committed or destroyed during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.enabled | false | When true, during worker shutdown, the worker will wait for all released slots to be committed or destroyed. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout | 120s | The wait time of waiting for sorting partition files during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.recoverPath | &lt;tmp&gt;/recover | The path to store levelDB. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.timeout | 600s | The worker's graceful shutdown timeout time. | 0.2.0 | 
| celeborn.worker.heartbeat.timeout | 120s | Worker heartbeat timeout. | 0.2.0 | 
| celeborn.worker.memory.checkInterval | 10ms | Interval of worker direct memory checking. | 0.2.0 | 
| celeborn.worker.memory.reportInterval | 10s | Interval of worker direct memory tracker reporting to log. | 0.2.0 | 
| celeborn.worker.metrics.prometheus.host | 0.0.0.0 | Worker's Prometheus host. | 0.2.0 | 
| celeborn.worker.metrics.prometheus.port | 9096 | Worker's Prometheus port. | 0.2.0 | 
| celeborn.worker.monitor.disk.checkInterval | 60s | Intervals between device monitor to check disk. | 0.2.0 | 
| celeborn.worker.monitor.disk.checklist | readwrite,diskusage | Monitor type for disk, available items are: iohang, readwrite and diskusage. | 0.2.0 | 
| celeborn.worker.monitor.disk.enabled | true | When true, worker will monitor device and report to master. | 0.2.0 | 
| celeborn.worker.monitor.disk.sys.block.dir | /sys/block | The directory where linux file block information is stored. | 0.2.0 | 
| celeborn.worker.noneEmptyDirExpireDuration | 1d | If a non-empty application shuffle data dir have not been operated during le duration time, will mark this application as expired. | 0.2.0 | 
| celeborn.worker.partitionSorter.directMemoryRatioThreshold | 0.1 | Max ratio of partition sorter's memory for sorting, when reserved memory is higher than max partition sorter memory, partition sorter will stop sorting. | 0.2.0 | 
| celeborn.worker.partitionSorter.reservedMemoryPerPartition | 1mb | Initial reserve memory when sorting a shuffle file off-heap. | 0.2.0 | 
| celeborn.worker.partitionSorter.sort.timeout | 220s | Timeout for a shuffle file to sort. | 0.2.0 | 
| celeborn.worker.push.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to handle client push data. The default threads number is `size(celeborn.worker.storage.dirs)*2`. | 0.2.0 | 
| celeborn.worker.push.port | 0 | Server port for Worker to receive push data request from ShuffleClient. | 0.2.0 | 
| celeborn.worker.register.timeout | 180s | Worker register timeout. | 0.2.0 | 
| celeborn.worker.replicate.fastFail.duration | 60s | If a replicate request not replied during the duration, worker will mark the replicate data request as failed. | 0.2.0 | 
| celeborn.worker.replicate.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to replicate shuffle data. The default threads number is `size(celeborn.worker.storage.dirs)*2`. | 0.2.0 | 
| celeborn.worker.replicate.port | 0 | Server port for Worker to receive replicate data request from other Workers. | 0.2.0 | 
| celeborn.worker.replicate.threads | 64 | Thread number of worker to replicate shuffle data. | 0.2.0 | 
| celeborn.worker.rpc.port | 0 | Server port for Worker to receive RPC request. | 0.2.0 | 
| celeborn.worker.shuffle.commit.timeout | &lt;value of celeborn.rpc.askTimeout&gt; | Timeout for a Celeborn worker to commit files of a shuffle. | 0.2.0 | 
| celeborn.worker.storage.baseDir.number | 16 | How many directories will be used if `celeborn.worker.storage.dirs` is not set. The directory name is a combination of `celeborn.worker.storage.baseDir.prefix` and from one(inclusive) to `celeborn.worker.storage.baseDir.number`(inclusive) step by one. | 0.2.0 | 
| celeborn.worker.storage.baseDir.prefix | /mnt/disk | Base directory for Celeborn worker to write if `celeborn.worker.storage.dirs` is not set. | 0.2.0 | 
| celeborn.worker.storage.dirs | &lt;undefined&gt; | Directory list to store shuffle data. It's recommended to configure one directory on each disk. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 flush threads on the same disk partition if you are using HDD, and should be 8 or more flush threads on the same disk partition if you are using SSD. For example: `dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=]` | 0.2.0 | 
| celeborn.worker.workingDir | hadoop/rss-worker/shuffle_data | Worker's working dir path name. | 0.2.0 | 
| celeborn.worker.writer.close.timeout | 120s | Timeout for a file writer to close | 0.2.0 | 
| celeborn.worker.writer.create.maxAttempts | 3 | Retry count for a file writer to create if its creation was failed. | 0.2.0 | 
<!--end-include-->
