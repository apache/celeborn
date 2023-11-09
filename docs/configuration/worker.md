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
| celeborn.master.endpoints | &lt;localhost&gt;:9097 | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.master.estimatedPartitionSize.minSize | 8mb | Ignore partition size smaller than this configuration of partition size for estimation. | 0.3.0 | 
| celeborn.shuffle.chunk.size | 8m | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. | 0.2.0 | 
| celeborn.storage.availableTypes | HDD | Enabled storages. Available options: MEMORY,HDD,SSD,HDFS. Note: HDD and SSD would be treated as identical. | 0.3.0 | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | HDFS base directory for Celeborn to store shuffle data. | 0.2.0 | 
| celeborn.storage.hdfs.kerberos.keytab | &lt;undefined&gt; | Kerberos keytab file path for HDFS storage connection. | 0.3.2 | 
| celeborn.storage.hdfs.kerberos.principal | &lt;undefined&gt; | Kerberos principal for HDFS storage connection. | 0.3.2 | 
| celeborn.worker.activeConnection.max | &lt;undefined&gt; | If the number of active connections on a worker exceeds this configuration value, the worker will be marked as high-load in the heartbeat report, and the master will not include that node in the response of RequestSlots. | 0.3.1 | 
| celeborn.worker.bufferStream.threadsPerMountpoint | 8 | Threads count for read buffer per mount point. | 0.3.0 | 
| celeborn.worker.clean.threads | 64 | Thread number of worker to clean up expired shuffle keys. | 0.3.2 | 
| celeborn.worker.closeIdleConnections | false | Whether worker will close idle connections. | 0.2.0 | 
| celeborn.worker.commitFiles.threads | 32 | Thread number of worker to commit shuffle data files asynchronously. It's recommended to set at least `128` when `HDFS` is enabled in `celeborn.storage.activeTypes`. | 0.3.0 | 
| celeborn.worker.commitFiles.timeout | 120s | Timeout for a Celeborn worker to commit files of a shuffle. It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.activeTypes`. | 0.3.0 | 
| celeborn.worker.congestionControl.check.interval | 10ms | Interval of worker checks congestion if celeborn.worker.congestionControl.enabled is true. | 0.3.2 | 
| celeborn.worker.congestionControl.enabled | false | Whether to enable congestion control or not. | 0.3.0 | 
| celeborn.worker.congestionControl.high.watermark | &lt;undefined&gt; | If the total bytes in disk buffer exceeds this configure, will start to congestusers whose produce rate is higher than the potential average consume rate. The congestion will stop if the produce rate is lower or equal to the average consume rate, or the total pending bytes lower than celeborn.worker.congestionControl.low.watermark | 0.3.0 | 
| celeborn.worker.congestionControl.low.watermark | &lt;undefined&gt; | Will stop congest users if the total pending bytes of disk buffer is lower than this configuration | 0.3.0 | 
| celeborn.worker.congestionControl.sample.time.window | 10s | The worker holds a time sliding list to calculate users' produce/consume rate | 0.3.0 | 
| celeborn.worker.congestionControl.user.inactive.interval | 10min | How long will consider this user is inactive if it doesn't send data | 0.3.0 | 
| celeborn.worker.decommission.checkInterval | 30s | The wait interval of checking whether all the shuffle expired during worker decommission | 0.4.0 | 
| celeborn.worker.decommission.forceExitTimeout | 6h | The wait time of waiting for all the shuffle expire during worker decommission. | 0.4.0 | 
| celeborn.worker.directMemoryRatioForMemoryShuffleStorage | 0.0 | Max ratio of direct memory to store shuffle data | 0.2.0 | 
| celeborn.worker.directMemoryRatioForReadBuffer | 0.1 | Max ratio of direct memory for read buffer | 0.2.0 | 
| celeborn.worker.directMemoryRatioToPauseReceive | 0.85 | If direct memory usage reaches this limit, the worker will stop to receive data from Celeborn shuffle clients. | 0.2.0 | 
| celeborn.worker.directMemoryRatioToPauseReplicate | 0.95 | If direct memory usage reaches this limit, the worker will stop to receive replication data from other workers. This value should be higher than celeborn.worker.directMemoryRatioToPauseReceive. | 0.2.0 | 
| celeborn.worker.directMemoryRatioToResume | 0.7 | If direct memory usage is less than this limit, worker will resume. | 0.2.0 | 
| celeborn.worker.disk.clean.threads | 4 | Thread number of worker to clean up directories of expired shuffle keys on disk. | 0.3.2 | 
| celeborn.worker.fetch.heartbeat.enabled | false | enable the heartbeat from worker to client when fetching data | 0.3.0 | 
| celeborn.worker.fetch.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to handle client fetch data. The default threads number is the number of flush thread. | 0.2.0 | 
| celeborn.worker.fetch.port | 0 | Server port for Worker to receive fetch data request from ShuffleClient. | 0.2.0 | 
| celeborn.worker.flusher.buffer.size | 256k | Size of buffer used by a single flusher. | 0.2.0 | 
| celeborn.worker.flusher.diskTime.slidingWindow.size | 20 | The size of sliding windows used to calculate statistics about flushed time and count. | 0.3.0 | 
| celeborn.worker.flusher.hdd.threads | 1 | Flusher's thread count per disk used for write data to HDD disks. | 0.2.0 | 
| celeborn.worker.flusher.hdfs.buffer.size | 4m | Size of buffer used by a HDFS flusher. | 0.3.0 | 
| celeborn.worker.flusher.hdfs.threads | 8 | Flusher's thread count used for write data to HDFS. | 0.2.0 | 
| celeborn.worker.flusher.shutdownTimeout | 3s | Timeout for a flusher to shutdown. | 0.2.0 | 
| celeborn.worker.flusher.ssd.threads | 16 | Flusher's thread count per disk used for write data to SSD disks. | 0.2.0 | 
| celeborn.worker.flusher.threads | 16 | Flusher's thread count per disk for unknown-type disks. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.interval | 1s | The wait interval of checking whether all released slots to be committed or destroyed during worker graceful shutdown | 0.2.0 | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout | 480s | The wait time of waiting for the released slots to be committed or destroyed during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.enabled | false | When true, during worker shutdown, the worker will wait for all released slots to be committed or destroyed. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout | 120s | The wait time of waiting for sorting partition files during worker graceful shutdown. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.recoverPath | &lt;tmp&gt;/recover | The path to store levelDB. | 0.2.0 | 
| celeborn.worker.graceful.shutdown.saveCommittedFileInfo.interval | 5s | Interval for a Celeborn worker to flush committed file infos into Level DB. | 0.3.1 | 
| celeborn.worker.graceful.shutdown.saveCommittedFileInfo.sync | false | Whether to call sync method to save committed file infos into Level DB to handle OS crash. | 0.3.1 | 
| celeborn.worker.graceful.shutdown.timeout | 600s | The worker's graceful shutdown timeout time. | 0.2.0 | 
| celeborn.worker.monitor.disk.check.interval | 30s | Intervals between device monitor to check disk. | 0.3.0 | 
| celeborn.worker.monitor.disk.check.timeout | 30s | Timeout time for worker check device status. | 0.3.0 | 
| celeborn.worker.monitor.disk.checklist | readwrite,diskusage | Monitor type for disk, available items are: iohang, readwrite and diskusage. | 0.2.0 | 
| celeborn.worker.monitor.disk.enabled | true | When true, worker will monitor device and report to master. | 0.3.0 | 
| celeborn.worker.monitor.disk.notifyError.expireTimeout | 10m | The expire timeout of non-critical device error. Only notify critical error when the number of non-critical errors for a period of time exceeds threshold. | 0.3.0 | 
| celeborn.worker.monitor.disk.notifyError.threshold | 64 | Device monitor will only notify critical error once the accumulated valid non-critical error number exceeding this threshold. | 0.3.0 | 
| celeborn.worker.monitor.disk.sys.block.dir | /sys/block | The directory where linux file block information is stored. | 0.2.0 | 
| celeborn.worker.monitor.memory.check.interval | 10ms | Interval of worker direct memory checking. | 0.3.0 | 
| celeborn.worker.monitor.memory.report.interval | 10s | Interval of worker direct memory tracker reporting to log. | 0.3.0 | 
| celeborn.worker.monitor.memory.trimChannelWaitInterval | 1s | Wait time after worker trigger channel to trim cache. | 0.3.0 | 
| celeborn.worker.monitor.memory.trimFlushWaitInterval | 1s | Wait time after worker trigger StorageManger to flush data. | 0.3.0 | 
| celeborn.worker.partition.initial.readBuffersMax | 1024 | Max number of initial read buffers | 0.3.0 | 
| celeborn.worker.partition.initial.readBuffersMin | 1 | Min number of initial read buffers | 0.3.0 | 
| celeborn.worker.partitionSorter.directMemoryRatioThreshold | 0.1 | Max ratio of partition sorter's memory for sorting, when reserved memory is higher than max partition sorter memory, partition sorter will stop sorting. | 0.2.0 | 
| celeborn.worker.push.heartbeat.enabled | false | enable the heartbeat from worker to client when pushing data | 0.3.0 | 
| celeborn.worker.push.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to handle client push data. The default threads number is the number of flush thread. | 0.2.0 | 
| celeborn.worker.push.port | 0 | Server port for Worker to receive push data request from ShuffleClient. | 0.2.0 | 
| celeborn.worker.readBuffer.allocationWait | 50ms | The time to wait when buffer dispatcher can not allocate a buffer. | 0.3.0 | 
| celeborn.worker.readBuffer.target.changeThreshold | 1mb | The target ratio for pre read memory usage. | 0.3.0 | 
| celeborn.worker.readBuffer.target.ratio | 0.9 | The target ratio for read ahead buffer's memory usage. | 0.3.0 | 
| celeborn.worker.readBuffer.target.updateInterval | 100ms | The interval for memory manager to calculate new read buffer's target memory. | 0.3.0 | 
| celeborn.worker.readBuffer.toTriggerReadMin | 32 | Min buffers count for map data partition to trigger read. | 0.3.0 | 
| celeborn.worker.register.timeout | 180s | Worker register timeout. | 0.2.0 | 
| celeborn.worker.replicate.fastFail.duration | 60s | If a replicate request not replied during the duration, worker will mark the replicate data request as failed.It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.activeTypes`. | 0.2.0 | 
| celeborn.worker.replicate.io.threads | &lt;undefined&gt; | Netty IO thread number of worker to replicate shuffle data. The default threads number is the number of flush thread. | 0.2.0 | 
| celeborn.worker.replicate.port | 0 | Server port for Worker to receive replicate data request from other Workers. | 0.2.0 | 
| celeborn.worker.replicate.randomConnection.enabled | true | Whether worker will create random connection to peer when replicate data. When false, worker tend to reuse the same cached TransportClient to a specific replicate worker; when true, worker tend to use different cached TransportClient. Netty will use the same thread to serve the same connection, so with more connections replicate server can leverage more netty threads | 0.2.1 | 
| celeborn.worker.replicate.threads | 64 | Thread number of worker to replicate shuffle data. | 0.2.0 | 
| celeborn.worker.rpc.port | 0 | Server port for Worker to receive RPC request. | 0.2.0 | 
| celeborn.worker.shuffle.partitionSplit.enabled | true | enable the partition split on worker side | 0.3.0 | 
| celeborn.worker.shuffle.partitionSplit.max | 2g | Specify the maximum partition size for splitting, and ensure that individual partition files are always smaller than this limit. | 0.3.0 | 
| celeborn.worker.shuffle.partitionSplit.min | 1m | Min size for a partition to split | 0.3.0 | 
| celeborn.worker.sortPartition.reservedMemoryPerPartition | 1mb | Reserved memory when sorting a shuffle file off-heap. | 0.3.0 | 
| celeborn.worker.sortPartition.threads | &lt;undefined&gt; | PartitionSorter's thread counts. It's recommended to set at least `64` when `HDFS` is enabled in `celeborn.storage.activeTypes`. | 0.3.0 | 
| celeborn.worker.sortPartition.timeout | 220s | Timeout for a shuffle file to sort. | 0.3.0 | 
| celeborn.worker.storage.checkDirsEmpty.maxRetries | 3 | The number of retries for a worker to check if the working directory is cleaned up before registering with the master. | 0.3.0 | 
| celeborn.worker.storage.checkDirsEmpty.timeout | 1000ms | The wait time per retry for a worker to check if the working directory is cleaned up before registering with the master. | 0.3.0 | 
| celeborn.worker.storage.dirs | &lt;undefined&gt; | Directory list to store shuffle data. It's recommended to configure one directory on each disk. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 flush threads on the same disk partition if you are using HDD, and should be 8 or more flush threads on the same disk partition if you are using SSD. For example: `dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=]` | 0.2.0 | 
| celeborn.worker.storage.disk.reserve.ratio | &lt;undefined&gt; | Celeborn worker reserved ratio for each disk. The minimum usable size for each disk is the max space between the reserved space and the space calculate via reserved ratio. | 0.3.2 | 
| celeborn.worker.storage.disk.reserve.size | 5G | Celeborn worker reserved space for each disk. | 0.3.0 | 
| celeborn.worker.storage.expireDirs.timeout | 1h | The timeout for a expire dirs to be deleted on disk. | 0.3.2 | 
| celeborn.worker.storage.workingDir | celeborn-worker/shuffle_data | Worker's working dir path name. | 0.3.0 | 
| celeborn.worker.writer.close.timeout | 120s | Timeout for a file writer to close | 0.2.0 | 
| celeborn.worker.writer.create.maxAttempts | 3 | Retry count for a file writer to create if its creation was failed. | 0.2.0 | 
<!--end-include-->
