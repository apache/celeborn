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
| celeborn.client.application.heartbeatInterval | 10s | Interval for client to send heartbeat message to master. | 0.3.0 | celeborn.application.heartbeatInterval | 
| celeborn.client.application.unregister.enabled | true | When true, Celeborn client will inform celeborn master the application is already shutdown during client exit, this allows the cluster to release resources immediately, resulting in resource savings. | 0.3.2 |  | 
| celeborn.client.closeIdleConnections | true | Whether client will close idle connections. | 0.3.0 |  | 
| celeborn.client.commitFiles.ignoreExcludedWorker | false | When true, LifecycleManager will skip workers which are in the excluded list. | 0.3.0 |  | 
| celeborn.client.eagerlyCreateInputStream.threads | 32 | Threads count for streamCreatorPool in CelebornShuffleReader. | 0.3.1 |  | 
| celeborn.client.excludePeerWorkerOnFailure.enabled | true | When true, Celeborn will exclude partition's peer worker on failure when push data to replica failed. | 0.3.0 |  | 
| celeborn.client.excludedWorker.expireTimeout | 180s | Timeout time for LifecycleManager to clear reserved excluded worker. Default to be 1.5 * `celeborn.master.heartbeat.worker.timeout`to cover worker heartbeat timeout check period | 0.3.0 | celeborn.worker.excluded.expireTimeout | 
| celeborn.client.fetch.buffer.size | 64k | Size of reducer partition buffer memory for shuffle reader. The fetched data will be buffered in memory before consuming. For performance consideration keep this buffer size not less than `celeborn.client.push.buffer.max.size`. | 0.4.0 |  | 
| celeborn.client.fetch.dfsReadChunkSize | 8m | Max chunk size for DfsPartitionReader. | 0.3.1 |  | 
| celeborn.client.fetch.excludeWorkerOnFailure.enabled | false | Whether to enable shuffle client-side fetch exclude workers on failure. | 0.3.0 |  | 
| celeborn.client.fetch.excludedWorker.expireTimeout | &lt;value of celeborn.client.excludedWorker.expireTimeout&gt; | ShuffleClient is a static object, it will be used in the whole lifecycle of Executor,We give a expire time for excluded workers to avoid a transient worker issues. | 0.3.0 |  | 
| celeborn.client.fetch.maxReqsInFlight | 3 | Amount of in-flight chunk fetch request. | 0.3.0 | celeborn.fetch.maxReqsInFlight | 
| celeborn.client.fetch.maxRetriesForEachReplica | 3 | Max retry times of fetch chunk on each replica | 0.3.0 | celeborn.fetch.maxRetriesForEachReplica,celeborn.fetch.maxRetries | 
| celeborn.client.fetch.timeout | 600s | Timeout for a task to open stream and fetch chunk. | 0.3.0 | celeborn.fetch.timeout | 
| celeborn.client.flink.compression.enabled | true | Whether to compress data in Flink plugin. | 0.3.0 | remote-shuffle.job.enable-data-compression | 
| celeborn.client.flink.inputGate.concurrentReadings | 2147483647 | Max concurrent reading channels for a input gate. | 0.3.0 | remote-shuffle.job.concurrent-readings-per-gate | 
| celeborn.client.flink.inputGate.memory | 32m | Memory reserved for a input gate. | 0.3.0 | remote-shuffle.job.memory-per-gate | 
| celeborn.client.flink.inputGate.minMemory | 8m | Min memory reserved for a input gate. | 0.3.0 | remote-shuffle.job.min.memory-per-gate | 
| celeborn.client.flink.inputGate.supportFloatingBuffer | true | Whether to support floating buffer in Flink input gates. | 0.3.0 | remote-shuffle.job.support-floating-buffer-per-input-gate | 
| celeborn.client.flink.resultPartition.memory | 64m | Memory reserved for a result partition. | 0.3.0 | remote-shuffle.job.memory-per-partition | 
| celeborn.client.flink.resultPartition.minMemory | 8m | Min memory reserved for a result partition. | 0.3.0 | remote-shuffle.job.min.memory-per-partition | 
| celeborn.client.flink.resultPartition.supportFloatingBuffer | true | Whether to support floating buffer for result partitions. | 0.3.0 | remote-shuffle.job.support-floating-buffer-per-output-gate | 
| celeborn.client.mr.pushData.max | 32m | Max size for a push data sent from mr client. | 0.4.0 |  | 
| celeborn.client.push.buffer.initial.size | 8k |  | 0.3.0 | celeborn.push.buffer.initial.size | 
| celeborn.client.push.buffer.max.size | 64k | Max size of reducer partition buffer memory for shuffle hash writer. The pushed data will be buffered in memory before sending to Celeborn worker. For performance consideration keep this buffer size higher than 32K. Example: If reducer amount is 2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` heap memory. | 0.3.0 | celeborn.push.buffer.max.size | 
| celeborn.client.push.excludeWorkerOnFailure.enabled | false | Whether to enable shuffle client-side push exclude workers on failures. | 0.3.0 |  | 
| celeborn.client.push.limit.inFlight.sleepInterval | 50ms | Sleep interval when check netty in-flight requests to be done. | 0.3.0 | celeborn.push.limit.inFlight.sleepInterval | 
| celeborn.client.push.limit.inFlight.timeout | &lt;undefined&gt; | Timeout for netty in-flight requests to be done.Default value should be `celeborn.client.push.timeout * 2`. | 0.3.0 | celeborn.push.limit.inFlight.timeout | 
| celeborn.client.push.limit.strategy | SIMPLE | The strategy used to control the push speed. Valid strategies are SIMPLE and SLOWSTART. The SLOWSTART strategy usually works with congestion control mechanism on the worker side. | 0.3.0 |  | 
| celeborn.client.push.maxReqsInFlight.perWorker | 32 | Amount of Netty in-flight requests per worker. Default max memory of in flight requests  per worker is `celeborn.client.push.maxReqsInFlight.perWorker` * `celeborn.client.push.buffer.max.size` * compression ratio(1 in worst case): 64KiB * 32 = 2MiB. The maximum memory will not exceed `celeborn.client.push.maxReqsInFlight.total`. | 0.3.0 |  | 
| celeborn.client.push.maxReqsInFlight.total | 256 | Amount of total Netty in-flight requests. The maximum memory is `celeborn.client.push.maxReqsInFlight.total` * `celeborn.client.push.buffer.max.size` * compression ratio(1 in worst case): 64KiB * 256 = 16MiB | 0.3.0 | celeborn.push.maxReqsInFlight | 
| celeborn.client.push.queue.capacity | 512 | Push buffer queue size for a task. The maximum memory is `celeborn.client.push.buffer.max.size` * `celeborn.client.push.queue.capacity`, default: 64KiB * 512 = 32MiB | 0.3.0 | celeborn.push.queue.capacity | 
| celeborn.client.push.replicate.enabled | false | When true, Celeborn worker will replicate shuffle data to another Celeborn worker asynchronously to ensure the pushed shuffle data won't be lost after the node failure. It's recommended to set `false` when `HDFS` is enabled in `celeborn.storage.activeTypes`. | 0.3.0 | celeborn.push.replicate.enabled | 
| celeborn.client.push.retry.threads | 8 | Thread number to process shuffle re-send push data requests. | 0.3.0 | celeborn.push.retry.threads | 
| celeborn.client.push.revive.batchSize | 2048 | Max number of partitions in one Revive request. | 0.3.0 |  | 
| celeborn.client.push.revive.interval | 100ms | Interval for client to trigger Revive to LifecycleManager. The number of partitions in one Revive request is `celeborn.client.push.revive.batchSize`. | 0.3.0 |  | 
| celeborn.client.push.revive.maxRetries | 5 | Max retry times for reviving when celeborn push data failed. | 0.3.0 |  | 
| celeborn.client.push.sendBufferPool.checkExpireInterval | 30s | Interval to check expire for send buffer pool. If the pool has been idle for more than `celeborn.client.push.sendBufferPool.expireTimeout`, the pooled send buffers and push tasks will be cleaned up. | 0.3.1 |  | 
| celeborn.client.push.sendBufferPool.expireTimeout | 60s | Timeout before clean up SendBufferPool. If SendBufferPool is idle for more than this time, the send buffers and push tasks will be cleaned up. | 0.3.1 |  | 
| celeborn.client.push.slowStart.initialSleepTime | 500ms | The initial sleep time if the current max in flight requests is 0 | 0.3.0 |  | 
| celeborn.client.push.slowStart.maxSleepTime | 2s | If celeborn.client.push.limit.strategy is set to SLOWSTART, push side will take a sleep strategy for each batch of requests, this controls the max sleep time if the max in flight requests limit is 1 for a long time | 0.3.0 |  | 
| celeborn.client.push.sort.randomizePartitionId.enabled | false | Whether to randomize partitionId in push sorter. If true, partitionId will be randomized when sort data to avoid skew when push to worker | 0.3.0 | celeborn.push.sort.randomizePartitionId.enabled | 
| celeborn.client.push.splitPartition.threads | 8 | Thread number to process shuffle split request in shuffle client. | 0.3.0 | celeborn.push.splitPartition.threads | 
| celeborn.client.push.stageEnd.timeout | &lt;value of celeborn.&lt;module&gt;.io.connectionTimeout&gt; | Timeout for waiting StageEnd. During this process, there are `celeborn.client.requestCommitFiles.maxRetries` times for retry opportunities for committing files and 1 times for releasing slots request. User can customize this value according to your setting. By default, the value is the max timeout value `celeborn.<module>.io.connectionTimeout`. | 0.3.0 | celeborn.push.stageEnd.timeout | 
| celeborn.client.push.takeTaskMaxWaitAttempts | 1 | Max wait times if no task available to push to worker. | 0.3.0 |  | 
| celeborn.client.push.takeTaskWaitInterval | 50ms | Wait interval if no task available to push to worker. | 0.3.0 |  | 
| celeborn.client.push.timeout | 120s | Timeout for a task to push data rpc message. This value should better be more than twice of `celeborn.<module>.push.timeoutCheck.interval` | 0.3.0 | celeborn.push.data.timeout | 
| celeborn.client.readLocalShuffleFile.enabled | false | Enable read local shuffle file for clusters that co-deployed with yarn node manager. | 0.3.1 |  | 
| celeborn.client.readLocalShuffleFile.threads | 4 | Threads count for read local shuffle file. | 0.3.1 |  | 
| celeborn.client.registerShuffle.maxRetries | 3 | Max retry times for client to register shuffle. | 0.3.0 | celeborn.shuffle.register.maxRetries | 
| celeborn.client.registerShuffle.retryWait | 3s | Wait time before next retry if register shuffle failed. | 0.3.0 | celeborn.shuffle.register.retryWait | 
| celeborn.client.requestCommitFiles.maxRetries | 4 | Max retry times for requestCommitFiles RPC. | 0.3.0 |  | 
| celeborn.client.reserveSlots.maxRetries | 3 | Max retry times for client to reserve slots. | 0.3.0 | celeborn.slots.reserve.maxRetries | 
| celeborn.client.reserveSlots.rackaware.enabled | false | Whether need to place different replicates on different racks when allocating slots. | 0.3.1 | celeborn.client.reserveSlots.rackware.enabled | 
| celeborn.client.reserveSlots.retryWait | 3s | Wait time before next retry if reserve slots failed. | 0.3.0 | celeborn.slots.reserve.retryWait | 
| celeborn.client.rpc.cache.concurrencyLevel | 32 | The number of write locks to update rpc cache. | 0.3.0 | celeborn.rpc.cache.concurrencyLevel | 
| celeborn.client.rpc.cache.expireTime | 15s | The time before a cache item is removed. | 0.3.0 | celeborn.rpc.cache.expireTime | 
| celeborn.client.rpc.cache.size | 256 | The max cache items count for rpc cache. | 0.3.0 | celeborn.rpc.cache.size | 
| celeborn.client.rpc.getReducerFileGroup.askTimeout | &lt;value of celeborn.&lt;module&gt;.io.connectionTimeout&gt; | Timeout for ask operations during getting reducer file group information. During this process, there are `celeborn.client.requestCommitFiles.maxRetries` times for retry opportunities for committing files and 1 times for releasing slots request. User can customize this value according to your setting. By default, the value is the max timeout value `celeborn.<module>.io.connectionTimeout`. | 0.2.0 |  | 
| celeborn.client.rpc.maxRetries | 3 | Max RPC retry times in LifecycleManager. | 0.3.2 |  | 
| celeborn.client.rpc.registerShuffle.askTimeout | &lt;value of celeborn.&lt;module&gt;.io.connectionTimeout&gt; | Timeout for ask operations during register shuffle. During this process, there are two times for retry opportunities for requesting slots, one request for establishing a connection with Worker and `celeborn.client.reserveSlots.maxRetries` times for retry opportunities for reserving slots. User can customize this value according to your setting. By default, the value is the max timeout value `celeborn.<module>.io.connectionTimeout`. | 0.3.0 | celeborn.rpc.registerShuffle.askTimeout | 
| celeborn.client.rpc.requestPartition.askTimeout | &lt;value of celeborn.&lt;module&gt;.io.connectionTimeout&gt; | Timeout for ask operations during requesting change partition location, such as reviving or splitting partition. During this process, there are `celeborn.client.reserveSlots.maxRetries` times for retry opportunities for reserving slots. User can customize this value according to your setting. By default, the value is the max timeout value `celeborn.<module>.io.connectionTimeout`. | 0.2.0 |  | 
| celeborn.client.rpc.reserveSlots.askTimeout | &lt;value of celeborn.rpc.askTimeout&gt; | Timeout for LifecycleManager request reserve slots. | 0.3.0 |  | 
| celeborn.client.rpc.shared.threads | 16 | Number of shared rpc threads in LifecycleManager. | 0.3.2 |  | 
| celeborn.client.shuffle.batchHandleChangePartition.interval | 100ms | Interval for LifecycleManager to schedule handling change partition requests in batch. | 0.3.0 | celeborn.shuffle.batchHandleChangePartition.interval | 
| celeborn.client.shuffle.batchHandleChangePartition.threads | 8 | Threads number for LifecycleManager to handle change partition request in batch. | 0.3.0 | celeborn.shuffle.batchHandleChangePartition.threads | 
| celeborn.client.shuffle.batchHandleCommitPartition.interval | 5s | Interval for LifecycleManager to schedule handling commit partition requests in batch. | 0.3.0 | celeborn.shuffle.batchHandleCommitPartition.interval | 
| celeborn.client.shuffle.batchHandleCommitPartition.threads | 8 | Threads number for LifecycleManager to handle commit partition request in batch. | 0.3.0 | celeborn.shuffle.batchHandleCommitPartition.threads | 
| celeborn.client.shuffle.batchHandleReleasePartition.interval | 5s | Interval for LifecycleManager to schedule handling release partition requests in batch. | 0.3.0 |  | 
| celeborn.client.shuffle.batchHandleReleasePartition.threads | 8 | Threads number for LifecycleManager to handle release partition request in batch. | 0.3.0 |  | 
| celeborn.client.shuffle.compression.codec | LZ4 | The codec used to compress shuffle data. By default, Celeborn provides three codecs: `lz4`, `zstd`, `none`. | 0.3.0 | celeborn.shuffle.compression.codec,remote-shuffle.job.compression.codec | 
| celeborn.client.shuffle.compression.zstd.level | 1 | Compression level for Zstd compression codec, its value should be an integer between -5 and 22. Increasing the compression level will result in better compression at the expense of more CPU and memory. | 0.3.0 | celeborn.shuffle.compression.zstd.level | 
| celeborn.client.shuffle.decompression.lz4.xxhash.instance | &lt;undefined&gt; | Decompression XXHash instance for Lz4. Available options: JNI, JAVASAFE, JAVAUNSAFE. | 0.3.2 |  | 
| celeborn.client.shuffle.expired.checkInterval | 60s | Interval for client to check expired shuffles. | 0.3.0 | celeborn.shuffle.expired.checkInterval | 
| celeborn.client.shuffle.manager.port | 0 | Port used by the LifecycleManager on the Driver. | 0.3.0 | celeborn.shuffle.manager.port | 
| celeborn.client.shuffle.mapPartition.split.enabled | false | whether to enable shuffle partition split. Currently, this only applies to MapPartition. | 0.3.1 |  | 
| celeborn.client.shuffle.partition.type | REDUCE | Type of shuffle's partition. | 0.3.0 | celeborn.shuffle.partition.type | 
| celeborn.client.shuffle.partitionSplit.mode | SOFT | soft: the shuffle file size might be larger than split threshold. hard: the shuffle file size will be limited to split threshold. | 0.3.0 | celeborn.shuffle.partitionSplit.mode | 
| celeborn.client.shuffle.partitionSplit.threshold | 1G | Shuffle file size threshold, if file size exceeds this, trigger split. | 0.3.0 | celeborn.shuffle.partitionSplit.threshold | 
| celeborn.client.shuffle.rangeReadFilter.enabled | false | If a spark application have skewed partition, this value can set to true to improve performance. | 0.2.0 | celeborn.shuffle.rangeReadFilter.enabled | 
| celeborn.client.shuffle.register.filterExcludedWorker.enabled | false | Whether to filter excluded worker when register shuffle. | 0.4.0 |  | 
| celeborn.client.slot.assign.maxWorkers | 10000 | Max workers that slots of one shuffle can be allocated on. Will choose the smaller positive one from Master side and Client side, see `celeborn.master.slot.assign.maxWorkers`. | 0.3.1 |  | 
| celeborn.client.spark.fetch.throwsFetchFailure | false | client throws FetchFailedException instead of CelebornIOException | 0.4.0 |  | 
| celeborn.client.spark.push.sort.memory.threshold | 64m | When SortBasedPusher use memory over the threshold, will trigger push data. | 0.3.0 | celeborn.push.sortMemory.threshold | 
| celeborn.client.spark.push.unsafeRow.fastWrite.enabled | true | This is Celeborn's optimization on UnsafeRow for Spark and it's true by default. If you have changed UnsafeRow's memory layout set this to false. | 0.2.2 |  | 
| celeborn.client.spark.shuffle.forceFallback.enabled | false | Whether force fallback shuffle to Spark's default. | 0.3.0 | celeborn.shuffle.forceFallback.enabled | 
| celeborn.client.spark.shuffle.forceFallback.numPartitionsThreshold | 2147483647 | Celeborn will only accept shuffle of partition number lower than this configuration value. | 0.3.0 | celeborn.shuffle.forceFallback.numPartitionsThreshold | 
| celeborn.client.spark.shuffle.writer | HASH | Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine when memory pressure is high or shuffle partition count is huge. | 0.3.0 | celeborn.shuffle.writer | 
| celeborn.master.endpoints | &lt;localhost&gt;:9097 | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 |  | 
| celeborn.storage.availableTypes | HDD | Enabled storages. Available options: MEMORY,HDD,SSD,HDFS. Note: HDD and SSD would be treated as identical. | 0.3.0 | celeborn.storage.activeTypes | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | HDFS base directory for Celeborn to store shuffle data. | 0.2.0 |  | 
<!--end-include-->
