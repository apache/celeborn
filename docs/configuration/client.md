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
| celeborn.application.heartbeatInterval | 10s | Interval for client to send heartbeat message to master. | 0.2.0 | 
| celeborn.client.blacklistSlave.enabled | true | When true, Celeborn will add partition's peer worker into blacklist when push data to slave failed. | 0.3.0 | 
| celeborn.client.closeIdleConnections | true | Whether client will close idle connections. | 0.3.0 | 
| celeborn.client.maxRetries | 15 | Max retry times for client to connect master endpoint | 0.2.0 | 
| celeborn.client.network.memory.perResultPartition | 64m | The size of network buffers required per result partition. The minimum valid value is 8M. Usually, several hundreds of megabytes memory is enough for large scale batch jobs. | 0.3.0 | 
| celeborn.fetch.maxReqsInFlight | 3 | Amount of in-flight chunk fetch request. | 0.2.0 | 
| celeborn.fetch.maxRetries | 3 | Max retries of fetch chunk | 0.2.0 | 
| celeborn.fetch.timeout | 120s | Timeout for a task to fetch chunk. | 0.2.0 | 
| celeborn.master.endpoints | &lt;localhost&gt;:9097 | Endpoints of master nodes for celeborn client to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. | 0.2.0 | 
| celeborn.push.buffer.initial.size | 8k |  | 0.2.0 | 
| celeborn.push.buffer.max.size | 64k | Max size of reducer partition buffer memory for shuffle hash writer. The pushed data will be buffered in memory before sending to Celeborn worker. For performance consideration keep this buffer size higher than 32K. Example: If reducer amount is 2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` heap memory. | 0.2.0 | 
| celeborn.push.data.timeout | 120s | Timeout for a task to push data rpc message. This value should better be more than twice of `celeborn.push.timeoutCheck.interval` | 0.2.0 | 
| celeborn.push.limit.inFlight.sleepInterval | 50ms | Sleep interval when check netty in-flight requests to be done. | 0.2.0 | 
| celeborn.push.limit.inFlight.timeout | &lt;undefined&gt; | Timeout for netty in-flight requests to be done.Default value should be `celeborn.push.data.timeout * 2`. | 0.2.0 | 
| celeborn.push.limit.strategy | SIMPLE | The strategy used to control the push speed. Valid strategies are SIMPLE and SLOWSTART. the SLOWSTART strategy is usually cooperate with congest control mechanism in the worker side. | 0.3.0 | 
| celeborn.push.maxReqsInFlight | 4 | Amount of Netty in-flight requests per worker. The maximum memory is `celeborn.push.maxReqsInFlight` * `celeborn.push.buffer.max.size` * compression ratio(1 in worst case), default: 64Kib * 32 = 2Mib | 0.2.0 | 
| celeborn.push.queue.capacity | 512 | Push buffer queue size for a task. The maximum memory is `celeborn.push.buffer.max.size` * `celeborn.push.queue.capacity`, default: 64KiB * 512 = 32MiB | 0.2.0 | 
| celeborn.push.replicate.enabled | true | When true, Celeborn worker will replicate shuffle data to another Celeborn worker asynchronously to ensure the pushed shuffle data won't be lost after the node failure. | 0.2.0 | 
| celeborn.push.retry.threads | 8 | Thread number to process shuffle re-send push data requests. | 0.2.0 | 
| celeborn.push.revive.maxRetries | 5 | Max retry times for reviving when celeborn push data failed. | 0.3.0 | 
| celeborn.push.slowStart.initialSleepTime | 500ms | The initial sleep time if the current max in flight requests is 0 | 0.3.0 | 
| celeborn.push.slowStart.maxSleepTime | 2s | If celeborn.push.limit.strategy is set to SLOWSTART, push side will take a sleep strategy for each batch of requests, this controls the max sleep time if the max in flight requests limit is 1 for a long time | 0.3.0 | 
| celeborn.push.sortMemory.threshold | 64m | When SortBasedPusher use memory over the threshold, will trigger push data. | 0.2.0 | 
| celeborn.push.splitPartition.threads | 8 | Thread number to process shuffle split request in shuffle client. | 0.2.0 | 
| celeborn.push.stageEnd.timeout | &lt;undefined&gt; | Timeout for waiting StageEnd. Default value should be `celeborn.rpc.askTimeout * (celeborn.rpc.requestCommitFiles.maxRetries + 1)`. | 0.2.0 | 
| celeborn.rpc.cache.concurrencyLevel | 32 | The number of write locks to update rpc cache. | 0.2.0 | 
| celeborn.rpc.cache.expireTime | 15s | The time before a cache item is removed. | 0.2.0 | 
| celeborn.rpc.cache.size | 256 | The max cache items count for rpc cache. | 0.2.0 | 
| celeborn.rpc.getReducerFileGroup.askTimeout | &lt;undefined&gt; | Timeout for ask operations during get reducer file group. Default value should be `celeborn.rpc.askTimeout * (celeborn.rpc.requestCommitFiles.maxRetries + 1 + 1)`. | 0.2.0 | 
| celeborn.rpc.maxParallelism | 1024 | Max parallelism of client on sending RPC requests. | 0.2.0 | 
| celeborn.rpc.registerShuffle.askTimeout | &lt;undefined&gt; | Timeout for ask operations during register shuffle. Default value should be `celeborn.rpc.askTimeout * (celeborn.slots.reserve.maxRetries + 1 + 1)`. | 0.2.0 | 
| celeborn.rpc.requestCommitFiles.maxRetries | 2 | Max retry times for requestCommitFiles RPC. | 1.0.0 | 
| celeborn.rpc.requestPartition.askTimeout | &lt;undefined&gt; | Timeout for ask operations during request change partition location, such as revive or split partition. Default value should be `celeborn.rpc.askTimeout * (celeborn.slots.reserve.maxRetries + 1)`. | 0.2.0 | 
| celeborn.shuffle.batchHandleChangePartition.enabled | false | When true, LifecycleManager will handle change partition request in batch. Otherwise, LifecycleManager will process the requests one by one | 0.2.0 | 
| celeborn.shuffle.batchHandleChangePartition.interval | 100ms | Interval for LifecycleManager to schedule handling change partition requests in batch. | 0.2.0 | 
| celeborn.shuffle.batchHandleChangePartition.threads | 8 | Threads number for LifecycleManager to handle change partition request in batch. | 0.2.0 | 
| celeborn.shuffle.batchHandleCommitPartition.enabled | false | When true, LifecycleManager will handle commit partition request in batch. Otherwise, LifecycleManager won't commit partition before stage end | 0.2.0 | 
| celeborn.shuffle.batchHandleCommitPartition.interval | 5s | Interval for LifecycleManager to schedule handling commit partition requests in batch. | 0.2.0 | 
| celeborn.shuffle.batchHandleCommitPartition.threads | 8 | Threads number for LifecycleManager to handle commit partition request in batch. | 0.2.0 | 
| celeborn.shuffle.chuck.size | 8m | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. | 0.2.0 | 
| celeborn.shuffle.compression.codec | LZ4 | The codec used to compress shuffle data. By default, Celeborn provides two codecs: `lz4` and `zstd`. | 0.2.0 | 
| celeborn.shuffle.compression.zstd.level | 1 | Compression level for Zstd compression codec, its value should be an integer between -5 and 22. Increasing the compression level will result in better compression at the expense of more CPU and memory. | 0.2.0 | 
| celeborn.shuffle.expired.checkInterval | 60s | Interval for client to check expired shuffles. | 0.2.0 | 
| celeborn.shuffle.forceFallback.enabled | false | Whether force fallback shuffle to Spark's default. | 0.2.0 | 
| celeborn.shuffle.forceFallback.numPartitionsThreshold | 500000 | Celeborn will only accept shuffle of partition number lower than this configuration value. | 0.2.0 | 
| celeborn.shuffle.manager.port | 0 | Port used by the LifecycleManager on the Driver. | 0.2.0 | 
| celeborn.shuffle.partition.type | REDUCE | Type of shuffle's partition. | 0.2.0 | 
| celeborn.shuffle.partitionSplit.mode | SOFT | soft: the shuffle file size might be larger than split threshold. hard: the shuffle file size will be limited to split threshold. | 0.2.0 | 
| celeborn.shuffle.partitionSplit.threshold | 1G | Shuffle file size threshold, if file size exceeds this, trigger split. | 0.2.0 | 
| celeborn.shuffle.rangeReadFilter.enabled | false | If a spark application have skewed partition, this value can set to true to improve performance. | 0.2.0 | 
| celeborn.shuffle.register.maxRetries | 3 | Max retry times for client to register shuffle. | 0.2.0 | 
| celeborn.shuffle.register.retryWait | 3s | Wait time before next retry if register shuffle failed. | 0.2.0 | 
| celeborn.shuffle.writer | HASH | Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine when memory pressure is high or shuffle partition count is huge. | 0.2.0 | 
| celeborn.slots.reserve.maxRetries | 3 | Max retry times for client to reserve slots. | 0.2.0 | 
| celeborn.slots.reserve.retryWait | 3s | Wait time before next retry if reserve slots failed. | 0.2.0 | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | HDFS dir configuration for Celeborn to access HDFS. | 0.2.0 | 
| celeborn.test.fetchFailure | false | Whether to test fetch chunk failure | 0.2.0 | 
| celeborn.test.retryCommitFiles | false | Fail commitFile request for test | 0.2.0 | 
| celeborn.test.retryRevive | false | Fail push data and request for test | 0.2.0 | 
| celeborn.worker.excluded.checkInterval | 30s | Interval for client to refresh excluded worker list. | 0.2.0 | 
| celeborn.worker.excluded.expireTimeout | 600s | Timeout time for LifecycleManager to clear reserved excluded worker. | 0.2.0 | 
<!--end-include-->
