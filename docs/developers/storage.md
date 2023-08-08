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

# Storage
This article describes the detailed design of Celeborn `Worker`'s storage management.

## `PartitionLocation` Physical Storage
Logically, `PartitionLocation` contains all data with the same partition id. Physically, Celeborn stores
`PartitionLocation` in multiple files, each file corresponds to one `PartitionLocation` object with a unique epoch
for the partition. All `PartitionLocation`s with the same partition id but different epochs aggregate to the complete 
data for the partition. The file can be in memory, local disks, or DFS/OSS, see `Multi-layered Storage` below.

A `PartitionLocation` file can be read only after it is committed, trigger by `CommitFiles` RPC.

## File Layout
Celeborn supports two kinds of partitions:

- `ReducePartition`, where each `PartitionLocation` file stores a portion of data with the same partition id,
  currently used for Apache Spark.
- `MapPartition`, where each `PartitionLocation` file stores a portion of data from the same map id, currently
  used for Apache Flink.

#### ReducePartition
The layout of `ReducePartition` is as follows:

![ReducePartition](../../assets/img/reducepartition.svg)

`ReducePartition` data file consists of several chunks (defaults to 8 MiB). Each data file has an in-memory index
which points to start positions of each chunk. Upon requesting data from some partition, `Worker` first returns the
index, then sequentially reads and returns a chunk upon each `ChunkFetchRequest`, which is very efficient.

Notice that chunk boundaries is simply decided by the current chunk's size. In case of replication, since the
order of data batch arrival is not guaranteed to be the same for primary and replica, chunks with the same chunk
index will probably contain different data in primary and replica. Nevertheless, the whole files in primary and
replica contain the same data batches in normal cases.

#### MapPartition
The layout of `MapPartition` is as follows:

![MapPartition](../../assets/img/mappartition.svg)

`MapPartition` data file consists of several regions (defaults to 64MiB), each region is sorted by partition id.
Each region has an in-memory index which points to start positions of each partition. Upon requesting data from
some partition, `Worker` reads the partition data from every region.

For more details about reading data, please refer to [ReadData](../../developers/readdata).

## Local Disk and Memory Buffer
To the time this article is written, the most common case is local disk only. Users specify directories and
capacity that Celeborn can use to store data. It is recommended to specify one directory per disk. If users
specify more directories on one disk, Celeborn will try to figure it out and manage in the disk-level
granularity.

`Worker` periodically checks disk health, isolates unhealthy or spaceless disks, and reports to `Master`
through heartbeat.

Upon receiving `ReserveSlots`, `Worker` will first try to create a `FileWriter` on the hinted disk. If that disk is
unavailable, `Worker` will choose a healthy one.

Upon receiving `PushData` or `PushMergedData`, `Worker` unpacks the data (for `PushMergedData`) and logically appends
to the buffered data for each `PartitionLocation` (no physical memory copy). If the buffer exceeds the threshold
(defaults to 256KiB), data will be flushed to the file asynchronously.

If data replication is turned on, `Worker` will send the data to replica asynchronously. Only after `Worker`
receives ACK from replica will it return ACK to `ShuffleClient`. Notice that it's not required that data is flushed
to file before sending ACK.

Upon receiving `CommitFiles`, `Worker` will flush all buffered data for `PartitionLocation`s specified in
the RPC and close files, then responds the succeeded and failed `PartitionLocation` lists.

## Trigger Split
Upon receiving `PushData` (note: currently receiving `PushMergedData` does not trigger Split, it's future work),
`Worker` will check whether disk usage exceeds disk reservation (defaults to 5GiB). If so, `Worker` will respond
Split to `ShuffleClient`.

Celeborn supports two configurable kinds of split:

- `HARD_SPLIT`, meaning old `PartitionLocation` epoch refuses to accept any data, and future data of the
  `PartitionLocation` will only be pushed after new `PartitionLocation` epoch is ready
- `SOFT_SPLIT`, meaning old `PartitionLocation` epoch continues to accept data, when new epoch is ready, `ShuffleClient`
  switches to the new location transparently

The detailed design of split can be found [Here](../../developers/shuffleclient#split).

## Self Check
In additional to health and space check on each disk, `Worker` also collects perf statistics to feed Master for
better [slots allocation](../../developers/master#slots-allocation):

- Average flush time of the last time window
- Average fetch time of the last time window

## Multi-layered Storage
Celeborn aims to store data in multiple layers, i.e. memory, local disks and distributed file systems(or object store
like S3, OSS). To the time this article is written, Celeborn supports local disks and HDFS.

The principles of data placement are:

- Try to cache small data in memory
- Always prefer faster storage
- Trade off between faster storage's space and cost of data movement

The high-level design of multi-layered storage is:

![storage](../../assets/img/multilayer.svg)

`Worker`'s memory is divided into two logical regions: `Push Region` and `Cache Region`. `ShuffleClient` pushes data
into `Push Region`, as ① indicates. Whenever the buffered data in `PushRegion` for a `PartitionLocation` exceeds the
threshold (defaults to 256KiB), `Worker` flushes it to some storage layer. The policy of data movement is as follows:

- If the `PartitionLocation` is not in `Cache Region` and `Cache Region` has enough space, logically move the data
  to `Cache Region`. Notice this just counts the data in `Cache Region` and does not physically do memory copy. As ②
  indicates.
- If the `PartitionLocation` is in `Cache Region`, logically append the current data, as ③ indicates.
- If the `PartitionLocation` is not in `Cache Region` and `Cache Region` does not have enough memory,
  flush the data into local disk, as ④ indicates.
- If the `PartitionLocation` is not in `Cache Region` and both `Cache Region` and local disk do not have enough memory,
  flush the data into DFS/OSS, as ⑤ indicates.
- If the `Cache Region` exceeds the threshold, choose the largest `PartitionLocation` and flush it to local disk, as ⑥
  indicates.
- Optionally, if local disk does not have enough memory, choose a `PartitionLocation` split and evict to HDFS/OSS.