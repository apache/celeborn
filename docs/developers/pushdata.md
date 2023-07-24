---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---


# Push Data

This article describes the detailed design of the process of push data.

## API specification
The push data API is as follows:
```java
  public abstract int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException;
```

- `shuffleId` is the unique shuffle id of the application
- `mapId` is the map id of the shuffle
- `attemptId` is the attempt id of the map task, i.e. speculative task or task rerun for Apache Spark
- `partitionId` is the partition id the data belongs to
- `data`,`offset`,`length` specifies the bytes to be pushed
- `numMappers` is the number map tasks in the shuffle
- `numPartitions` is the number of partitions in the shuffle

## Lazy Shuffle Register
The first time `pushData` is called, Client will check whether the shuffle id has been registered. If not,
it sends `RegisterShuffle` to `LifecycleManager`, `LifecycleManager` then sends `RequestSlots` to `Master`.
`RequestSlots` specifies how many `PartitionLocation`s this shuffle requires, each `PartitionLocation` logically
responds to data of some partition id.

Upon receiving `RequestSlots`, `Master` allocates slots for the shuffle among `Worker`s. If replication is turned on,
`Master` allocates a pair of `Worker`s for each `PartitionLocation` to store two replicas for each `PartitionLocation`.
The detailed allocation strategy can be found in [Slots Allocation](../../developers/slotsallocation). `Master` then
responds to `LifecycleManager` with the allocated `PartitionLocation`s.

`LifcycleManager` caches the `PartitionLocation`s for the shuffle and responds to each `RegisterShuffle` RPCs from
`ShuffleClient`s.

## Normal Push
In normal cases, the process of pushing data is as follows:

- `ShuffleClient` compresses data, currently supports `zstd` and `lz4`
- `ShuffleClient` addes Header for the data: `mapId`, `attemptId`, `batchId` and `size`. The `bastchId` is a unique
  id for the data batch inside the (`mapId`, `attemptId`), for the purpose of de-duplication
- `ShuffleClient` sends `PushData` to the `Worker` on which the current `PartitionLocation` is allocated, and holds push
  state for this pushing
- `Worker` receives the data, do replication if needed, then responds success ACK to `ShuffleClient`. For more details
  about how data is replicated and stored in `Worker`s, please refer to [Worker](../../developers/worker)
- Upon receiving success ACK from `Worker`, `ShuffleClient` considers success for this pushing and modifies the push state

## Push or Merge?
If the size of data to be pushed is small, say hundreds of bytes, it will be very inefficient to send to the wire.
So `ShuffleClient` offers another API: `mergeData` to batch data locally before sending to `Worker`.

`mergeData` merges data with the same target into `DataBatches`. `Same target` means the destination for both the
primary and replica are the same. When the size of a `DataBatches` exceeds a threshold (defaults to`64KiB`),
`ShuffleClient` triggers pushing and sends `PushMergedData` to the destination.

Upon receiving `PushMergedData`, `Worker` unpacks it into data segments each for a specific `PartitionLocation`, then
stores them accordingly.

## Async Push
Celeborn's `ShuffleClient` does not block compute engine's execution by asynchronous pushing, implemented in
`DataPusher`.

Whenever compute engine decides to push data, it calls `DataPusher#addTask`, `DataPusher` creates a `PushTask` which
contains the data, and added the `PushTask` in a non-blocking queue. `DataPusher` continuously poll the queue
and invokes `ShuffleClient#pushData` to do actual push.

## Split
As mentioned before, Celeborn will split a `PartitionLocation` when any of the following conditions happens:

- `PartitionLocation` file exceeds threshold (defaults to 1GiB)
- Usable space of local disk is less than threshold (defaults to 5GiB)
- `Worker` is in `Graceful Shutdown` state
- Push data fails

For the first three cases, `Worker` informs `ShuffleClient` that it should trigger split; for the last case,
`ShuffleClient` triggers split itself.

There are two kinds of Split:
- `HARD_SPLIT`, meaning old `PartitionLocation` epoch refuses to accept any data, and future data of the
  `PartitionLocation` will only be pushed after new `PartitionLocation` epoch is ready
- `SOFT_SPLIT`, meaning old `PartitionLocation` epoch continues to accept data, when new epoch is ready, `ShuffleClient`
  switches to the new location transparently

The process of `SOFT_SPLIT` is as follows:

![softsplit](../../assets/img/softsplit.svg)

`LifecycleManager` keeps the split information and tells reducer to read from all splits of the `PartitionLocation`
to guarantee no data is lost.