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

# Fault Tolerant
This article describes the detailed design of Celeborn's fault-tolerant.

In addition to data replication to handle `Worker` lost, Celeborn tries to handle exceptions during shuffle
as much as possible, especially the following:

- When `PushData`/`PushMergedData` fail
- When fetch chunk fails
- When disk is unhealthy or reaching limit

This article is based on [ReducePartition](../../developers/storage#reducepartition).

## Handle PushData Failure
The detailed description of push data can be found in [PushData](../../developers/shuffleclient#pushdata). Push data can fail for
various reasons, i.e. CPU high load, network fluctuation, JVM GC, `Worker` lost. 

Celeborn does not eagerly consider `Worker` lost when push data fails, instead it considers it as temporary
unavailable, and asks for another (pair of) `PartitionLocation`(s) on different `Worker`(s) to continue pushing.
The process is called `Revive`:

![Revive](../../assets/img/revive.svg)

Handling [PushMergedData](../../developers/shuffleclient#push-or-merge) failure is similar but more complex. Currently,
`PushMergedData` is in all-or-nothing fashion, meaning either all data batches in the request succeed or all fail.
Partial success is not supported yet.

Upon `PushMergedData` failure, `ShuffleClient` first unpacks and revives for every data batch. Notice that previously
all data batches in `PushMergedData` have the same primary and replica (if any) destination, after reviving new
`PartitionLocation`s can spread across multiple `Worker`s.

Then `ShuffleClient` groups the new `PartitionLocations` in the same way as before, resulting in multiple
`PushMergedData` requests, then send them to their destinations.

Celeborn detects data lost when processing `CommitFiles` (See [Worker](../..developers/overview#shuffle-lifecycle)).
Celeborn considers no `DataLost` if and only if every `PartitionLocation` has succeeded to commit at least one replica
(if replication is turned off, there is only one replica for each `PartitionLocation`).

When a `Worker` is down, all `PartitionLocation`s on the `Worker` will be revived, causing `Revive` RPC flood
to `LifecycleManager`. To alleviate this, `ShuffleClient` batches all `Revive` requests before sending to
`LifecycleManager`:

![BatchRevive](../../assets/img/batchrevive.svg)

## Handle Fetch Failure
As [ReducePartition](../../developers/storage#reducepartition) describes, data file consists of chunks, `ShuffleClient`
asks for a chunk once a time.

`ShuffleClient` defines the max number of retries for each replica(defaults to 3). When fetch chunk fails,
`ShuffleClient` will try another replica (in case where replication is off, retry the same one).

If the max retry number exceeds, `ShuffleClient` gives up retrying and throws Exception.

## Disk Check
`Worker` periodically checks disk health and usage. When health check fails, `Worker` isolates the disk and will
not allocate slots on it until it becomes healthy again.

Similarly, if usable space goes less than threshold (defaults to 5GiB), `Worker` will not allocate slots on it. In
addition, to avoid exceeding space, `Worker` will trigger `HARD_SPLIT` for all `PartitionLocation`s on the disk to
avoid file size growth.

## Exactly Once
It can happen that `Worker` successfully receives and writes a data batch but fails to send ACK to `ShuffleClient`, or
primary successfully receives and writes a data batch but replica fails. Also, different task attempts
(i.e. speculative execution) will push the same data twice.

In a word, it can happen that the same data batch are duplicated across `PartitionLocation` splits. To guarantee
exactly once, Celeborn ensures no data is lost, and no duplicate read:

- For each data batch, `ShuffleClient` adds a `(Map Id, Attempt Id, Batch Id)` header, in which
  `Batch Id` is a unique id for the data batch in the map attempt
- `LifecycleManager` keeps all `PartitionLocation`s with the same partition id
- For each `PartitionLocation` split, at least one replica is successfully committed before shuffle read
- `LifecycleManager` records the successful task attempt for each map id, and only data from that attempt is read
  for the map id
- `ShuffleClient` discards data batches with a batch id that it has already read
