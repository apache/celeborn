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

# Master
The main functions of Celeborn `Master` are:

- Maintain overall status of Celeborn cluster
- Maintain active shuffles
- Pursue High Availability
- Allocate slots for every shuffle according to cluster status

## Maintain Cluster Status
Upon start, `Worker` will register itself to `Master`. After that, `Worker` periodically sends heartbeat to `Master`,
carrying the following information:

- Disk status for each disk on the `Worker`
- Active shuffle id list served on the `Worker`

The disk status contains the following information:

- Health status
- Usable space
- Active slots
- Flush/Fetch speed in the last time window

When a `Worker`'s heartbeat times out, `Master` will consider it lost and removes it. If in the future
`Master` receives heartbeat from an unknown `Worker`, it tells the `Worker` to register itself.

When `Master` finds all disks in a `Worker` unavailable, it excludes the `Worker` from allocating slots until future
heartbeat renews the disk status.

Upon graceful shut down, `Worker` sends `ReportWorkerUnavailable` to `Master`. `Master` puts it in shutdown-workers
list. If in the future `Master` receives register request from that worker again, it removes it from the list.

Upon decommission or immediately shut down, `Worker` sends `WorkerLost` to `Master`, `Master` just removes the `Worker`
information.

## Maintain Active Shuffles
Application failure is common, Celeborn needs a way to decide whether an app is alive to clean up resource.
To achieve this, `LifecycleManager` periodically sends heartbeat to `Master`. If `Master` finds an app's heartbeat
times out, it considers the app fails, even though the app resends heartbeat in the future.

`Master` keeps all shuffle ids it has allocated slots for. Upon app heartbeat timeout or receiving UnregisterShuffle,
it removes the related shuffle ids. Upon receiving heartbeat from `Worker`, `Master` compares local shuffle ids
with `Worker`'s, and tells the `Worker` to clean up the unknown shuffles.

Heartbeat for `LifecycleManager` also carries total file count and bytes written by the app. `Master` calculates
estimated file size by `Sum(bytes) / Sum(files)` every 10 minutes using the newest metrics. To resist from impact of
small files, only files larger than threshold (defaults to 8MiB) will be considered.

## High Availability
Celeborn achieves `Master` HA through Raft.

Practically, `Master` replicates cluster and shuffle information among
multiple participants of `Ratis`. Any state-changing RPC will only be ACKed after the leader replicates logs to the
majority of participants.

## Slots Allocation
Upon receiving `RequestSlots`, `Master` allocates a (pair of) slot for each `PartitionLocation` of the shuffle. As `Master`
maintains all disks' status of all `Worker`s, it can leverage that information to achieve better load balance.

Currently, Celeborn supports two allocation strategies:

- Round Robin
- Load Aware

For both strategies, `Master` will only allocate slots on active `Worker`s with available disks.

During the allocation process, `Master` also simulates the space usage. For example, say a disk's usable space is 1GiB,
and the estimated file size for each `PartitionLocation` is 64MiB, then at most 16 slots will be allocated on that disk.

#### Round Robin
Round Robin is the simplest allocation strategy. The basic idea is:

- Calculate available slots that can be allocated on each disk
- Allocate slots among all `Worker`s and all disks in a round-robin fashion, decrement one after allocating, and
  exclude if no slots available on a disk or `Worker`
- If the cluster's total available slots is not enough, re-run the algorithm for un-allocated slots as if each
  disk has infinite capacity

#### Load Aware
For heterogeneous clusters, `Worker`s may have different CPU/disk/network performance, so it's necessary to allocate
different workloads based on metrics.

Currently, Celeborn allocates slots on disks based on flush and fetch performance in the last time window. As mentioned
before, disk status in heartbeat from `Worker` contains flush and fetch speed. `Master` put all available disks
into different groups based on performance metrics, then assign slots into different groups in a gradient descent way.

Inside each group, how many slots should be assigned on each disk is calculated according to their usable space.

For example, totally four disks are put into two groups with gradient 0.5, say I want to allocate 1500 slots, then
`Master` will assign the faster group 1000 slots, and the slower group 500 slots. Say the two disks in faster group
have 1GiB and 3GiB space, then they will be assigned 250 and 750 slots respectively.