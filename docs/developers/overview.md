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

# Celeborn Architecture

This article introduces high level Apache Celeborn™(Incubating) Architecture. For more detailed description of each module/process,
please refer to dedicated articles.

## Why Celeborn
In distributed compute engines, data exchange between compute nodes is common but expensive. The cost comes from
the disk and network inefficiency (M * N between Mappers and Reducers) in traditional shuffle frame, as following:

![ESS](../../assets/img/ess.svg)

Besides inefficiency, traditional shuffle framework requires large local storage in compute node to store shuffle
data, thus blocks the adoption of disaggregated architecture.

Apache Celeborn(Incubating) solves the problems by reorganizing shuffle data in a more efficient way, and storing the data in
a separate service. The high level architecture of Celeborn is as follows:

![Celeborn](../../assets/img/celeborn.svg)

## Components
Celeborn(Incubating) has three primary components: Master, Worker, and Client.

- Master manages Celeborn cluster and achieves high availability(HA) based on Raft.
- Worker processes read-write requests.
- Client writes/reads data to/from Celeborn cluster, and manages shuffle metadata for the application.

In most distributed compute engines, there are typically two roles: one role for application lifecycle management
and task orchestration, i.e. `Driver` in Spark and `JobMaster` for Flink; the other role for executing tasks, i.e.
`Executor` in Spark and `TaskManager` for Flink.

Similarly, Celeborn Client is also divided into two roles: `LifecycleManager` for control plane, responsible for
managing all shuffle metadata for the application; and `ShuffleClient` for data plane, responsible for write/read
data to/from Workers.

`LifecycleManager` resides in `Driver` or `JobMaster`, one instance in each application; `ShuffleClient` resides in
each `Executor` or `TaskManager`, one instance in each process of `Executor`/`TaskManager`.

## Shuffle Lifecycle
A typical lifecycle of a shuffle with Celeborn is as follows:

1. Client sends `RegisterShuffle` to Master. Master allocates slots among Workers and responds to Client.
2. Client sends `ReserveSlots` to Workers. Workers reserve slots for the shuffle and responds to Client.
3. Clients push data to allocated Workers. Data of the same `partitionId` are pushed to the same logical `PartitionLocation`.
4. After all Clients finishes pushing data, Client sends `CommitFiles` to each Worker. Workers commit data
   for the shuffle then respond to Client.
5. Clients send `OpenStream` to Workers for each partition split file to prepare for reading.
6. Clients send `ChunkFetchRequest` to Workers to read chunks.
7. After Client finishes reading data, Client sends `UnregisterShuffle` to Master to release resources.

## Data Reorganization
Celeborn improves disk and network efficiency through data reorganization. Typically, Celeborn stores all shuffle data
with the same `partitionId` in a logical `PartitionLocation`.

In normal cases each `PartitionLocation` corresponds to a single file. When a reducer requires for the partition's data,
it just needs one network connection and sequentially read the coarse grained file.

In abnormal cases, such as when the file grows too large, or push data fails, Celeborn spawns a new split of the
`PartitionLocation`, and future data within the partition will be pushed to the new split.

`LifecycleManager` keeps the split information and tells reducer to read from all splits of the `PartitionLocation`
to guarantee no data is lost.

## Data Storage
Celeborn stores shuffle data in configurable multiple layers, i.e. `Memroy`, `Local Disks`, `Distributed File System`,
and `Object Store`. Users can specify any combination of the layers on each Worker.

Currently, Celeborn only supports `Local Disks` and `HDFS`. Supporting for other storage systems are under working.

## Compute Engine Integration
Celeborn's primary components(i.e. Master, Worker, Client) are engine irrelevant. The Client APIs are extensible
and easy to implement plugins for various engines.

Currently, Celeborn officially supports [Spark](https://spark.apache.org/)(both Spark 2.x and Spark 3.x),
[Flink](https://flink.apache.org/)(1.14/1.15/1.17), and
[Gluten](https://github.com/oap-project/gluten). Also developers are integrating Celeborn with other engines,
for example [MR3](https://mr3docs.datamonad.com/docs/mr3/).

Celeborn community is also working on integrating Celeborn with other engines.

## Graceful Shutdown
In order not to impact running applications when upgrading Celeborn Cluster, Celeborn implements Graceful Upgrade.

When graceful shutdown is turned on, upon shutdown, Celeborn will do the following things:

1. Master will not allocate slots on the Worker
2. Worker will inform Clients to split
3. Client will send `CommitFiles` to the Worker

Then the Worker waits until all `PartitionLocation` flushes data to persistent storage, stores states in local leveldb,
then stops itself. The process is typically within one minute.

For more details, please refer to [Rolling upgrade](../../upgrading/#rolling-upgrade)
