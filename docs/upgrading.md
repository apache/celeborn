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

Upgrading
===

## Rolling upgrade

It is necessary to support a fast rolling upgrade process for the Celeborn cluster.
In order to achieve a fast and unaffected rolling upgrade process,
Celeborn should support that the written file in the worker should be committed
and support reading after the worker restarted. Celeborn have done the
following mechanism to support rolling upgrade.

### Background

**Fixed fetch port and client retry**

In the shuffle reduce side, the read client will obtain the worker's host/port and
information of the file to be read. In order to ensure that the data can be read
normally after the rolling restart process of the worker is completed,
the worker needs to use a fixed fetch service port,
the configuration is `celeborn.worker.fetch.port`, the default value is `0`.
At startup, it will automatically select a free port, user need to set a fixed value, such as `9092`.

At the same time, users need to adjust the number of retry times and retry wait time
of the client according to cluster rolling restart situation
to support the shuffle client to read data through retries after worker restarted.
The shuffle client fetch data retry times configuration is `celeborn.client.fetch.maxRetriesForEachReplica`, default value is `3`.
The shuffle client fetch data retry wait time configuration is `celeborn.data.io.retryWait`, default value is `5s`.
Users can increase the configuration value appropriately according to the situation.

**Worker store file meta information**

Shuffle client records the shuffle partition location's host, service port, and filename,
to support workers recovering reading existing shuffle data after worker restart,
during worker shutdown, workers should store the meta about reading shuffle partition files
in LevelDB, and restore the meta after restarting workers.
Users should set `celeborn.worker.graceful.shutdown.enabled` to `true` to enable graceful shutdown.
During this process, worker will wait all allocated partition's in this worker to be committed
within a timeout of `celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout`, which default value is `480s`.
Then worker will wait for partition sorter finish all sort task within a timeout of
`celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout`, which default value is `120s`.
The whole graceful shutdown process should be finished within a timeout of
`celeborn.worker.graceful.shutdown.timeout`, which default value is `600s`.

**Allocated partition do hard split and Pre-commit hard split partition**

As mentioned in the previous section that the worker needs to wait for all allocated partition files
to be committed during the restart process, which means that the worker need to wait for all the shuffle
running on this worker to finish running before restarting the worker, otherwise part of the information
will be lost, and abnormal partition files are left, and reading cannot be resumed.

In order to speed up the restart process, worker let all push data requests return the HARD_SPLIT flag
during worker shutdown, and shuffle client will re-apply for a new partition location for these allocated partitions.
Then client side can record all HARD_SPLIT partition information and pre-commit these partition,
then the worker side allocated partitions can be committed in a very short time. User should enable
`celeborn.client.shuffle.batchHandleCommitPartition.enabled`, the default value is false.

### Example setting

#### Worker

| Key                                                               | Value |
|-------------------------------------------------------------------|-------| 
| celeborn.worker.graceful.shutdown.enabled                         | true  |
| celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout      | 480s  |
| celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout | 120s  |
| celeborn.worker.graceful.shutdown.timeout                         | 600s  |
| celeborn.worker.fetch.port                                        | 9092  |

#### Client

| Key                                                              | Value |
|------------------------------------------------------------------|-------| 
| spark.celeborn.client.shuffle.batchHandleCommitPartition.enabled | true  |
| spark.celeborn.client.fetch.maxRetriesForEachReplica             | 5     |
| spark.celeborn.data.io.retryWait                                 | 10s   |
