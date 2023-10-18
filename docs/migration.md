---
hide:
  - navigation

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

# Migration Guide

## Upgrading from 0.3.1 to 0.3.2

- Since 0.3.1, Celeborn changed the default value of `raft.client.rpc.request.timeout` from `3s` to `10s`.

- Since 0.3.1, Celeborn changed the default value of `raft.client.rpc.watch.request.timeout` from `10s` to `20s`.

## Upgrading from 0.3.0 to 0.3.1

- Since 0.3.1, Celeborn changed the default value of `celeborn.worker.directMemoryRatioToResume` from `0.5` to `0.7`.

- Since 0.3.1, Celeborn changed the default value of `celeborn.worker.monitor.disk.check.interval` from `60` to `30`.

- Since 0.3.1, name of JVM metrics changed, see details at CELEBORN-1007.

## Upgrading from 0.2 to 0.3

 - Celeborn 0.2 Client is compatible with 0.3 Master/Server, it allows to upgrade Master/Worker first then Client.
   Note that: It's strongly recommended to use the same version of Client and Celeborn Master/Worker in production.

 - Since 0.3.0, the support of deprecated configurations `rss.*` is removed.
   All configurations listed in 0.2.1 docs still take effect, but some of those are deprecated too, please read
   the bootstrap logs and follow the suggestion to migrate to the new configuration.

 - From 0.3.0 on the default value for `celeborn.client.push.replicate.enabled` is changed from `true` to `false`, users
   who want replication on should explicitly enable replication. For example, to enable replication for Spark
   users should add the spark config when submitting job: `spark.celeborn.client.push.replicate.enabled=true`

 - From 0.3.0 on the default value for `celeborn.worker.storage.workingDir` is changed from `hadoop/rss-worker/shuffle_data` to `celeborn-worker/shuffle_data`,
   users who want to use origin working dir path should set this configuration.

 - Since 0.3.0, configuration namespace `celeborn.ha.master` is deprecated, and will be removed in the future versions.
   All configurations `celeborn.ha.master.*` should migrate to `celeborn.master.ha.*`.

 - Since 0.3.0, environment variables `CELEBORN_MASTER_HOST` and `CELEBORN_MASTER_PORT` are removed.
   Instead `CELEBORN_LOCAL_HOSTNAME` works on both master and worker, which takes high priority than configurations defined in properties file.

 - Since 0.3.0, the Celeborn Master URL schema is changed from `rss://` to `celeborn://`, for users who start Worker by
   `sbin/start-worker.sh rss://<master-host>:<master-port>`, should migrate to `sbin/start-worker.sh celeborn://<master-host>:<master-port>`.

 - Since 0.3.0, Celeborn supports overriding Hadoop configuration(`core-site.xml`, `hdfs-site.xml`, etc.) from Celeborn configuration with the additional prefix `celeborn.hadoop.`. 
   On Spark client side, user should set Hadoop configuration like `spark.celeborn.hadoop.foo=bar`, note that `spark.hadoop.foo=bar` does not take effect;
   on Flink client and Celeborn Master/Worker side, user should set like `celeborn.hadoop.foo=bar`.

 - Since 0.3.0, Celeborn master metrics `BlacklistedWorkerCount` is renamed as `ExcludedWorkerCount`.

 - Since 0.3.0, Celeborn master http request url `/blacklistedWorkers` is renamed as `/excludedWorkers`.

 - Since 0.3.0, introduces a terminology update for Celeborn worker data replication, replacing the previous `master/slave` terminology with `primary/replica`. In alignment with this change, corresponding metrics keywords have been adjusted.
   The following table presents a comprehensive overview of the changes:

     | Key Before v0.3.0             | Key After v0.3.0               |
     |-------------------------------|--------------------------------|
     | `MasterPushDataTime`          | `PrimaryPushDataTime`          |
     | `MasterPushDataHandshakeTime` | `PrimaryPushDataHandshakeTime` |
     | `MasterRegionStartTime`       | `PrimaryRegionStartTime`       |
     | `MasterRegionFinishTime`      | `PrimaryRegionFinishTime`      |
     | `SlavePushDataTime`           | `ReplicaPushDataTime`          |
     | `SlavePushDataHandshakeTime`  | `ReplicaPushDataHandshakeTime` |
     | `SlaveRegionStartTime`        | `ReplicaRegionStartTime`       |
     | `SlaveRegionFinishTime`       | `ReplicaRegionFinishTime`      |

 - Since 0.3.0, Celeborn's spark shuffle manager change from `org.apache.spark.shuffle.celeborn.RssShuffleManager` to `org.apache.spark.shuffle.celeborn.SparkShuffleManager`. User can set spark property `spark.shuffle.manager` to `org.apache.spark.shuffle.celeborn.SparkShuffleManager` to use Celeborn remote shuffle service.
   In 0.3.0, Celeborn still support `org.apache.spark.shuffle.celeborn.RssShuffleManager`, it will be removed in 0.4.0.
