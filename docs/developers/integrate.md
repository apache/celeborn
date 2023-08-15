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

# Integrating Celeborn
## Overview
The core components of Celeborn, i.e. `Master`, `Worker`, and `Client` are all engine irrelevant. Developers can
integrate Celeborn with various engines or applications by using or extending Celeborn's `Client`, as the officially
supported plugins for Apache Spark and Apache Flink, see [Spark Plugin](../../developers/spark) and 
[Flink Plugin](../../developers/flink).

This article briefly describes an example of integrating Celeborn into a simple distributed application using
Celeborn `Client`.

## Background
Say we have an distributed application who has two phases:

- Write phase that parallel tasks write data to some data service, each record is classified into some logical id,
  say partition id.
- Read phase that parallel tasks read data from the data service, each task read data from a specified partition id.

Suppose the application has failover mechanism so that it's acceptable that when some data is lost the application
will rerun tasks.

Say developers of this application is searching for a suitable data service, and accidentally finds this article.

## Step One: Setup Celeborn Cluster
First, you need an available Celeborn Cluster. Refer to [QuickStart](../../) to set up a simple cluster in a
single node, or [Deploy](../../deploy) to set up a multi-node cluster, standalone or on K8s.

## Step Two: Create LifecycleManager
As described in [Client](../../developers/client), `Client` is separated into `LifecycleManager`, which is singleton
through an application; and `ShuffleClient`, which can have multiple instances.

Step two is to create a `LifecycleManager` instance, using the following API:

```scala
class LifecycleManager(val appUniqueId: String, val conf: CelebornConf)
```

- `appUniqueId` is the application id. Celeborn cluster stores, serves, and cleans up data in the granularity of
  (application id, shuffle id)
- `conf` is an object of `CelebornConf`. The only required configuration is the address of Celeborn `Master`. For
  the thorough description of configs, refer to [Configuration](../../configuration)

The example java code to create an `LifecycleManager` instance is as follows:

```java
CelebornConf celebornConf = new CelebornConf();
celebornConf.set("celeborn.master.endpoints", "<Master IP>:<Master Port>");

LifecycleManager lm = new LifecycleManager("myApp", celebornConf);
```

`LifecycleManager` object automatically starts necessary service after creation, so there is no need to call
other APIs to initialize it. You can get `LifecycleManager`'s address after creating it, which is needed to
create `ShuffleClient`.

```java
String host = lm.getHost();
int = lm.getPort();
```

## Step Three: Create ShuffleClient
With `LifecycleManager`'s host and port, you can create `ShuffleClient` using the following API:

```java
public static ShuffleClient get(
    String appUniqueId,
    String host,
    int port,
    CelebornConf conf,
    UserIdentifier userIdentifier)
```

- `appUniqueId` is the application id, same as above.
- `host` is the host of `LifecycleManager`
- `port` is the port of `LifecycleManager`
- `conf` is an object of `CelebornConf`, safe to pass an empty object
- `userIdentifier` specifies user identity, safe to pass null

You can create a `ShuffleClient` object using the following code:

```java
ShuffleClient shuffleClient =
    ShuffleClient.get(
        "myApp",
        <LifecycleManager Host>,
        <LifecycleManager Port>,
        new CelebornConf(),
        null);
```

This method returns a singleton `ShuffleClientImpl` object, and it's recommended to use this way as `ShuffleClientImpl`
maintains status and reuses resource across all shuffles. To make it work, you have to ensure that the
`LifecycleManager`'s host and port are reachable.

In practice, one `ShuffleClient` instance is created in each Executor process of Spark, or in each TaskManager
process of Flink.

## Step Four: Push Data
You can then push data with `ShuffleClient` with [pushData](../../developers/shuffleclient#api-specification), like
the following:

```java
int bytesWritten =
    shuffleClient.pushData(
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        0,
        length,
        numMappers,
        numPartitions);
```

Each call of `pushData` passes a byte array containing data from the same partition id. In addition to specifying the
shuffleId, mapId, attemptId that the data belongs, `ShuffleClient` should also specify the number of mappers and the
number of partitions for [Lazy Register](../../developers/shuffleclient#lazy-shuffle-register).

After the map task finishes, `ShuffleClient` should call `mapperEnd` to tell `LifecycleManager` that the map task
finishes pushing its data:

```java
public abstract void mapperEnd(
    int shuffleId,
    int mapId,
    int attempted,
    int numMappers)
```

- `shuffleId` shuffle id of the current task
- `mapId` map id of the current task
- `attemptId` attempt id of the current task
- `numMappers` number of map ids in this shuffle

## Step Five: Read Data
After all tasks successfully called `mapperEnd`, you can start reading data from some partition id, using the
[readPartition API](../../developers/shuffleclient#api-specification_1), as the following code:

```java
InputStream inputStream = shuffleClient.readPartition(
  shuffleId,
  partitionId,
  attemptId,
  startMapIndex,
  endMapIndex);

int byte = inputstream.read();
```

For simplicity, to read the whole data from the partition, you can pass 0 and `Integer.MAX_VALUE` to `startMapIndex`
and `endMapIndex`. This method will create an InputStream for the data, and guarantees no data lost and no
duplicate reading, else exception will be thrown.

## Step Six: Clean Up
After the shuffle finishes, you can call `LifecycleManager.unregisterShuffle` to clean up resources related to the
shuffle:

```java
lm.unregisterShuffle(0);
```

It's safe not to call `unregisterShuffle`, because Celeborn `Master` recognizes application finish by heartbeat
timeout, and will self-clean resources in the cluster.