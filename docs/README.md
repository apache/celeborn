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
Quick Start
===
This documentation gives a quick start guide for running Apache Spark with Apache Celeborn(Incubating).

### Download Celeborn
Download the latest Celeborn binary from the [Downloading Page](https://celeborn.apache.org/download/).
Decompress the binary and set `$CELEBORN_HOME`
```shell
tar -C <DST_DIR> -zxvf apache-celeborn-<VERSION>-bin.tgz
export $CELEBORN_HOME=<Decompressed path>
```

## Configure Logging and Storage
#### Configure Logging
```shell
cd $CELEBORN_HOME/conf
cp log4j2.xml.template log4j2.xml
```
#### Configure Storage
Configure the directory to store shuffle data, for example `$CELEBORN_HOME/shuffle`
```shell
cd $CELEBORN_HOME/conf
echo "celeborn.worker.storage.dirs=$CELEBORN_HOME/shuffle" > celeborn-defaults.conf
```

## Start Celeborn Service
#### Start Master
```shell
cd $CELEBORN_HOME
./sbin/start-master.sh
```
You should see `Master`'s ip:port in the log:
```
INFO [main] NettyRpcEnvFactory: Starting RPC Server [MasterSys] on 192.168.2.109:9097
```
#### Start Worker
Use the Master's IP and Port to start Worker:
```shell
cd $CELEBORN_HOME
./sbin/start-worker.sh celeborn://${Master IP}:${Master Port}
```
You should see the following message in Worker's log:
```
INFO [main] MasterClient: connect to master 192.168.2.109:9097.
INFO [main] Worker: Register worker successfully.
```
And also the following message in Master's log:
```
INFO [dispatcher-event-loop-9] Master: Registered worker
Host: 192.168.2.109
RpcPort: 57806
PushPort: 57807
FetchPort: 57809
ReplicatePort: 57808
SlotsUsed: 0
LastHeartbeat: 0
HeartbeatElapsedSeconds: xxx
Disks:
  DiskInfo0: xxx
UserResourceConsumption: empty
WorkerRef: null
```

## Start Spark with Celeborn
#### Copy Celeborn Client to Spark's jars
Celeborn release binary contains clients for Spark 2.x and Spark 3.x, copy the corresponding client jar into Spark's
`jars/` directory:
```shell
cp $CELEBORN_HOME/spark/<Celeborn Client Jar> $SPARK_HOME/jars/
```
#### Start spark-shell
Set `spark.shuffle.manager` to Celeborn's ShuffleManager, and turn off `spark.shuffle.service.enabled`:
```shell
cd $SPARK_HOME

./bin/spark-shell \
--conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager \
--conf spark.shuffle.service.enabled=false
```
Then run the following test case:
```scala
spark.sparkContext
  .parallelize(1 to 10, 10)
  .flatMap(_ => (1 to 100).iterator.map(num => num))
  .repartition(10)
  .count
```
During the Spark Job, you should see the following message in Celeborn Master's log:
```
Master: Offer slots successfully for 10 reducers of local-1690000152711-0 on 1 workers.
```
And the following message in Celeborn Worker's log:
```
INFO [dispatcher-event-loop-9] Controller: Reserved 10 primary location and 0 replica location for local-1690000152711-0
INFO [dispatcher-event-loop-10] Controller: Start commitFiles for local-1690000152711-0
INFO [async-reply] Controller: CommitFiles for local-1690000152711-0 success with 10 committed primary partitions, 0 empty primary partitions, 0 failed primary partitions, 0 committed replica partitions, 0 empty replica partitions, 0 failed replica partitions.
```
