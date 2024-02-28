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

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
Quick Start
===
This documentation gives a quick start guide for running Apache Spark/Flink with Apache Celeborn™(Incubating).

### Download Celeborn
Download the latest Celeborn binary from the [Downloading Page](https://celeborn.apache.org/download/).
Decompress the binary and set `$CELEBORN_HOME`
```shell
tar -C <DST_DIR> -zxvf apache-celeborn-<VERSION>-bin.tgz
export CELEBORN_HOME=<Decompressed path>
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
```log
INFO [main] NettyRpcEnvFactory: Starting RPC Server [MasterSys] on 192.168.2.109:9097 with advisor endpoint 192.168.2.109:9097
```
#### Start Worker
Use the Master's IP and Port to start Worker:
```shell
cd $CELEBORN_HOME
./sbin/start-worker.sh celeborn://<Master IP>:<Master Port>
```
You should see the following message in Worker's log:
```log
INFO [main] MasterClient: connect to master 192.168.2.109:9097.
INFO [main] Worker: Register worker successfully.
INFO [main] Worker: Worker started.
```
And also the following message in Master's log:
```log
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
```log
Master: Offer slots successfully for 10 reducers of local-1690000152711-0 on 1 workers.
```
And the following message in Celeborn Worker's log:
```log
INFO [dispatcher-event-loop-9] Controller: Reserved 10 primary location and 0 replica location for local-1690000152711-0
INFO [dispatcher-event-loop-8] Controller: Start commitFiles for local-1690000152711-0
INFO [async-reply] Controller: CommitFiles for local-1690000152711-0 success with 10 committed primary partitions, 0 empty primary partitions, 0 failed primary partitions, 0 committed replica partitions, 0 empty replica partitions, 0 failed replica partitions.
```

## Start Flink with Celeborn
#### Copy Celeborn Client to Flink's lib
Celeborn release binary contains clients for Flink 1.14.x, Flink 1.15.x, Flink 1.17.x and Flink 1.18.x, copy the corresponding client jar into Flink's
`lib/` directory:
```shell
cp $CELEBORN_HOME/flink/<Celeborn Client Jar> $FLINK_HOME/lib/
```
#### Add Celeborn configuration to Flink's conf
Set `shuffle-service-factory.class` to Celeborn's ShuffleServiceFactory in Flink configuration file:
```shell
cd $FLINK_HOME
vi conf/flink-conf.yaml
```
```properties
shuffle-service-factory.class: org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory
execution.batch-shuffle-mode: ALL_EXCHANGES_BLOCKING
```
**Note**: The config option `execution.batch-shuffle-mode` should configure as `ALL_EXCHANGES_BLOCKING`.

Then deploy the example word count job to the running cluster:
```shell
cd $FLINK_HOME

./bin/flink run examples/streaming/WordCount.jar --execution-mode BATCH
```
During the Flink Job, you should see the following message in Celeborn Master's log:
```log
Master: Offer slots successfully for 1 reducers of local-1690000152711-0 on 1 workers.
```
And the following message in Celeborn Worker's log:
```log
INFO [dispatcher-event-loop-4] Controller: Reserved 1 primary location and 0 replica location for local-1690000152711-0
INFO [dispatcher-event-loop-3] Controller: Start commitFiles for local-1690000152711-0
INFO [async-reply] Controller: CommitFiles for local-1690000152711-0 success with 1 committed primary partitions, 0 empty primary partitions, 0 failed primary partitions, 0 committed replica partitions, 0 empty replica partitions, 0 failed replica partitions.
```

## Start MapReduce With Celeborn
### Add Celeborn client jar to MapReduce's classpath
1.Add $CELEBORN_HOME/mr/*.jar to `mapreduce.application.classpath` and `yarn.application.classpath`.
2.Restart your yarn cluster.
### Add Celeborn configurations to MapReduce's conf
Modify `${HADOOP_CONF_DIR}/yarn-site.xml`
```xml
<configuration>
    <property>
        <name>yarn.app.mapreduce.am.job.recovery.enable</name>
        <value>false</value>
    </property>

    <property>
        <name>yarn.app.mapreduce.am.command-opts</name>
        <!-- Append 'org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn' to this setting  -->
        <value>org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn</value>
    </property>
</configuration>
```
Modify `${HADOOP_CONF_DIR}/mapred-site.xml`
```xml
<configuration>
    <property>
        <name>mapreduce.job.reduce.slowstart.completedmaps</name>
        <value>1</value>
    </property>
    <property>
        <name>mapreduce.celeborn.master.endpoints</name>
        <!-- Replace placeholder to the real master address       -->
        <value>placeholder</value>
    </property>
    <property>
        <name>mapreduce.job.map.output.collector.class</name>
        <value>org.apache.hadoop.mapred.CelebornMapOutputCollector</value>
    </property>
    <property>
        <name>mapreduce.job.reduce.shuffle.consumer.plugin.class</name>
        <value>org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer</value>
    </property>
</configuration>
```
Then you can run a word count to check whether your configs are correct.
```shell
cd $HADOOP_HOME
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar wordcount /sometext /someoutput
```
During the MapReduce Job, you should see the following message in Celeborn Master's log:
```log
Master: Offer slots successfully for 1 reducers of application_1694674023293_0003-0 on 1 workers.
```
And the following message in Celeborn Worker's log:
```log
INFO [dispatcher-event-loop-4] Controller: Reserved 1 primary location and 0 replica location for application_1694674023293_0003-0
INFO [dispatcher-event-loop-3] Controller: Start commitFiles for application_1694674023293_0003-0
INFO [async-reply] Controller: CommitFiles for application_1694674023293_0003-0 success with 1 committed primary partitions, 0 empty primary partitions, 0 failed primary partitions, 0 committed replica partitions, 0 empty replica partitions, 0 failed replica partitions.
```