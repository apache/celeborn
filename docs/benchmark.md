# benchmark
Terasort : [hibench](https://github.com/Intel-bigdata/HiBench)
TPC-DS : [hive-testbench](https://github.com/hortonworks/hive-testbench)

## environment  
Aliyun E-mapreduce EMR-5.5.0  
master Group: 1x ecs.g6.2xlarge, 8 vCPU, 32 GiB vMem, Band Width: 2.560 Gbps  
core Group: 10x ecs.g6.8xlarge, 32 vCPU, 128 GiB vMem, Band Width: 10.240 Gbps, 4x 2TB Disk  
We deployed RSS workers and ESS on every server in the Core Group. And We manually replaced Spark to enable magnet.  

## configurations
hiBench.conf
```properties
hibench.scale.profile                custom
hibench.default.map.parallelism         6000
hibench.default.shuffle.parallelism     6000
```
workloads/micro/terasort.conf
```properties
hibench.terasort.custom.datasize 54975581385
```
rss-env.sh
```properties
RSS_MASTER_MEMORY=2g
RSS_WORKER_MEMORY=1g
RSS_WORKER_OFFHEAP_MEMORY=7g
```
rss-defaults.conf
```
rss.worker.flush.queue.capacity 2048
```
### RSS without replication
spark-defaults.conf  
```
spark.executor.memory  26g
spark.executor.memoryOverhead 2g
spark.driver.memory    8g
spark.driver.cores 16
spark.dynamicAllocation.enabled false
spark.eventLog.enabled           true

spark.shuffle.manager org.apache.spark.shuffle.rss.RssShuffleManager
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.rss.master.address rss-master-host:9097
spark.shuffle.service.enabled false
spark.rss.shuffle.writer.mode hash
spark.rss.push.data.replicate false
spark.sql.adaptive.enabled false
spark.sql.adaptive.localShuffleReader.enabled false
spark.sql.adaptive.skewJoin.enabled false 
spark.hadoop.mapreduce.input.fileinputformat.split.maxsize 1073741824
spark.hadoop.mapreduce.input.fileinputformat.split.minsize 1073741824
```

### RSS with replication
spark-defaults.conf
```
spark.executor.memory  26g
spark.executor.memoryOverhead 2g
spark.driver.memory    8g
spark.driver.cores 16
spark.dynamicAllocation.enabled false
spark.eventLog.enabled           true

spark.shuffle.manager org.apache.spark.shuffle.rss.RssShuffleManager
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.rss.master.address rss-master-host:9097
spark.shuffle.service.enabled false
spark.rss.shuffle.writer.mode hash
spark.rss.push.data.replicate true
spark.sql.adaptive.enabled false
spark.sql.adaptive.localShuffleReader.enabled false
spark.sql.adaptive.skewJoin.enabled false 
spark.hadoop.mapreduce.input.fileinputformat.split.maxsize 1073741824
spark.hadoop.mapreduce.input.fileinputformat.split.minsize 1073741824
```

### magnet without push
spark-defaults.conf  
```
spark.executor.memory  26g
spark.executor.memoryOverhead 2g
spark.driver.memory    8g
spark.driver.cores 16
spark.dynamicAllocation.enabled false
spark.eventLog.enabled           true
spark.hadoop.mapreduce.input.fileinputformat.split.maxsize 1073741824
spark.hadoop.mapreduce.input.fileinputformat.split.minsize 1073741824

spark.shuffle.service.enabled true
spark.executor.instances 300
```
### magnet with push
spark-defaults.conf  
```
spark.executor.memory  26g
spark.executor.memoryOverhead 2g
spark.driver.memory    8g
spark.driver.cores 16
spark.dynamicAllocation.enabled false
spark.eventLog.enabled           true
spark.hadoop.mapreduce.input.fileinputformat.split.maxsize 1073741824
spark.hadoop.mapreduce.input.fileinputformat.split.minsize 1073741824

spark.shuffle.service.enabled true
spark.executor.instances 300
spark.shuffle.push.enabled true
spark.shuffle.push.server.mergedShuffleFileManagerImpl org.apache.spark.network.shuffle.RemoteBlockPushResolver
```
yarn-site.xml

```xml
<property>
    <name>spark.shuffle.push.server.mergedShuffleFileManagerImpl</name>
    <value>org.apache.spark.network.shuffle.RemoteBlockPushResolver</value>
</property>
```