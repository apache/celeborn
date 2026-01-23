# HDFS Part

## When use both HDFS and local disks
When Celeborn workers have both local disks and HDFS, OFFHEAP memory should 
be increased, we can start with following configurations:

CELEBORN_WORKER_MEMORY=2g

CELEBORN_WORKER_OFFHEAP_MEMORY=20g

We use TPCDS benchmark as an example, the following configurations are 
recommended for Celeborn workers:
```
celeborn.worker.flusher.hdfs.buffer.size 4m
celeborn.worker.sortPartition.threads 96
celeborn.worker.commitFiles.timeout 600s
celeborn.worker.commitFiles.threads 256
celeborn.worker.flusher.hdfs.threads 12
celeborn.rpc.askTimeout 800s
celeborn.worker.replicate.fastFail.duration 400s
celeborn.worker.writer.close.timeout 800s
celeborn.storage.activeTypes SSD,HDFS
celeborn.storage.hdfs.dir hdfs://MTPrime-MWHE02-1-Extra-0/projects-ssd/Celeborn
celeborn.worker.hdfs.replicate.enabled true
```   

TPCDS benchmark Spark app, we run query on q24a,q24b with 10TB data on HDFS
```
 {
      "sparkVersion": "3.3",
      "file": "hdfs://namenode0-vip.MTPrime-PROD-mwhe02.mwhe02.ap.gbl/user/jutia/tpcds-benchmark_2.12-1.1-SNAPSHOT.jar",
      "className": "com.microsoft.TpcdsBenchmark",
      "queue": "default",
      "driverMemory": "30g",
      "driverCores": 8,
      "numExecutors": 500,
      "executorMemory": "18g",
      "executorCores": 8,
      "files" : ["hdfs://namenode0-vip.MTPrime-PROD-MWHE02.MWHE02.ap.gbl/user/jutia/celeborn/MWHE02CelebornSource/conf/celeborn-defaults-before-expand.conf"],
      "jars": ["hdfs://namenode0-vip.mtprime-prod-MWHE02.MWHE02.ap.gbl/user/jutia/celeborn-client-spark-3-shaded_2.12-0.5.4.jar"],
      "args": ["-d","hdfs://namenode0-vip.mtprime-prod-mwhe02.mwhe02.ap.gbl/projects/MT/spark/tpcds10-copy2","-r","hdfs://namenode0-vip.MTPrime-PROD-MWHE02.MWHE02.ap.gbl/user/jutia/tpcds-benchmark-result/mwhe02/CelebornPureHDFS-10t/iter1", "-e", "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q20,q21,q22,q23a,q23b,q25,q26,q27,q28,q29,q30,q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q70,q71,q72,q73,q74,q75,q76,q77,q78,q79,q80,q81,q82,q83,q84,q85,q86,q87,q88,q89,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99,ss_max"],
   
      "conf": {
          "spark.app.name": "ShuffleService-TPC-DC-test-10t-pure-hdfs-24a-b",
          "spark.yarn.executor.nodeLabelExpression":"*persistent*",
          "spark.dynamicAllocation.enabled":"false",
          "spark.executor.memoryOverhead":"6G",
          "spark.sql.broadcastTimeout":"10000",
          "spark.sql.shuffle.partitions":"4000",
          "spark.task.cpus":"2",
          "spark.sql.statistics.histogram.enabled":"false",
          "spark.sql.cbo.enabled":"true",
          "spark.sql.cbo.joinReorder.enabled":"true",
          "spark.sql.autoBroadcastJoinThreshold":"1g",
          "spark.speculation":"true",
          "spark.driver.maxResultSize":"17g",
          "spark.sql.adaptive.enabled":"true",
          "spark.sql.adaptive.forceApply":"true",
          "spark.yarn.maxAppAttempts":1,
          "spark.eventLog.logStageExecutorMetrics":"true",
          "spark.sql.adaptive.logLevel":"INFO",
          "spark.sql.adaptive.advisoryPartitionSizeInBytes":"128M",
          "spark.executor.extraJavaOptions":"-Xms18G -XX:+UseCompressedOops -XX:ParallelGCThreads=8 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxGCPauseMillis=60000 -XX:CompressedClassSpaceSize=100m -XX:GCTimeRatio=19 -XX:+PrintTenuringDistribution -XX:NativeMemoryTracking=summary",
         "spark.shuffle.manager" : "org.apache.spark.shuffle.celeborn.SparkShuffleManager",
          "spark.serializer" : "org.apache.spark.serializer.KryoSerializer",
          "spark.kryoserializer.buffer.max": "1024m",
          "spark.celeborn.master.endpoints" : "MW02EAP00015E61:29097,MW02EAP00016262:29097,MW02EAP000161EB:29097,MW02EAP0001624B:29097,MW02EAP00015E4A:29097,MW02EAP00015DEA:29097,MW02EAP00015D9B:29097",
          "spark.celeborn.storage.activeTypes": "HDFS",
          "spark.celeborn.client.push.timeout" : "800s",
          "spark.celeborn.data.io.connectionTimeout" : "800s",
          "spark.celeborn.worker.commitFiles.timeout" : "480s",
          "spark.celeborn.rpc.askTimeout" : "800s",
          "spark.celeborn.client.rpc.commitFiles.askTimeout" : "800s",
        "spark.celeborn.storage.hdfs.dir" : "hdfs://MTPrime-MWHE02-1-Extra-0/projects-ssd/Celeborn"
      }
  }
```