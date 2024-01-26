# Metrics

We provide various metrics about memory, disk, and important procedures. These metrics could help identify performance
issue or monitor Celeborn cluster.

## Prerequisites

1. Enable Celeborn metrics. Set configuration `celeborn.metrics.enabled` to true (true by default).

2. Configure Celeborn metrics properties.
```shell
cd $CELEBORN_HOME/conf
cp metrics.properties.template metrics.properties
```
The default values of the Celeborn metrics configuration are as follows:
```
*.sink.prometheusServlet.class=org.apache.celeborn.common.metrics.sink.PrometheusServlet
```

3. Install Prometheus (https://prometheus.io/). We provide an example for Prometheus config file:

```yaml
# Prometheus example config
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "Celeborn"
    metrics_path: /metrics/prometheus
    scrape_interval: 15s
    static_configs:
      - targets: [ "master-ip:9098","worker1-ip:9096","worker2-ip:9096","worker3-ip:9096","worker4-ip:9096" ]
```

4. Install Grafana server (https://grafana.com/grafana/download).

5. Import Celeborn dashboard into Grafana.

You can find the Celeborn dashboard templates under the `assets/grafana` directory.
`celeborn-dashboard.json` displays Celeborn internal metrics and `celeborn-jvm-dashboard.json` displays Celeborn JVM related metrics.

### Optional

We recommend you to install node exporter (https://github.com/prometheus/node_exporter)
on every host, and configure Prometheus to scrape information about the host.
Grafana will need a dashboard (dashboard id:8919) to display host details.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "Celeborn"
    metrics_path: /metrics/prometheus
    scrape_interval: 15s
    static_configs:
      - targets: [ "master-ip:9098","worker1-ip:9096","worker2-ip:9096","worker3-ip:9096","worker4-ip:9096" ]
  - job_name: "node"
    static_configs:
      - targets: [ "master-ip:9100","worker1-ip:9100","worker2-ip:9100","worker3-ip:9100","worker4-ip:9100" ]
```

### Import Dashboard Steps
Here is an example of Grafana dashboard importing.

![g1](assets/img/g1.png)
![g2](assets/img/g2.png)
![g3](assets/img/g3.png)
<img alt="g4" height="90%" src="assets/img/g4.png" width="90%"/>
<img alt="g6" height="90%" src="assets/img/g6.png" width="90%"/>
<img alt="g5" height="90%" src="assets/img/g5.png" width="90%"/>

## Details

|               MetricName               |       Scope       |                                                      Description                                                       |
|:--------------------------------------:|:-----------------:|:----------------------------------------------------------------------------------------------------------------------:|
|         RegisteredShuffleCount         | master and worker |                                      The value means count of registered shuffle.                                      |
|        RunningApplicationCount         | master and worker |                                     The value means count of running applications.                                     |
|           ActiveShuffleSize            | master and worker |      The value means the active shuffle size for workers or a worker including master replica and slave replica.       |
|         ActiveShuffleFileCount         | master and worker |   The value means the active shuffle file count for workers or a worker including master replica and slave replica.    |
|             diskFileCount              | master and worker |                                   The count of disk files consumption by each user.                                    |
|            diskBytesWritten            | master and worker |                                   The amount of disk files consumption by each user.                                   |
|             hdfsFileCount              | master and worker |                                   The count of hdfs files consumption by each user.                                    |
|            hdfsBytesWritten            | master and worker |                                   The amount of hdfs files consumption by each user.                                   |
|              WorkerCount               |      master       |                                              The count of active workers.                                              |
|              LostWorkers               |      master       |                                           The count of workers in lost list.                                           |
|          ExcludedWorkerCount           |      master       |                                         The count of workers in excluded list.                                         |
|             IsActiveMaster             |      master       |                                         Whether the current master is active.                                          |
|             PartitionSize              |      master       |              The estimated partition size of last 20 flush window whose length is 15 seconds by defaults.              |
|             OfferSlotsTime             |      master       |                                                The time of offer slots.                                                |
|             OpenStreamTime             |      worker       |                OpenStream means read a shuffle file and send client about chunks size and stream index.                |
|             FetchChunkTime             |      worker       |                         FetchChunk means read a chunk from a shuffle file and send to client.                          |
|            ChunkStreamCount            |      worker       |                        The stream count for reduce partition reading streams in current worker.                        |
|         OpenStreamSuccessCount         |      worker       |                                 The count of opening stream succeed in current worker.                                 |
|          OpenStreamFailCount           |      worker       |                                 The count of opening stream failed in current worker.                                  |
|         FetchChunkSuccessCount         |      worker       |                                 The count of fetching chunk succeed in current worker.                                 |
|          FetchChunkFailCount           |      worker       |                                 The count of fetching chunk failed in current worker.                                  |
|          PrimaryPushDataTime           |      worker       |                          PrimaryPushData means handle PushData of primary partition location.                          |
|          ReplicaPushDataTime           |      worker       |                          ReplicaPushData means handle PushData of replica partition location.                          |
|        WriteDataHardSplitCount         |      worker       |               The count of writing PushData or PushMergedData to HARD_SPLIT partition in current worker.               |
|         WriteDataSuccessCount          |      worker       |                       The count of writing PushData or PushMergedData succeed in current worker.                       |
|           WriteDataFailCount           |      worker       |                       The count of writing PushData or PushMergedData failed in current worker.                        |
|         ReplicateDataFailCount         |      worker       |                     The count of replicating PushData or PushMergedData failed in current worker.                      |
|      ReplicateDataWriteFailCount       |      worker       |           The count of replicating PushData or PushMergedData failed caused by write failure in peer worker.           |
| ReplicateDataCreateConnectionFailCount |      worker       |    The count of replicating PushData or PushMergedData failed caused by creating connection failed in peer worker.     |
| ReplicateDataConnectionExceptionCount  |      worker       |       The count of replicating PushData or PushMergedData failed caused by connection exception in peer worker.        |
|       ReplicateDataTimeoutCount        |      worker       |           The count of replicating PushData or PushMergedData failed caused by push timeout in peer worker.            |
|       PushDataHandshakeFailCount       |      worker       |                                The count of PushDataHandshake failed in current worker.                                |
|          RegionStartFailCount          |      worker       |                                   The count of RegionStart failed in current worker.                                   |
|         RegionFinishFailCount          |      worker       |                                  The count of RegionFinish failed in current worker.                                   |
|      PrimaryPushDataHandshakeTime      |      worker       |                     PrimaryPushDataHandshake means handle PushData of primary partition location.                      |
|      ReplicaPushDataHandshakeTime      |      worker       |                     ReplicaPushDataHandshake means handle PushData of replica partition location.                      |
|         PrimaryRegionStartTime         |      worker       |                       PrimaryRegionStart means handle RegionStart of primary partition location.                       |
|         ReplicaRegionStartTime         |      worker       |                       ReplicaRegionStart means handle RegionStart of replica partition location.                       |
|        PrimaryRegionFinishTime         |      worker       |                      PrimaryRegionFinish means handle RegionFinish of primary partition location.                      |
|        ReplicaRegionFinishTime         |      worker       |                      ReplicaRegionFinish means handle RegionFinish of replica partition location.                      |
|           PausePushDataTime            |      worker       |                                  PausePushData means stop receiving data from client.                                  |
|     PausePushDataAndReplicateTime      |      worker       |                   PausePushDataAndReplicate means stop receiving data from client and other workers.                   |
|             PausePushData              |      worker       |                          The count of stopping receiving data from client in current worker.                           |
|       PausePushDataAndReplicate        |      worker       |                 The count of stopping receiving data from client and other workers in current worker.                  |
|             TakeBufferTime             |      worker       |                                 TakeBuffer means get a disk buffer from disk flusher.                                  |
|             FlushDataTime              |      worker       |                                      FlushData means flush a disk buffer to disk.                                      |
|            CommitFilesTime             |      worker       |                              CommitFiles means flush and close a shuffle partition file.                               |
|             SlotsAllocated             |      worker       |                                              Slots allocated in last hour                                              |
|            ReserveSlotsTime            |      worker       |                        ReserveSlots means acquire a disk buffer and record partition location.                         |
|         ActiveConnectionCount          |      worker       |                                  The value means count of active network connection.                                   |
|              NettyMemory               |      worker       |                            The value measures all kinds of transport memory used by netty.                             |
|                SortTime                |      worker       |                               SortTime measures the time used by sorting a shuffle file.                               |
|               SortMemory               |      worker       |                           SortMemory means total reserved memory for sorting shuffle files .                           |
|              SortingFiles              |      worker       |                                  This value means the count of sorting shuffle files.                                  |
|              SortedFiles               |      worker       |                                  This value means the count of sorted shuffle files.                                   |
|             SortedFileSize             |      worker       |                           This value means the count of sorted shuffle files 's total size.                            |
|               DiskBuffer               |      worker       |     Disk buffers are part of netty used memory, means data need to write to disk but haven't been written to disk.     |
|         BufferStreamReadBuffer         |      worker       |                               This value means memory used by credit stream read buffer.                               |
|   ReadBufferDispatcherRequestsLength   |      worker       |                          This value means the queue size of read buffer allocation requests.                           |
|        ReadBufferAllocatedCount        |      worker       |                                    This value means count of allocated read buffer.                                    |
|           CreditStreamCount            |      worker       |                          This value means count of stream for map partition reading streams.                           |
|        ActiveMapPartitionCount         |      worker       |                            This value means count of active map partition reading streams.                             |
|           DeviceOSFreeBytes            |      worker       |                             This value means actual usable space of OS for device monitor.                             |
|           DeviceOSTotalBytes           |      worker       |                             This value means total usable space of OS for device monitor.                              |
|        DeviceCelebornFreeBytes         |      worker       |                          This value means actual usable space of Celeborn for device monitor.                          |
|        DeviceCelebornTotalBytes        |      worker       |                        This value means configured usable space of Celeborn for device monitor.                        |
|         PotentialConsumeSpeed          |      worker       |                        This value means speed of potential consumption for congestion control.                         |
|            UserProduceSpeed            |      worker       |                           This value means speed of user production for congestion control.                            |
|           WorkerConsumeSpeed           |      worker       |                          This value means speed of worker consumption for congestion control.                          |
|              jvm_gc_count              |        JVM        |                                        The GC count of each garbage collector.                                         |
|              jvm_gc_time               |        JVM        |                                      The GC cost time of each garbage collector.                                       |
|          jvm_memory_heap_init          |        JVM        |                                            The amount of heap init memory.                                             |
|          jvm_memory_heap_max           |        JVM        |                                             The amount of heap max memory.                                             |
|          jvm_memory_heap_used          |        JVM        |                                            The amount of heap used memory.                                             |
|       jvm_memory_heap_committed        |        JVM        |                                          The amount of heap committed memory.                                          |
|         jvm_memory_heap_usage          |        JVM        |                                          The percentage of heap memory usage.                                          |
|        jvm_memory_non_heap_init        |        JVM        |                                          The amount of non-heap init memory.                                           |
|        jvm_memory_non_heap_max         |        JVM        |                                           The amount of non-heap max memory.                                           |
|        jvm_memory_non_heap_used        |        JVM        |                                          The amount of non-heap uesd memory.                                           |
|     jvm_memory_non_heap_committed      |        JVM        |                                        The amount of non-heap committed memory.                                        |
|       jvm_memory_non_heap_usage        |        JVM        |                                        The percentage of non-heap memory usage.                                        |
|         jvm_memory_pools_init          |        JVM        |                                     The amount of each memory pool's init memory.                                      |
|          jvm_memory_pools_max          |        JVM        |                                      The amount of each memory pool's max memory.                                      |
|         jvm_memory_pools_used          |        JVM        |                                     The amount of each memory pool's used memory.                                      |
|       jvm_memory_pools_committed       |        JVM        |                                   The amount of each memory pool's committed memory.                                   |
|     jvm_memory_pools_used_after_gc     |        JVM        |                                 The amount of each memory pool's used memory after GC.                                 |
|         jvm_memory_pools_usage         |        JVM        |                                   The percentage of each memory pool's memory usage.                                   |
|         jvm_memory_total_init          |        JVM        |                                            The amount of total init memory.                                            |
|          jvm_memory_total_max          |        JVM        |                                            The amount of total max memory.                                             |
|         jvm_memory_total_used          |        JVM        |                                            The amount of total used memory.                                            |
|       jvm_memory_total_committed       |        JVM        |                                   The amount of each memory pool's committed memory.                                   |
|          jvm_direct_capacity           |        JVM        |                             An estimate of the total capacity of the buffers in this pool                              |
|            jvm_direct_count            |        JVM        |                                    An estimate of the number of buffers in the pool                                    |
|            jvm_direct_used             |        JVM        |                            An estimate of the memory that JVM is using for this buffer pool                            |
|          jvm_mapped_capacity           |        JVM        |                             An estimate of the total capacity of the buffers in this pool                              |
|            jvm_mapped_count            |        JVM        |                                    An estimate of the number of buffers in the pool                                    |
|            jvm_mapped_used             |        JVM        |                            An estimate of the memory that JVM is using for this buffer pool                            |
|            jvm_thread_count            |        JVM        |                                             The current number of threads.                                             |
|        jvm_thread_daemon_count         |        JVM        |                                         The current number of daemon threads.                                          |
|        jvm_thread_blocked_count        |        JVM        |                                  The current number of threads having blocked state.                                   |
|       jvm_thread_deadlock_count        |        JVM        |                                  The current number of threads having deadlock state.                                  |
|          jvm_thread_new_count          |        JVM        |                                    The current number of threads having new state.                                     |
|       jvm_thread_runnable_count        |        JVM        |                                  The current number of threads having runnable state.                                  |
|      jvm_thread_terminated_count       |        JVM        |                                 The current number of threads having terminated state.                                 |
|     jvm_thread_timed_waiting_count     |        JVM        |                               The current number of threads having timed_waiting state.                                |
|        jvm_thread_waiting_count        |        JVM        |                                  The current number of threads having waiting state.                                   |
|         jvm_classloader_loaded         |        JVM        |                             The total number of classes loaded since the start of the JVM.                             |
|        jvm_classloader_unloaded        |        JVM        |                            The total number of classes unloaded since the start of the JVM.                            |
|               JVMCPUTime               |      system       |                                                The JVM costs cpu time.                                                 |
|          AvailableProcessors           |      system       |                                       The amount of system available processors.                                       |
|          LastMinuteSystemLoad          |      system       |                                            The last minute load of system.                                             |
|          active_thread_count           |        JVM        |                          The approximate number of threads that are actively executing tasks.                          |
|           pending_task_count           |        JVM        |                                     The pending task not executed in block queue.                                      |
|               pool_size                |        JVM        |                                       The current number of threads in the pool.                                       |
|             core_pool_size             |        JVM        |                                              The core number of threads.                                               |
|           maximum_pool_size            |        JVM        |                                         The maximum allowed number of threads.                                         |
|           largest_pool_size            |        JVM        |                     The largest number of threads that have ever simultaneously been in the pool.                      |
|             is_terminating             |        JVM        | If this executor is in the process of terminating after shutdown() or shutdownNow() but has not completely terminated. |
|             is_terminated              |        JVM        |   If this executor is in the process of terminated after shutdown() or shutdownNow() and has completely terminated.    |
|              is_shutdown               |        JVM        |                                             If this executor is shutdown.                                              |
|              thread_count              |        JVM        |                                       The thread count of current thread group.                                        |
|       thread_is_terminated_count       |        JVM        |                                  The terminated thread count of current thread group.                                  |
|        thread_is_shutdown_count        |        JVM        |                                   The shutdown thread count of current thread group.                                   |

## Implementation

Celeborn master metrics : `org/apache/celeborn/service/deploy/master/MasterSource.scala`.

Celeborn worker metrics : `org/apache/celeborn/service/deploy/worker/WorkerSource.scala`.

Other common metrics are implemented in `org.apache.celeborn.common.metrics.source` package.

## Dashboard Snapshots

The dashboard [Celeborn-dashboard](assets/grafana/celeborn-dashboard.json) was generated by Grafana of version 10.0.3.

Here are some snapshots:

![d1](assets/img/dashboard1.png)
![d2](assets/img/dashboard_full.webp)
