---
hide:
  - navigation

license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
      https://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Configuration Guide
===
This documentation contains Celeborn configuration details and a tuning guide.

## Important Configurations

### Environment Variables

- `CELEBORN_WORKER_MEMORY=4g`
- `CELEBORN_WORKER_OFFHEAP_MEMORY=24g`

Celeborn workers tend to improve performance by using off-heap buffers.
Off-heap memory requirement can be estimated as below:

```
numDirs = `celeborn.worker.storage.dirs`             # the amount of directory will be used by Celeborn storage
bufferSize = `celeborn.worker.flusher.buffer.size`   # the amount of memory will be used by a single flush buffer 
off-heap-memory = bufferSize * estimatedTasks * 2 + network memory
```

For example, if a Celeborn worker has 10 storage directories or disks and the buffer size is set to 256 KiB.
The necessary off-heap memory is 10 GiB.

Network memory will be consumed when netty reads from a TCP channel, there will need some extra
memory. Empirically, Celeborn worker off-heap memory should be set to `(numDirs  * bufferSize * 1.2)`.

## All Configurations

### Master

{!
include-markdown "./master.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

Apart from these, the following properties are also available for enable master HA:
### Master HA

{!
include-markdown "./ha.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Worker

{!
include-markdown "./worker.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}


### Client

{!
include-markdown "./client.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}


### Quota

{!
include-markdown "./quota.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Network

{!
include-markdown "./network.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}


### Columnar Shuffle

{!
include-markdown "./columnar-shuffle.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Metrics

Below metrics configuration both work for master and worker.

{!
include-markdown "./metrics.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

#### metrics.properties

```properties
*.sink.csv.class=org.apache.celeborn.common.metrics.sink.CsvSink
*.sink.prometheusServlet.class=org.apache.celeborn.common.metrics.sink.PrometheusServlet
```

### Environment Variables

Recommend configuring in `conf/celeborn-env.sh`.

| Key                              | Default                                         | Description                                            |
|----------------------------------|-------------------------------------------------|--------------------------------------------------------|
| `CELEBORN_HOME`                  | ``$(cd "`dirname "$0"`"/..; pwd)``              |                                                        |
| `CELEBORN_CONF_DIR`              | `${CELEBORN_CONF_DIR:-"${CELEBORN_HOME}/conf"}` |                                                        |
| `CELEBORN_MASTER_MEMORY`         | 1 GB                                            |                                                        |
| `CELEBORN_WORKER_MEMORY`         | 1 GB                                            |                                                        |
| `CELEBORN_WORKER_OFFHEAP_MEMORY` | 1 GB                                            |                                                        |
| `CELEBORN_MASTER_JAVA_OPTS`      |                                                 |                                                        |
| `CELEBORN_WORKER_JAVA_OPTS`      |                                                 |                                                        |
| `CELEBORN_PID_DIR`               | `${CELEBORN_HOME}/pids`                         |                                                        |
| `CELEBORN_LOG_DIR`               | `${CELEBORN_HOME}/logs`                         |                                                        |
| `CELEBORN_SSH_OPTS`              | `-o StrictHostKeyChecking=no`                   |                                                        |
| `CELEBORN_SLEEP`                 |                                                 | Waiting time for `start-all` and `stop-all` operations |
| `CELEBORN_PREFER_JEMALLOC`       |                                                 | set `true` to enable jemalloc memory allocator         |
| `CELEBORN_JEMALLOC_PATH`         |                                                 | jemalloc library path                                  |

## Tuning

Assume we have a cluster described as below:
5 Celeborn Workers with 20 GB off-heap memory and 10 disks.
As we need to reserve 20% off-heap memory for netty,
so we could assume 16 GB off-heap memory can be used for flush buffers.

If `spark.celeborn.client.push.buffer.max.size` is 64 KB, we can have in-flight requests up to 1310720.
If you have 8192 mapper tasks, you could set `spark.celeborn.client.push.maxReqsInFlight=160` to gain performance improvements.

If `celeborn.worker.flusher.buffer.size` is 256 KB, we can have total slots up to 327680 slots.

## Rack Awareness

Celeborn can be rack-aware by setting `celeborn.client.reserveSlots.rackware.enabled` to `true` on client side.
Shuffle partition block replica placement will use rack awareness for fault tolerance by placing one shuffle partition replica
on a different rack. This provides data availability in the event of a network switch failure or partition within the cluster.

Celeborn master daemons obtain the rack id of the cluster workers by invoking either an external script or Java class as specified by configuration files.
Using either the Java class or external script for topology, output must adhere to the java `org.apache.hadoop.net.DNSToSwitchMapping` interface.
The interface expects a one-to-one correspondence to be maintained and the topology information in the format of `/myrack/myhost`,
where `/` is the topology delimiter, `myrack` is the rack identifier, and `myhost` is the individual host.
Assuming a single `/24` subnet per rack, one could use the format of `/192.168.100.0/192.168.100.5` as a unique rack-host topology mapping.

To use the Java class for topology mapping, the class name is specified by the `celeborn.hadoop.net.topology.node.switch.mapping.impl` parameter in the master configuration file.
An example, `NetworkTopology.java`, is included with the Celeborn distribution and can be customized by the Celeborn administrator. 
Using a Java class instead of an external script has a performance benefit in that Celeborn doesn't need to fork an external process when a new worker node registers itself.

If implementing an external script, it will be specified with the `celeborn.hadoop.net.topology.script.file.name` parameter in the master side configuration files. 
Unlike the Java class, the external topology script is not included with the Celeborn distribution and is provided by the administrator. 
Celeborn will send multiple IP addresses to ARGV when forking the topology script. The number of IP addresses sent to the topology script 
is controlled with `celeborn.hadoop.net.topology.script.number.args` and defaults to 100.
If `celeborn.hadoop.net.topology.script.number.args` was changed to 1, a topology script would get forked for each IP submitted by workers.

If `celeborn.hadoop.net.topology.script.file.name` or `celeborn.hadoop.net.topology.node.switch.mapping.impl` is not set, the rack id `/default-rack` is returned for any passed IP address.
While this behavior appears desirable, it can cause issues with shuffle partition block replication as default behavior
is to write one replicated block off rack and is unable to do so as there is only a single rack named `/default-rack`.

Example can refer to [Hadoop Rack Awareness](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/RackAwareness.html) since Celeborn use hadoop's code about rack-aware.


## Worker Recover Status After Restart

`ShuffleClient` records the shuffle partition location's host, service port, and filename,
to support workers recovering reading existing shuffle data after worker restart,
during worker shutdown, workers should store the meta about reading shuffle partition files in LevelDB,
and restore the meta after restarting workers, also workers should keep a stable service port to support
`ShuffleClient` retry reading data. Users should set `celeborn.worker.graceful.shutdown.enabled` to `true` and
set below service port with stable port to support worker recover status.
```
celeborn.worker.rpc.port
celeborn.worker.fetch.port
celeborn.worker.push.port
celeborn.worker.replicate.port
```
