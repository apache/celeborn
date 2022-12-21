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

For example, if an Celeborn worker has 10 storage directories or disks and the buffer size is set to 256 KiB.
The necessary off-heap memory is 10 GiB.

Network memory will be consumed when netty reads from a TPC channel, there will need some extra
memory. Empirically, Celeborn worker off-heap memory should be set to `(numDirs  * bufferSize * 1.2)`.

## All Configurations

### Client

{!
include-markdown "./client.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Columnar Shuffle

{!
include-markdown "./columnar-shuffle.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Master

{!
include-markdown "./master.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

### Worker

{!
include-markdown "./worker.md"
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

### Metrics

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

| Key                              | Default                                         | Description |
|----------------------------------|-------------------------------------------------|-------------|
| `CELEBORN_HOME`                  | ``$(cd "`dirname "$0"`"/..; pwd)``              |             |
| `CELEBORN_CONF_DIR`              | `${CELEBORN_CONF_DIR:-"${CELEBORN_HOME}/conf"}` |             |
| `CELEBORN_MASTER_MEMORY`         | 1 GB                                            |             |
| `CELEBORN_WORKER_MEMORY`         | 1 GB                                            |             |
| `CELEBORN_WORKER_OFFHEAP_MEMORY` | 1 GB                                            |             |
| `CELEBORN_MASTER_JAVA_OPTS`      |                                                 |             |
| `CELEBORN_WORKER_JAVA_OPTS`      |                                                 |             |
| `CELEBORN_PID_DIR`               | `${CELEBORN_HOME}/pids`                         |             |
| `CELEBORN_LOG_DIR`               | `${CELEBORN_HOME}/logs`                         |             |
| `CELEBORN_SSH_OPTS`              | `-o StrictHostKeyChecking=no`                   |             |
| `CELEBORN_SLEEP`                 |                                                 |             |

## Tuning

Assume we have a cluster described as below:
5 Celeborn Workers with 20 GB off-heap memory and 10 disks.
As we need to reserve 20% off-heap memory for netty,
so we could assume 16 GB off-heap memory can be used for flush buffers.

If `spark.celeborn.push.buffer.size` is 64 KB, we can have in-flight requests up to 1310720.
If you have 8192 mapper tasks, you could set `spark.celeborn.push.maxReqsInFlight=160` to gain performance improvements.

If `celeborn.worker.flush.buffer.size` is 256 KB, we can have total slots up to 327680 slots.

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
