---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at
    
      https://www.apache.org/licenses/LICENSE-2.0
    
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Decommissioning
===

## Worker Decommission

Celeborn provides support for decommissioning workers via a REST API, which enables administrators to
efficiently manage cluster resizing and the removal of unhealthy worker nodes without disrupting ongoing jobs.

## Decommission Process

Here's a detailed breakdown of how the decommissioning process works:

- Decommissioning Request: Administrators can send a decommission request through the REST API
to initiate the process for one or more worker nodes.

- Handling New Requests: Once the decommissioning process starts, the affected worker nodes will no longer
accept new shuffle slot requests or new data. This ensures that no new tasks are assigned to
the workers that are set to be decommissioned.

- Existing Data Handling: The worker nodes will continue to handle their existing shuffle partitions
until all the partitions have expired. This mechanism ensures that current jobs running on these nodes
can complete their data shuffle operations without interruption.

- Worker Exit: After all existing shuffle partitions on the worker nodes have expired,
the workers will gracefully exit. This ensures that the node is safely removed from the cluster
without causing data loss or job failures.

This decommissioning process is essential for maintaining cluster health and efficiency,
as it allows for the smooth removal of unhealthy nodes and enables dynamic resizing of the cluster
to meet varying workload demands.

## Decommission Configuration

| Key                                               | Value |
|---------------------------------------------------|-------| 
| celeborn.worker.decommission.forceExitTimeout     | 6h    |
| celeborn.worker.decommission.checkInterval        | 30s   |
| celeborn.worker.decommission.signal.checker.class | org.apache.celeborn.service.deploy.worker.decommission.NoOpDecommissionSignalChecker |
| celeborn.worker.decommission.signal.check.interval | 30s  |
| celeborn.worker.decommission.signal.file.path     | (none) |


## Perform Decommissioning

Administrators perform decommissioning operation in two approaches:

1. Via Celeborn Worker REST API endpoint:
  ```shell
  curl -X POST -H "Content-Type: application/json" -d '{"type":"Decommission"}' http://ip:port/api/v1/workers/exit
  ```
2. Via Celeborn Master(Leader) REST API endpoint:
  ```shell
  curl -X POST -H "Content-Type: application/json" -d '{"eventType":"Decommission","workers":[{"host":"192.168.15.140","rpcPort":"37359","pushPort":"38303","fetchPort":"37569","replicatePort":"37093"},{"host":"192.168.15.141","rpcPort":"37359","pushPort":"38303","fetchPort":"37569","replicatePort":"37093"}]}' http://ip:port/api/v1/workers/events
  curl -X POST -H "Content-Type: application/json" -d '{"eventType":"DecommissionThenIdle","workers":[{"host":"192.168.15.140","rpcPort":"37359","pushPort":"38303","fetchPort":"37569","replicatePort":"37093"},{"host":"192.168.15.141","rpcPort":"37359","pushPort":"38303","fetchPort":"37569","replicatePort":"37093"}]}' http://ip:port/api/v1/workers/events
  ```

Details of decommissioning interface can refer to [REST API](../restapi/#rest-api)

## Self-decommission via external signal

A worker can decommission itself when something outside Celeborn says the host is going away,
such as a spot-instance interruption notice, a Kubernetes node drain, or a maintenance agent.
Set `celeborn.worker.decommission.signal.checker.class` to an implementation of
`org.apache.celeborn.service.deploy.worker.decommission.DecommissionSignalChecker`. The worker
calls it every `celeborn.worker.decommission.signal.check.interval`. The first time it returns
`true`, the worker runs the same decommission as the worker REST API with type `Decommission`,
and polling stops.

Things to know:

- The trigger is one-way. A later `false` does not recommission the worker.
- The worker does not poll the checker while it is already decommissioning or shutting down.
  A worker in `DecommissionThenIdle` is still polled, and a signal moves it to `Decommission`.
- An exception from the checker is logged and treated as "no signal".
- If the class can't be loaded, doesn't implement the interface, or its `init` throws, the worker
  fails to start with an error that names the config key and the class.
- The default `NoOpDecommissionSignalChecker` never signals, and the worker starts no polling
  thread for it.

### File-based checker

Celeborn ships `FileDecommissionSignalChecker`, which signals once a file exists. For example, a
Kubernetes `preStop` hook or a spot-notice agent can run `touch /var/run/celeborn/decommission`,
with this in `celeborn-defaults.conf`:

```properties
celeborn.worker.decommission.signal.checker.class org.apache.celeborn.service.deploy.worker.decommission.FileDecommissionSignalChecker
celeborn.worker.decommission.signal.file.path /var/run/celeborn/decommission
celeborn.worker.decommission.signal.check.interval 10s
```

### Custom checker

Put a class like the one below on the worker classpath. It needs a public no-arg constructor.
`init` is called once before the first poll, `shouldDecommission` is called from a single
dedicated thread and should return quickly, and `close` is called once when the worker stops.

```java
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.service.deploy.worker.decommission.DecommissionSignalChecker;

public class SpotNoticeChecker implements DecommissionSignalChecker {
  @Override
  public void init(CelebornConf conf) {
    // read settings from conf, open clients
  }

  @Override
  public boolean shouldDecommission() throws Exception {
    return spotInterruptionNoticed(); // e.g. query the instance metadata endpoint
  }

  @Override
  public String getReason() {
    return "spot interruption notice";
  }
}
```

## Decommission Monitoring

Administrators can monitor the status of the workers to ensure they are gracefully exiting
after all tasks are complete.

Administrators can monitor the status of the workers under decommission through worker REST API [ip:port/isDecommissioning](../monitoring/#worker_1)
or worker metrics [IsDecommissioningWorker](../monitoring/#worker).
Also, administrator can monitor count of workers decommissioned through master metrics [DecommissionWorkerCount](../monitoring/#master).

By providing a REST API and metrics for decommissioning workers,
Celeborn ensures that cluster administrators have a robust and flexible tool
to manage cluster resources effectively, enhancing overall system stability and performance.
