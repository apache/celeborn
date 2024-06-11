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

Decommission Worker
===

## Decommission Worker

Celeborn supports using Restful API to decommission workers, facilitating admins to manage cluster resizing,
and decommissioning of unhealthy worker nodes without impacting the running jobs.
When starting decommissioning workers, corresponding worker won't receive new shuffle slot request and new data,
after all existing shuffle partition expired. The worker will exit.
User also can set `celeborn.worker.decommission.forceExitTimeout` to set the max wait time for decommission.
User can know if current work is during decommission through Restful API [ip:port/isDecommissioning](../monitoring/#worker_1)
or metrics [IsDecommissioningWorker](../monitoring/#worker).

### Worker setting

| Key                                               | Value |
|---------------------------------------------------|-------| 
| celeborn.worker.decommission.forceExitTimeout     | 6h    |
| celeborn.worker.decommission.checkInterval        | 30s   |

### Restful Example
Can refer to [Restful API](../monitoring/#worker_1)

```text
curl --request POST --url 'ip:port/exit' --data '{"type":"Decommission"}'
```
