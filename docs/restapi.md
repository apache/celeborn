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

## REST API

Celeborn supports REST API and available for both master and worker. The endpoints are mounted at `host:port`.
For example,
for the master, they would typically be accessible at `http://<master-http-host>:<master-http-port><path>`, and
for the worker, at `http://<worker-http-host>:<worker-http-port><path>`.

And the swagger UI is available at `http://<http-host>:<http-port>/swagger` (since 0.5.0) both for master and worker.

The configuration of `<master-http-host>`, `<master-http-port>`, `<worker-http-host>`, `<worker-http--port>` as below:

| Key                       | Default | Description         | Since |
|---------------------------|---------|---------------------|-------|
| celeborn.master.http.host | 0.0.0.0 | Master's http host. | 0.4.0 |
| celeborn.master.http.port | 9098    | Master's http port. | 0.4.0 |
| celeborn.worker.http.host | 0.0.0.0 | Worker's http host. | 0.4.0 |
| celeborn.worker.http.port | 9096    | Worker's http port. | 0.4.0 |

### Deprecated REST APIs

Since 0.6.0, the legacy REST APIs are deprecated and will be removed in the future.
The new REST APIs are available at `/api/v1`.
See the [migration guide](migration.md) for API mappings.

#### Master

| Path                 | Method | Parameters                                   | Meaning                                                                                                                                                                                                                                                                                                                                                      |
|----------------------|--------|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /applications        | GET    |                                              | List all running application's ids of the cluster.                                                                                                                                                                                                                                                                                                           |
| /conf                | GET    |                                              | List the conf setting of the master.                                                                                                                                                                                                                                                                                                                         |
| /excludedWorkers     | GET    |                                              | List all excluded workers of the master.                                                                                                                                                                                                                                                                                                                     |
| /help                | GET    |                                              | List the available API providers of the master.                                                                                                                                                                                                                                                                                                              |
| /hostnames           | GET    |                                              | List all running application's LifecycleManager's hostnames of the cluster.                                                                                                                                                                                                                                                                                  |
| /listDynamicConfigs  | GET    | level=${LEVEL} tenant=${TENANT} name=${NAME} | List the dynamic configs of the master. The parameter level specifies the config level of dynamic configs. The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level. |
| /lostWorkers         | GET    |                                              | List all lost workers of the master.                                                                                                                                                                                                                                                                                                                         |
| /masterGroupInfo     | GET    |                                              | List master group information of the service. It will list all master's LEADER, FOLLOWER information.                                                                                                                                                                                                                                                        |
| /metrics/prometheus  | GET    |                                              | List the metrics data in prometheus format of the master. The url path is defined by configure `celeborn.metrics.prometheus.path`.                                                                                                                                                                                                                           |
| /shuffle             | GET    |                                              | List all running shuffle keys of the service. It will return all running shuffle's key of the cluster.                                                                                                                                                                                                                                                       |
| /shutdownWorkers     | GET    |                                              | List all shutdown workers of the master.                                                                                                                                                                                                                                                                                                                     |
| /decommissionWorkers | GET    |                                              | List all decommission workers of the master.                                                                                                                                                                                                                                                                                                                 |
| /threadDump          | GET    |                                              | List the current thread dump of the master.                                                                                                                                                                                                                                                                                                                  |
| /workerEventInfo     | GET    |                                              | List all worker event information of the master.                                                                                                                                                                                                                                                                                                             |
| /workerInfo          | GET    |                                              | List worker information of the service. It will list all registered workers' information.                                                                                                                                                                                                                                                                    |
| /exclude             | POST   | add=${ADD_WORKERS} remove=${REMOVE_WORKERS}  | Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove, which value is separated by commas.                                                                                                                                                           |
| /sendWorkerEvent     | POST   | type=${EVENT_TYPE} workers=${WORKERS}        | For Master(Leader) can send worker event to manager workers. Legal `type`s are 'None', 'Immediately', 'Decommission', 'DecommissionThenIdle', 'Graceful', 'Recommission', and the parameter workers is separated by commas.                                                                                                                                  |

#### Worker

| Path                       | Method | Parameters                                   | Meaning                                                                                                                                                                                                                                                                                                                                                      |
|----------------------------|--------|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /applications              | GET    |                                              | List all running application's ids of the worker. It only return application ids running in that worker.                                                                                                                                                                                                                                                     |
| /conf                      | GET    |                                              | List the conf setting of the worker.                                                                                                                                                                                                                                                                                                                         |
| /help                      | GET    |                                              | List the available API providers of the worker.                                                                                                                                                                                                                                                                                                              |
| /isRegistered              | GET    |                                              | Show if the worker is registered to the master success.                                                                                                                                                                                                                                                                                                      |
| /isShutdown                | GET    |                                              | Show if the worker is during the process of shutdown.                                                                                                                                                                                                                                                                                                        |
| /isDecommissioning         | GET    |                                              | Show if the worker is during the process of decommission.                                                                                                                                                                                                                                                                                                    |
| /listDynamicConfigs        | GET    | level=${LEVEL} tenant=${TENANT} name=${NAME} | List the dynamic configs of the worker. The parameter level specifies the config level of dynamic configs. The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level. |
| /listPartitionLocationInfo | GET    |                                              | List all the living PartitionLocation information in that worker.                                                                                                                                                                                                                                                                                            |
| /metrics/prometheus        | GET    |                                              | List the metrics data in prometheus format of the worker. The url path is defined by configure `celeborn.metrics.prometheus.path`.                                                                                                                                                                                                                           |
| /shuffle                   | GET    |                                              | List all the running shuffle keys of the worker. It only return keys of shuffles running in that worker.                                                                                                                                                                                                                                                     |
| /threadDump                | GET    |                                              | List the current thread dump of the worker.                                                                                                                                                                                                                                                                                                                  |
| /unavailablePeers          | GET    |                                              | List the unavailable peers of the worker, this always means the worker connect to the peer failed.                                                                                                                                                                                                                                                           |
| /workerInfo                | GET    |                                              | List the worker information of the worker.                                                                                                                                                                                                                                                                                                                   |
| /exit                      | POST   | type=${EXIT_TYPE}                            | Trigger this worker to exit. Legal `type`s are 'Decommission', 'Graceful' and 'Immediately'.                                                                                                                                                                                                                                                                 |

### `/api/v1` APIs (Since 0.6.0)

#### Master

See the master openapi spec yaml in the repo `openapi/openapi-client/src/main/openapi3/master_rest_v1.yaml`, or use the [Swagger Editor](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/celeborn/main/openapi/openapi-client/src/main/openapi3/master_rest_v1.yaml) online for visualization.

#### Worker

See the worker openapi spec yaml in the repo `openapi/openapi-client/src/main/openapi3/worker_rest_v1.yaml`, or use the [Swagger Editor](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/celeborn/main/openapi/openapi-client/src/main/openapi3/worker_rest_v1.yaml) online for visualization.
