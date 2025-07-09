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

# Worker
The main functions of Celeborn `Worker` are:

- Store, serve, and manage `PartitionLocation` data. See [Storage](../../developers/storage)
- Traffic control through `Back Pressure` and `Congestion Control`. See
  [Traffic Control](../../developers/trafficcontrol)
- Support rolling upgrade through `Graceful Shutdown`
- Support elasticity through `Decommission Shutdown`
- Self health check

Celeborn `Worker` has four dedicated servers:

- `Controller` handles control messages, i.e. `ReserveSlots`, `CommitFiles`, and `DestroyWorkerSlots`
- `Push Server` handles primary input data, i.e. `PushData` and `PushMergedData`, and push related control messages
- `Replicate Server` handles replica input data, it has the same logic with `Push Server`
- `Fetch Server` handles fetch requests, i.e. `ChunkFetchRequest`, and fetch related control messages
