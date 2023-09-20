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

# Traffic Control
This article describes the detailed design of Celeborn `Worker`'s traffic control.

## Design Goal
The design goal of Traffic Control is to prevent `Worker` OOM without harming performance. At the
same time, Celeborn tries to achieve fairness without harming performance.

Celeborn reaches the goal through `Back Pressure` and `Congestion Control`.

## Data Flow
From the `Worker`'s perspective, the income data flow comes from two sources:

- `ShuffleClient` that pushes primary data to the primary `Worker`
- Primary `Worker` that sends data replication to the replica `Worker`

The buffered memory can be released when the following conditions are satisfied:

- Data is flushed to file
- If replication is on, after primary data is written to wire

The basic idea is that, when `Worker` is under high memory pressure, slow down or stop income data, and at same
time force flush to release memory.

## Back Pressure
`Back Pressure` defines three watermarks:

- `Pause Receive` watermark (defaults to 0.85). If used direct memory ratio exceeds this, `Worker` will pause
  receiving data from `ShuffleClient`, and force flush buffered data into file.
- `Pause Replicate` watermark (defaults to 0.95). If used direct memory ratio exceeds this, `Worker` will pause
  receiving both data from `ShuffleClient` and replica data from primary `Worker`, and force flush buffered
  data into file.
- `Resume` watermark (defaults to 0.7). When either `Pause Receive` or `Pause Replicate` is triggered, to resume
  receiving data from `ShuffleClient`, the used direct memory ratio should decrease under this watermark.

`Worker` high-frequently checks used direct memory ratio, and triggers `Pause Receive`, `Pause Replicate` and `Resume`
accordingly. The state machine is as follows:

![backpressure](../../assets/img/backpressure.svg)

`Back Pressure` is the basic traffic control and can't be disabled. Users can tune the three watermarks through the
following configuration.

- `celeborn.worker.directMemoryRatio*`

## Congestion Control
`Congestion Control` is an optional mechanism for traffic control, the purpose is to slow down the push rate
from `ShuffleClient` when memory is under pressure, and suppress those who occupied the most resources in the
last time window. It defines two watermarks:

- `Low Watermark`, under which everything goes OK
- `High Watermark`, when exceeds this, top users will be Congestion Controlled

Celeborn uses `UserIdentifier` to identify users. `Worker` collects bytes pushed from each user in the last time
window. When used direct memory exceeds `High Watermark`, users who occupied more resources than the average
occupation will receive `Congestion Control` message.

`ShuffleClient` controls the push ratio in a fashion that is very like `TCP Congestion Control`. Initially, it's in
`Slow Start` phase, with a low push rate but increases very fast. When threshold is reached, it transfers to
`Congestion Avoidance` phase, which slowly increases push rate. Upon receiving `Congestion Control`, it goes back
to `Slow Start` phase.

`Congestion Control` can be enabled and tuned by the following configurations:

- `celeborn.worker.congestionControl.*`
