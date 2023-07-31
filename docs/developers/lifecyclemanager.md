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

# LifecycleManager

## Overview
`LifecycleManager` maintains information of each shuffle for the application:

- All active shuffle ids
- `Worker`s that are serving each shuffle, and what `PartitionLocation`s are on each `Worker`
- Status of each shuffle, i.e. not committed, committing, committed, data lost, expired
- The newest `PartitionLocation` with the largest epoch of each partition id
- User identifier for this application

Also, `LifecycleManager` handles control messages with `ShuffleClient` and Celeborn `Master`, typically, it receives
requests from `ShuffleClient`:

- RegisterShuffle
- Revive/PartitionSplit
- MapperEnd/StageEnd
- GetReducerFileGroup

to handle the requests, `LifecycleManager` will send requests to `Master` and `Worker`s:

- Heartbeat to `Master`
- RequestSlots to `Master`
- UnregisterShuffle to `Master`
- ReserveSlots to `Worker`
- CommitFiles to `Worker`
- DestroyWorkerSlots to `Worker`

## RegisterShuffle
As described in [PushData](../../developers/pushdata#lazy-shuffle-register), `ShuffleClient` lazily send
RegisterShuffle to LifecycleManager, so many concurrent requests will be sent to `LifecycleManager`.

To ensure only one request for each shuffle is handled, `LifecycleManager` puts tail requests in a set and only
let go the first request. When the first request finishes, `LifecycleManager` responds to all cached requests.

The process of handling RegisterShuffle is as follows:

`LifecycleManager` sends RequestSlots to `Master`, `Master` allocates slots for the shuffle, as
[Here](../../developers/master#slots-allocation) describes.

Upon receiving slots allocation result, `LifecycleManager` sends ReserveSlots to all `Workers`s allocated
in parallel. `Worker`s then select a disk and initialize for each `PartitionLocation`, see
[Here](../../developers/storage#local-disk-and-memory-buffer).

After all related `Worker`s successfully reserved slots, `LifecycleManager` stores the shuffle information in
memory and responds to all pending and future requests.

## Revive/PartitionSplit
Celeborn handles push data failure in a so-called Revive mechanism, see
[Here](../../developers/faulttolerant#handle-pushdata-failure). Similar to [Split](../../developers/pushdata#split),
they both asks `LifecycleManager` for a new epoch of `PartitionLocation` for future data pushing.

Upon receiving Revive/PartitionSplit, `LifecycleManager` first checks whether it has a newer epoch locally, if so
it just responds the newer one. If not, like handling RegisterShuffle, it puts tail requests for the same partition id
in a set and only let go the first one.

Unlike RegisterShuffle, `LifecycleManager` does not send RequestSlots to `Master` to ask for new `Worker`s. Instead,
it randomly picks `Worker`s from local `Worker` list, excluding the failing ones. This design is to avoid too many
RPCs to `Master`.

Then `LifecycleManager` sends ReserveSlots to the picked `Worker`s. When success, it responds the new
`PartitionLocation`s to `ShuffleClient`s.

## MapperEnd/StageEnd
Celeborn needs to known when shuffle write stage ends to persist shuffle data, check if any data lost, and prepare for
shuffle read. Many compute engines do not signal such event (for example, Spark's ShuffleManager does not
have such API), Celeborn has to recognize that itself.

To achieve this, Celeborn requires `ShuffleClient` to specify the number of map tasks in RegisterShuffle request,
and send MapperEnd request to `LifecycleManager` when a map task succeeds. When MapperEnd are received for every
map id, `LifecycleManager` knows that the shuffle write stage ends, and sends CommitFiles to related `Worker`s.

For many compute engines, a map task may launch multiple attempts (i.e. speculative execution), and the engine
chooses one of them as successful attempt. However, there is no way for Celeborn to know about the chosen attempt.
Instead, `LifecycleManager` records the first attempt sending MapperEnd as the success one for each map task,
and ignores other attempts. This is correct because compute engines guarantee that all attempts for a map task
generate the same output data.

Upon receiving CommitFiles, `Worker`s flush buffered data to files and responds the succeeded and failed
`PartitionLocation`s to `LifecycleManager`, see [Here](../../developers/storage#local-disk-and-memory-buffer).
`LifecycleManager` then checks if any of `PartitionLocation` loses both primary and replica data (mark data lost if so),
and stores the information in memory.

## GetReducerFileGroup
Reduce task asks `LifecycleManager` for `PartitionLocation`s of each partition id to read data. To reduce the number
of RPCs, `ShuffleClient` asks for the mapping from all partition ids to their `PartitionLocation`s and caches in
memory, through GetReducerFileGroup request

Upon receiving the request, `LifecycleManager` responds the cached mapping or indicates data lost.

## Heartbeat to Master
`LifecycleManager` periodically sends heartbeat to `Master`, piggybacking the following infomation:

- Bytes and files written by the application, used to calculate estimated partition size, see
  [Here](../../developers/master#maintain-active-shuffles)
- `Worker` list that `LifecycleManager` wants `Master` to tell status

## UnregisterShuffle
When compute engines tells Celeborn that some shuffle is complete (i.e. through unregisterShuffle for Spark),
`LifecycleManager` first checks and waits for write stage end, then put the shuffle id into unregistered set,
after some expire time it removes the id and sends UnregisterShuffle to `Master` for cleanup, see
[Here](../../developers/master#maintain-active-shuffles)

## DestroyWorkerSlots
Normally, `Worker`s cleanup resources for `PartitionLocation`s after notified shuffle unregistered. 
In some abnormal cases, `Master` will send DestroyWorkerSlots to early cleanup, for example if some `Worker`s fail
to reserve slots, `LifecycleManager` will tell the successfully reserved `Worker`s to release the slots.

## Batch RPCs
Some RPCs are of high frequent, for example Revive/PartitionSplit, CommitFiles, DestroyWorkerSlots. To reduce
the number of RPCs, `LifecycleManager` batches the same kind of RPCs and periodically checks and sends to `Master`
through a dedicated thread.

Users can enable and tune batch RPC through the following configs:
`celeborn.client.shuffle.batch*`