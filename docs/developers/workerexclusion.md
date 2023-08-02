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

# Worker Exclusion
`Worker`s can fail, temporarily or permanently. To reduce the impact of `Worker` failure, Celeborn tries to
figure out `Worker` status as soon as possible, and as correct as possible. This article describes detailed
design of `Worker` exclusion.

## Participants
As described [Previously](../../developers/overview#components), Celeborn has three components: `Master`, `Worker`,
and `Client`. `Client` is further separated into `LifecycleManager` and `ShuffleClient`. `Master`/`LifecycleManager`
/`ShuffleClient` need to know about `Worker` status, actively or reactively.

## Master Side Exclusion
`Master` maintains the ground-truth status of `Worker`s, with relatively longer delay. `Master` maintains four
lists of `Worker`s with different status:

- Active list. `Worker`s that have successfully registered to `Master`, and heartbeat never timed out.
- Excluded list. `Worker`s that are inside active list, but have no available disks for allocating new
  slots. `Master` recognizes such `Worker`s through heartbeat from `Worker`s.
- Graceful shutdown list. `Worker`s that are inside active list, but have triggered
  [Graceful Shutdown](../../upgrading). `Master` expects these `Worker`s should re-register themselves soon.
- Lost list. `Worker`s whose heartbeat timed out. These `Worker`s will be removed from active and excluded
  list, but will not be removed from graceful shutdown list.

Upon receiving RequestSlots, `Master` will choose `Worker`s in active list subtracting excluded and graceful
shutdown list. Since `Master` only exclude `Worker`s upon heartbeat, it has relative long delay.

## ShuffleClient Side Exclusion
`ShuffleClient`'s local exclusion list is essential to performance. Say the timeout to create network
connection is 10s, if `ShuffleClient` blindly pushes data to a non-exist `Worker`, the task will hang for a long time.

Waiting for `Master` to inform the exclusion list is unacceptable because of the delay. Instead, `ShuffleClient`
actively exclude `Worker`s when it encounters critical exceptions, for example:

- Fail to create network connection
- Fail to push data
- Fail to fetch data
- Connection exception happened

In addition to exclude the `Worker`s locally, `ShuffleClient` also carries the cause of push failure with
[Revive](../../developers/faulttolerant#handle-pushdata-failure) to `LifecycleManager`, see the section below.

Such strategy is aggressive, false negative may happen. To rectify, `ShuffleClient` removes a `Worker` from
the excluded list whenever an event happens that indicates that `Worker` is available, for example:

- When the `Worker` is allocated slots in register shuffle
- When `LifecycleManager` says the `Worker` is available in response of Revive

Currently, exclusion in `ShuffleClient` is optional, users can configure using the following configs:

`celeborn.client.push/fetch.excludeWorkerOnFailure.enabled`

## LifecycleManager Side Exclusion 
The accuracy and delay in `LifecycleManager`'s exclusion list stands between `Master` and `Worker`. `LifecyleManager`
excludes a `Worker` in the following scenarios:

- Receives Revive request and the cause is critical
- Fail to send RPC to a `Worker`
- From `Master`'s excluded list, carried in the heartbeat response

`LifecycleManager` will remove `Worker` from the excluded list in the following scenarios:

- For critical causes, when timeout expires (defaults to 180s)
- For non-critical causes, when it's not in `Master`'s exclusion list

In the response of Revive, `LifecycleManager` checks the status of the `Worker` where previous push data has failed.
`ShuffleClient` will remove from local exclusion list if the `Worker` is available.