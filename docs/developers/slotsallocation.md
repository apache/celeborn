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

# Slots allocation

This article describes the detailed design of Celeborn workers' slots allocation.
Slots allocation is the core components about how Celeborn distribute workload amount workers.
We have achieved two approaches of slots allocation.

## Principle
Allocate slots to local disks unless explicit assigned to HDFS.

## LoadAware
### Related configs
```properties
celeborn.master.slot.assign.policy LOADAWARE
celeborn.master.slot.assign.loadAware.numDiskGroups 5
celeborn.master.slot.assign.loadAware.diskGroupGradient 0.1
celeborn.master.slot.assign.loadAware.flushTimeWeight 0
celeborn.master.slot.assign.loadAware.fetchTimeWeight 0
[spark.client.]celeborn.storage.availableTypes HDD,SSD
```
### Detail
Load-aware slots allocation will take following elements into consideration.
- disk's fetch time 
- disk's flush time 
- disk's usable space
- disk's used slot 

Slots allocator will find out all worker involved in this allocation and sort their disks by 
`disk's average flushtime * flush time weight + disk's average fetch time * fetch time weight`.
After getting the sorted disks list, Celeborn will split the disks into
`celeborn.master.slot.assign.loadAware.numDiskGroups` groups. The slots number to be placed into a disk group 
is controlled by the `celeborn.master.slot.assign.loadAware.diskGroupGradient` which means that a group's 
allocated slots number will be (1+`celeborn.master.slot.assign.loadAware.diskGroupGradient`) 
times to the group's slower than it.
For example, there is 5 groups, G1 , G2, G3, G4 and G5. If the G5 is allocated 100 slots.
Other groups will be G4:110, G3:121, G2:133, G1:146.

After Celeborn has decided the slots number of a disk group, slots will be distributed in disks of a disk group.
Each disk has a usableSlots which is calculated by `(disk's usable space)/(average partition size)-usedSlots`. 
The slots number to allocate in a disk is calculated by ` slots of this disk group * ( current disk's usableSlots / the sum of all disks' usableSlots in this group)`.
For example, G5 need to allocate 100 slots and have 3 disks D1 with usable slots 100, D2 with usable slots 50, D3 with usable slots 20.
The distribution will be D1:59, D2: 29, D3: 12.

If all slots can be place in disk groups, the slots allocation process is done. 

requested slots are more than all usable slots, slots can not be placed into disks.
Worker will need to allocate these slots to workers with local disks one by one.

## RoundRobin
### Detail
Roundrobin slots allocation will distribute all slots into all registered workers with disks. Celeborn will treat 
all workers as an array and place 1 slots in a worker until all slots are allocated. 
If a worker has multiple disks, the chosen disk index is `(monotone increasing disk index +1)  % disk count`.  

## Celeborn Worker's Behavior
1. When reserve slots Celeborn worker will decide a slot be placed in local disks or HDFS when reserve slots.
2. If a partition is evicted from memory, the partition might be placed in HDFS.
3. If a slot is explicitly assigned to HDFS, worker will put the slot in HDFS. 