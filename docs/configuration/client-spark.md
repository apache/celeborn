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

<!--begin-include-->
| Key | Default | Description | Since |
| --- | ------- | ----------- | ----- |
| celeborn.client.push.buffer.initial.size | 8k |  | 0.3.0 | 
| celeborn.client.push.buffer.max.size | 64k | Max size of reducer partition buffer memory for shuffle hash writer. The pushed data will be buffered in memory before sending to Celeborn worker. For performance consideration keep this buffer size higher than 32K. Example: If reducer amount is 2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` heap memory. | 0.3.0 | 
| celeborn.client.push.sort.memory.threshold | 64m | When SortBasedPusher use memory over the threshold, will trigger push data. | 0.3.0 | 
| celeborn.client.push.sort.pipeline.enabled | false | Whether to enable pipelining for sort based shuffle writer. If true, double buffering will be used to pipeline push | 0.3.1 | 
| celeborn.client.shuffle.forceFallback.enabled | false | Whether force fallback shuffle to Spark's default. | 0.3.0 | 
| celeborn.client.shuffle.forceFallback.numPartitionsThreshold | 500000 | Celeborn will only accept shuffle of partition number lower than this configuration value. | 0.3.0 | 
| celeborn.client.shuffle.writer | HASH | Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine when memory pressure is high or shuffle partition count is huge. | 0.3.0 | 
| celeborn.push.unsafeRow.fastWrite.enabled | true | This is Celeborn's optimization on UnsafeRow for Spark and it's true by default. If you have changed UnsafeRow's memory layout set this to false. | 0.2.2 | 
<!--end-include-->
