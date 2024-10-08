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

# Blaze Support

## Build Blaze

[Blaze](https://github.com/kwai/blaze) supports Celeborn as remote shuffle service. Below introduction is used to enable this feature.

First refer to [Build From Source](https://github.com/kwai/blaze/blob/master/README.md#build-from-source) or [Build With Docker](https://github.com/kwai/blaze/blob/master/README.md#build-with-docker) to build Blaze.

## Blaze Configuration

Currently, to use Blaze following configurations are required in `spark-defaults.conf`.

```
spark.shuffle.manager org.apache.spark.sql.execution.blaze.shuffle.celeborn.BlazeCelebornShuffleManager

# celeborn master
spark.celeborn.master.endpoints clb-master:9097

# we recommend set `spark.celeborn.push.replicate.enabled` to true to enable server-side data replication
# If you have only one worker, this setting must be false 
spark.celeborn.client.push.replicate.enabled true

spark.celeborn.client.spark.shuffle.writer hash
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.localShuffleReader.enabled false
```

## Availability

| Celeborn Version | Available in Blaze? | 
|:----------------:|:-------------------:|
|     < 0.5.0      |         No          |    
|    \>= 0.5.0     |         Yes         |
