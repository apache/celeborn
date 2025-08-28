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

# Auron Support

## Build Auron

[Auron](https://github.com/apache/auron) supports Celeborn as remote shuffle service. Below introduction is used to enable this feature.

First refer to [Build From Source](https://github.com/apache/auron/blob/master/README.md#build-from-source) or [Build With Docker](https://github.com/apache/auron/blob/master/README.md#build-with-docker) to build Auron.

## Auron Configuration

Currently, to use Auron following configurations are required in `spark-defaults.conf`.

```
spark.shuffle.manager org.apache.spark.sql.execution.auron.shuffle.celeborn.AuronCelebornShuffleManager

# celeborn master
spark.celeborn.master.endpoints clb-master:9097

spark.celeborn.client.spark.shuffle.writer hash
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.localShuffleReader.enabled false
```

## Availability

| Celeborn Version | Available in Auron? | 
|:----------------:|:-------------------:|
|     < 0.5.0      |         No          |    
|    \>= 0.5.0     |         Yes         |
