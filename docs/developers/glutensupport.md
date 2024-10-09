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

# Gluten Support
## Velox Backend

[Gluten](https://github.com/apache/incubator-gluten) with velox backend supports Celeborn as remote shuffle service. Below introduction is used to enable this feature.

First refer to [Get Started With Velox](https://gluten.apache.org/docs/getting-started/velox-backend) to build Gluten with velox backend.

When compiling the Gluten Java module, it's required to enable `celeborn` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Pceleborn -DskipTests
```

Then add the Gluten and Spark Celeborn Client packages to your Spark application's classpath(usually add them into `$SPARK_HOME/jars`).

- Celeborn: `celeborn-client-spark-3-shaded_2.12-[celebornVersion].jar`
- Gluten: `gluten-velox-bundle-spark3.x_2.12-xx-xx-SNAPSHOT.jar` (The bundled Gluten Jar. Make sure -Pceleborn is specified when it is built.)

## ClickHouse Backend

[Gluten](https://github.com/apache/incubator-gluten) with clickhouse backend supports Celeborn as remote shuffle service. Below introduction is used to enable this feature.

First refer to [Get Started With ClickHouse](https://gluten.apache.org/docs/getting-started/clickhouse-backend) to build Gluten with clickhouse backend.

When compiling the Gluten Java module, it's required to enable `celeborn` profile, as follows:

```
mvn clean package -Pbackends-clickhouse -Pspark-3.3 -Pceleborn -DskipTests
```

Then add the Spark Celeborn Client packages to your Spark application's classpath(usually add them into `$SPARK_HOME/jars`).

- Celeborn: `celeborn-client-spark-3-shaded_2.12-[celebornVersion].jar`

## Gluten Configuration

Currently, to use Gluten following configurations are required in `spark-defaults.conf`.

```
spark.shuffle.manager org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager

# celeborn master
spark.celeborn.master.endpoints clb-master:9097

spark.celeborn.client.spark.shuffle.writer hash
# This is not necessary if your Spark external shuffle service is Spark 3.1 or newer
spark.shuffle.service.enabled false
spark.sql.adaptive.localShuffleReader.enabled false

# If you want to use dynamic resource allocation,
# please refer to this URL (https://github.com/apache/celeborn/tree/main/assets/spark-patch) to apply the patch into your own Spark.
spark.dynamicAllocation.enabled false
```

## Availability
| Celeborn Version | Available in Gluten? | 
|:----------------:|:--------------------:|
|     < 0.2.0      |          No          |    
|    \>= 0.2.0     |         Yes          |