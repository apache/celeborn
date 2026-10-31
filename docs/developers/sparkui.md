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

# Celeborn Spark UI

Celeborn ships a Spark plugin that adds a **Celeborn** tab to the Spark Web UI of applications using Celeborn for shuffle. The tab aggregates Celeborn shuffle metrics from task metrics, making it easy to check shuffle write/read throughput and the impact of shuffle on task duration at a glance.

The plugin classes are packaged in the shaded Celeborn client jars (`celeborn-client-spark-3-shaded` / `celeborn-client-spark-4-shaded`), so no extra jar is required — the shaded jar already on the classpath is enough.

## Enabling the Plugin

The UI extension is disabled by default. To enable it, set `spark.plugins` in the Spark configuration on top of the regular Celeborn shuffle settings (see [Deployment](../../deploy)):

```properties
spark.shuffle.manager org.apache.spark.shuffle.celeborn.SparkShuffleManager
...
spark.plugins org.apache.spark.shuffle.celeborn.CelebornPlugin
```

**Note**: `spark.plugins` accepts a comma-separated list; append `org.apache.spark.shuffle.celeborn.CelebornPlugin` if other Spark plugins are already configured.

## Using the Celeborn Tab

Once enabled, the Spark Web UI shows a **Celeborn** tab with a shuffle summary page. The summary covers overall shuffle write/read throughput and the share of task time spent on shuffle, followed by collapsible tables with details such as the effective `spark.celeborn.*` configuration (sensitive values are automatically redacted), shuffle slot assignments per shuffle and per worker, compression ratio, and the Celeborn client build information.

## Spark History Server

The plugin also works with the Spark History Server. `CelebornHistoryServerPlugin` is registered through Spark's `AppHistoryServerPlugin` SPI, so finished applications are replayed with the same Celeborn tab. Make sure the shaded Celeborn client jar is on the History Server classpath.

**Note**: the application must have enabled `CelebornPlugin` and set `spark.eventLog.enabled=true` while it was running, so that the Celeborn UI data is recorded in the event log. Adding the jar only to the History Server will not add the Celeborn tab to applications that did not enable the plugin.
