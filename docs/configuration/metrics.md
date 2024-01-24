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
| Key | Default | Description | Since | Deprecated |
| --- | ------- | ----------- | ----- | ---------- |
| celeborn.metrics.app.topDiskUsage.count | 50 | Size for top items about top disk usage applications list. | 0.2.0 |  | 
| celeborn.metrics.app.topDiskUsage.interval | 10min | Time length for a window about top disk usage application list. | 0.2.0 |  | 
| celeborn.metrics.app.topDiskUsage.windowSize | 24 | Window size about top disk usage application list. | 0.2.0 |  | 
| celeborn.metrics.capacity | 4096 | The maximum number of metrics which a source can use to generate output strings. | 0.2.0 |  | 
| celeborn.metrics.collectPerfCritical.enabled | false | It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them. | 0.2.0 |  | 
| celeborn.metrics.conf | &lt;undefined&gt; | Custom metrics configuration file path. Default use `metrics.properties` in classpath. | 0.3.0 |  | 
| celeborn.metrics.enabled | true | When true, enable metrics system. | 0.2.0 |  | 
| celeborn.metrics.extraLabels |  | If default metric labels are not enough, extra metric labels can be customized. Labels' pattern is: `<label1_key>=<label1_value>[,<label2_key>=<label2_value>]*`; e.g. `env=prod,version=1` | 0.3.0 |  | 
| celeborn.metrics.json.path | /metrics/json | URI context path of json metrics HTTP server. | 0.4.0 |  | 
| celeborn.metrics.json.pretty.enabled | true | When true, view metrics in json pretty format | 0.4.0 |  | 
| celeborn.metrics.prometheus.path | /metrics/prometheus | URI context path of prometheus metrics HTTP server. | 0.4.0 |  | 
| celeborn.metrics.sample.rate | 1.0 | It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0]. | 0.2.0 |  | 
| celeborn.metrics.timer.slidingWindow.size | 4096 | The sliding window size of timer metric. | 0.2.0 |  | 
| celeborn.metrics.worker.pauseSpentTime.forceAppend.threshold | 10 | Force append worker pause spent time even if worker still in pause serving state.Help user can find worker pause spent time increase, when worker always been pause state. |  |  | 
<!--end-include-->
