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
| celeborn.master.metrics.prometheus.host | 0.0.0.0 | Master's Prometheus host. | 0.2.0 | 
| celeborn.master.metrics.prometheus.port | 9098 | Master's Prometheus port. | 0.2.0 | 
| celeborn.metrics.capacity | 4096 | The maximum number of metrics which a source can use to generate output strings. | 0.2.0 | 
| celeborn.metrics.collectPerfCritical.enabled | false | It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them. | 0.2.0 | 
| celeborn.metrics.enabled | true | When true, enable metrics system. | 0.2.0 | 
| celeborn.metrics.sample.rate | 1.0 | It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0]. | 0.2.0 | 
| celeborn.metrics.timer.slidingWindow.size | 4096 | The sliding window size of timer metric. | 0.2.0 | 
| celeborn.worker.metrics.prometheus.host | 0.0.0.0 | Worker's Prometheus host. | 0.2.0 | 
| celeborn.worker.metrics.prometheus.port | 9096 | Worker's Prometheus port. | 0.2.0 | 
<!--end-include-->
