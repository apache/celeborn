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
| Key | Default | isDynamic | Description | Since | Deprecated |
| --- | ------- | --------- | ----------- | ----- | ---------- |
| celeborn.logConf.enabled | false | false | When `true`, log the CelebornConf for debugging purposes. | 0.5.0 |  | 
| celeborn.web.host | &lt;localhost&gt; | false | Hostname for web to bind. | 0.6.0 |  | 
| celeborn.web.http.host | &lt;localhost&gt; | false | Web's http host. | 0.6.0 |  | 
| celeborn.web.http.idleTimeout | 30s | false | Web http server idle timeout. | 0.6.0 |  | 
| celeborn.web.http.maxWorkerThreads | 200 | false | Maximum number of threads in the web http worker thread pool. | 0.6.0 |  | 
| celeborn.web.http.port | 9091 | false | Web's http port. | 0.6.0 |  | 
| celeborn.web.http.stopTimeout | 5s | false | Web http server stop timeout. | 0.6.0 |  | 
| celeborn.web.port | 9090 | false | Port for web to bind. | 0.6.0 |  | 
<!--end-include-->
