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
| celeborn.master.ha.enabled | false | false | When true, master nodes run as Raft cluster mode. | 0.3.0 | celeborn.ha.enabled | 
| celeborn.master.ha.node.&lt;id&gt;.host | &lt;required&gt; | false | Host to bind of master node <id> in HA mode. | 0.3.0 | celeborn.ha.master.node.&lt;id&gt;.host | 
| celeborn.master.ha.node.&lt;id&gt;.internal.port | 8097 | false | Internal port for the workers and other masters to bind to a master node <id> in HA mode. | 0.5.0 |  | 
| celeborn.master.ha.node.&lt;id&gt;.port | 9097 | false | Port to bind of master node <id> in HA mode. | 0.3.0 | celeborn.ha.master.node.&lt;id&gt;.port | 
| celeborn.master.ha.node.&lt;id&gt;.ratis.port | 9872 | false | Ratis port to bind of master node <id> in HA mode. | 0.3.0 | celeborn.ha.master.node.&lt;id&gt;.ratis.port | 
| celeborn.master.ha.ratis.raft.rpc.type | netty | false | RPC type for Ratis, available options: netty, grpc. | 0.3.0 | celeborn.ha.master.ratis.raft.rpc.type | 
| celeborn.master.ha.ratis.raft.server.storage.dir | /tmp/ratis | false | Root storage directory to hold RaftServer data. | 0.3.0 | celeborn.ha.master.ratis.raft.server.storage.dir | 
| celeborn.master.ha.ratis.raft.server.storage.startup.option | RECOVER | false | Startup option of RaftServer storage. Available options: RECOVER, FORMAT. | 0.5.0 |  | 
<!--end-include-->
