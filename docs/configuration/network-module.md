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

The various transport modules which can be configured are:

| Module | Parent Module | Description |
| ------ | ------------- | ----------- |
| rpc_app | rpc | Configure control plane RPC environment used by Celeborn within the application. For backward compatibility, supports fallback to `rpc` parent module for missing configuration.<br/> Note, this is for RPC environment - see below for other transport modules |
| rpc_service | rpc | Configure control plane RPC environment when communicating with Celeborn service hosts. This includes all RPC communication from application to Celeborn Master/Workers, as well as between Celeborn masters/workers themselves.<br/> For backward compatibility, supports fallback to `rpc` parent module for missing configuration.<br/>  As with `rpc_app`, this is only for RPC environment see below for other transport modules.|
| rpc | - | Fallback parent transport module for `rpc_app` and `rpc_service`. It is advisible to use the specific transport modules while configuring - `rpc` exists primarily for backward compatibility |
| push | - | Configure transport module for handling data push at Celeborn workers |
| fetch | - | Configure transport module for handling data fetch at Celeborn workers |
| data | - | Configure transport module for handling data push and fetch at Celeborn apps |
| replicate | - | Configure transport module for handling data replication between Celeborn workers |


<!--end-include-->
