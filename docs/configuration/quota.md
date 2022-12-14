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
| celeborn.quota.configuration.path | &lt;undefined&gt; | Quota configuration file path. The file format should be yaml. Quota configuration file template can be found under conf directory. | 0.2.0 | 
| celeborn.quota.enabled | true | When true, before registering shuffle, LifecycleManager should check if current user have enough quota space, if cluster don't have enough quota space for current user, fallback to Spark's default shuffle | 0.2.0 | 
| celeborn.quota.identity.provider | org.apache.celeborn.common.identity.DefaultIdentityProvider | IdentityProvider class name. Default class is `org.apache.celeborn.common.identity.DefaultIdentityProvider`, return `org.apache.celeborn.common.identity.UserIdentifier` with default tenant id and username from `org.apache.hadoop.security.UserGroupInformation`.  | 0.2.0 | 
| celeborn.quota.manager | org.apache.celeborn.common.quota.DefaultQuotaManager | QuotaManger class name. Default class is `org.apache.celeborn.common.quota.DefaultQuotaManager`. | 0.2.0 | 
<!--end-include-->
