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
| celeborn.quota.enabled | true | false | When Master side sets to true, the master will enable to check the quota via QuotaManager. When Client side sets to true, LifecycleManager will request Master side to check whether the current user has enough quota before registration of shuffle. Fallback to the default shuffle service of Spark when Master side checks that there is no enough quota for current user. | 0.2.0 |  | 
| celeborn.quota.identity.provider | org.apache.celeborn.common.identity.DefaultIdentityProvider | false | IdentityProvider class name. Default class is `org.apache.celeborn.common.identity.DefaultIdentityProvider`. Optional values: org.apache.celeborn.common.identity.HadoopBasedIdentityProvider user name will be obtained by UserGroupInformation.getUserName; org.apache.celeborn.common.identity.DefaultIdentityProvider user name and tenant id are default values or user-specific values. | 0.2.0 |  | 
| celeborn.quota.identity.user-specific.tenant | default | false | Tenant id if celeborn.quota.identity.provider is org.apache.celeborn.common.identity.DefaultIdentityProvider. | 0.3.0 |  | 
| celeborn.quota.identity.user-specific.userName | default | false | User name if celeborn.quota.identity.provider is org.apache.celeborn.common.identity.DefaultIdentityProvider. | 0.3.0 |  |
| celeborn.quota.interruptShuffle.enabled | false | false | Whether to enable interrupt shuffle when quota exceeds. | 0.6.0 |  |
| celeborn.quota.tenant.diskBytesWritten | 9223372036854775807 | true | Quota dynamic configuration for written disk bytes. 0 means that no shuffle submissions are allowed. | 0.5.0 |  |
| celeborn.quota.tenant.diskFileCount | 9223372036854775807 | true | Quota dynamic configuration for written disk file count. 0 means that no shuffle submissions are allowed. | 0.5.0 |  |
| celeborn.quota.tenant.hdfsBytesWritten | 9223372036854775807 | true | Quota dynamic configuration for written hdfs bytes. 0 means that no shuffle submissions are allowed. | 0.5.0 |  |
| celeborn.quota.tenant.hdfsFileCount | 9223372036854775807 | true | Quota dynamic configuration for written hdfs file count. 0 means that no shuffle submissions are allowed. | 0.5.0 |  |
<!--end-include-->
