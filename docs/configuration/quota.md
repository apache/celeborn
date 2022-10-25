---
license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
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
| celeborn.shuffle.checkQuota.enabled | true |  | 0.2.0 | 
| celeborn.shuffle.identity.provider | org.apache.celeborn.common.identity.DefaultIdentityProvider | Identity provider class name. Default value use `DefaultIdentityProvider`, return `UserIdentifier` with default tenant id and username from `UserGroupInformation`.  | 0.2.0 | 
| celeborn.shuffle.quota.configuration.path | &lt;undefined&gt; |  | 0.2.0 | 
| celeborn.shuffle.quota.manager | org.apache.celeborn.common.quota.DefaultQuotaManager |  | 0.2.0 | 
<!--end-include-->
