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

Quota Management
===

Celeborn allows the administrator to set quotas for the number of names used.
The administrator also can set a system level default quota for all user that 
doesn't set a specified a named quota.

When Master side sets `celeborn.quota.enabled` to true, the master will enable
to check the quota via QuotaManager. When Client side also sets `celeborn.quota.enabled`
to true, LifecycleManager will request master side to check whether the current user identifier
has enough quota before registration of shuffle. Fallback to the default shuffle service
of Spark when Master side checks that there is no enough quota for current user.

## Quota
Celeborn supports fine-grained quota management, now Celeborn including four indicators:
- `celeborn.quota.tenant.diskBytesWritten`: Maximum allowed disk write bytes, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.diskFileCount`: Maximum allowed disk write file num, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.hdfsBytesWritten`: Maximum allowed hdfs write bytes, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.hdfsFileCount`: Maximum allowed hdfs write file num, default value `Long.MAX_VALUE`.

## User Identifier
The Celeborn supports user identifier for two level:
  - tenant id
  - username
The client LifecycleManager will request master side to check quota for current user that defined by user setting.
User can set `celeborn.quota.identity.provider` to choose a identity provider, now we support two type provider:
  - `org.apache.celeborn.common.identity.HadoopBasedIdentityProvider`: username will be obtained by `UserGroupInformation.getUserName()`, tenant id will be default.
  - `org.apache.celeborn.common.identity.DefaultIdentityProvider`: username and tenant id are default values or user-specific values setting by `celeborn.quota.identity.user-specific.tenant` and `celeborn.quota.identity.user-specific.userName`.
Celeborn uses `org.apache.celeborn.common.identity.DefaultIdentityProvider` as default. 
Also, the Celeborn users can implement their own specified identify provider inheriting interface `org.apache.celeborn.common.identity.IdentityProvider` by yourself.

## QuotaManager
Celeborn will initialize a QuotaManager in the master side. Currently, QuotaManager supports three level configuration same as : 
  - Tenant User Level: When the tenant user level quota config is null or empty, fallback to the tenant level quota config. When the tenant level config is null or empty, fallback to the system level config again.
  - Tenant Level: When the tenant level config is null or empty, fallback to the system level config.
  - System Level: When the system level config is also null or empty, fallback to default value `Long.MAX_VALUE`.

QuotaManager uses dynamic [config service](#) to store quota settings mentioned in [Quota](#Quota),
QuotaManger also support two types of store backend configured by parameter `celeborn.dynamicConfig.store.backend`:
  - FS: [FileSystem Store Backend](#FileSystem Store Backend)
  - DB: [Database Store Backend](#Database Store Backend)

### FileSystem Store Backend
FileSystem store backend will read quota configuration file setting by 
`celeborn.quota.configuration.path`, if this path is not set,
The Celeborn will fall back to `dynamicConfig.yaml` under conf directory `$CELEBORN_HOME/conf/`.

### Example
The example yaml file as below:
```yaml
-  level: SYSTEM
   config:
     celeborn.quota.tenant.diskBytesWritten: 1G
     celeborn.quota.tenant.diskFileCount: 100
     celeborn.quota.tenant.hdfsBytesWritten: 1G

-  tenantId: tenant_01
   level: TENANT
   config:
     celeborn.quota.tenant.diskBytesWritten: 10G
     celeborn.quota.tenant.diskFileCount: 1000
     celeborn.quota.tenant.hdfsBytesWritten: 10G
   users:
     - name: Jerry
       config:
         celeborn.quota.tenant.diskBytesWritten: 100G
         celeborn.quota.tenant.diskFileCount: 10000
```

The quota for user `tenant_01.Jerry` is
  - diskBytesWritten: 100G
  - diskFileCount: 10000
  - hdfsBytesWritten: 10G
  - diskFileCount: Long.MAX_VALUE

The quota for tenant id `tenant_01` is
  - diskBytesWritten: 10G
  - diskFileCount: 1000
  - hdfsBytesWritten: 10G
  - diskFileCount: Long.MAX_VALUE

The quota for system default is
  - diskBytesWritten: 1G
  - diskFileCount: 100
  - hdfsBytesWritten: 1G
  - diskFileCount: Long.MAX_VALUE 

### Database Store Backend
Database Store Backend will read [quota configuration](#Quota) from user specified database.
For how to use Database store backend can refer to dynamic config service document.
