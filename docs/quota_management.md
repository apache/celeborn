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

Celeborn allows administrators to set quotas for the number of names used.
A system-level default quota can also be set for all users who haven't specified a named quota.

When `celeborn.quota.enabled` is set to true, the master checks the quota via QuotaManager.
If the same setting is enabled on the client side, the LifecycleManager will request
the master to check if the current user identifier has enough quota before registration of shuffle.
If there's not enough quota, the system will fallback to the default shuffle service of Spark.

## Quota Indicators

Celeborn supports fine-grained quota management, including four indicators:

- `celeborn.quota.tenant.diskBytesWritten`: Maximum allowed disk write bytes, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.diskFileCount`: Maximum allowed disk write file num, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.hdfsBytesWritten`: Maximum allowed HDFS write bytes, default value `Long.MAX_VALUE`.
- `celeborn.quota.tenant.hdfsFileCount`: Maximum allowed HDFS write file num, default value `Long.MAX_VALUE`.

## User Identifier

Celeborn supports user identifiers on two levels:

- Tenant ID
- Username

The client's LifecycleManager will request the master to check the quota for the current user defined by user setting. Users can set `celeborn.quota.identity.provider` to choose an identity provider. We currently support two types:

- `org.apache.celeborn.common.identity.HadoopBasedIdentityProvider`: The username will be obtained by `UserGroupInformation.getUserName()`, tenant id will be default.
- `org.apache.celeborn.common.identity.DefaultIdentityProvider`: The username and tenant id are default values or user-specific values set by `celeborn.quota.identity.user-specific.tenant` and `celeborn.quota.identity.user-specific.userName`.

By default, Celeborn uses `org.apache.celeborn.common.identity.DefaultIdentityProvider`.
Users can also implement their own identity provider by inheriting the `org.apache.celeborn.common.identity.IdentityProvider` interface.

## QuotaManager

Celeborn initializes a QuotaManager on the master side to check quotas.
QuotaManager uses the [dynamic config service](developers/configuration.md#dynamic-configuration)to store quota settings.
QuotaManager supports two types of store backends configured by the `celeborn.dynamicConfig.store.backend` parameter:

- FS: [FileSystem Store Backend](#FileSystem-Store-Backend)
- DB: [Database Store Backend](#Database-Store-Backend)

Currently, QuotaManager supports [three levels](developers/configuration.md#Config-Level) of quota settings.

### FileSystem Store Backend

This backend reads [quota](#Quota) settings from a user-specified dynamic config file.
Here's an example quota setting YAML file:

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

The quota for `system default` is
- diskBytesWritten: 1G
- diskFileCount: 100
- hdfsBytesWritten: 1G
- diskFileCount: Long.MAX_VALUE

### Database Store Backend
This backend reads quota settings from a user-specified database.
For more information on using the database store backend, refer to [database config service](developers/configuration.md#database-config-service).