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

# Worker Tags

Worker tags in Celeborn allow users to assign specific tags (labels) to workers
within a cluster. These tags enable grouping workers with similar characteristics,
allowing applications with different priorities or users to access distinct
groups of workers, thereby creating isolated sub-clusters.

Worker tags can be applied for various purposes, including but not limited to:

  - **Configuration-Based Tagging**: Workers tagged by hardware configurations (e.g., "hdd-14t", "ssd-245g", "high-nw").
  - **Environment Segmentation**: Workers grouped by environment names, such as "production" or "staging".
  - **Tenant Isolation**: Tags for different tenants to ensure resource isolation.
  - **Rolling Upgrades**: Tags like "v0-6-0" to manage controlled rolling upgrades effectively.

## Configuration

Worker tags can be enabled by setting `celeborn.tags.enabled` to `true` in the
`Master`. When enabled, `Master` will start selecting the workers using `TagsManager`
based on the tag expression provided for the application. If the worker tagging
is disabled or the application tag expression is empty, then all the available
workers will be selected.

Worker tags are part of `SystemConfig` and can be assigned and updated dynamically
using [dynamic config service backends](#store-backends).

### Tags Expression

Tags expression for an application is specified using `celeborn.tags.tagsExpr`.
This is a dynamic configuration that can be applied at the system, tenant, or
tenant-user level by administrators via the dynamic configuration service.

Clients can also specify custom tag expressions for applications using the
`celeborn.tags.tagsExpr` property, if the administrator sets
`celeborn.tags.preferClientTagsExpr` to true in the dynamic configuration.

Tags expressions are defined as a comma-separated list of tags, where each tag
is evaluated as an "AND" condition. This means only workers that match all
specified tags will be selected.

Example tag expression: `env=production,region=us-east,high-io,v0-0-6`
This tags expression will select workers that have all the following tags:
 - `env=production`
 - `region=us-east`
 - `high-io`
 - `v0-0-6`

### TagsQL

TagsQL extends the default tags expression format, offering enhanced flexibility
for worker tag selection. TagsQL can be enabled by setting `celeborn.tags.useTagsQL`
to `true` in `Master`.

TagsQL allows users to select workers based on tag key-value pairs and supports
the following syntax:
 - Match single value: `key:value`
 - Negate single value: `key:!value`
 - Match list of values: `key:{value1,value2}`
 - Negate list of values: `key:!{value1,value2}`

Example TagsQL expression: `env:production region:{us-east,us-west} env:!sandbox`
This tags expression will select the workers that have the following tags:
 - `env=production`
 - `region=us-east` OR `region=us-west`

and will ignore the workers that have the following tags:
 - `env=sandbox`

**NOTE: TagsQL only supports tags key-value pairs separate by a equal sign (`=`).**

### Store Backends

#### FileSystem Store Backend

This backend reads [worker tags and configurations](#configuration) settings from a
user-specified dynamic config file. For more information on using the FileSystem config store
backend, refer to [filesystem config service](developers/configuration.md#filesystem-config-service).

Here is an example of worker tags assignment and configuration via YAML file:

```yaml
-  level: SYSTEM
   config:
     celeborn.tags.preferClientTagsExpr: false
     celeborn.tags.tagsExpr: 'env=production'
   tags:
     env=production:
       - 'host1:1111'
       - 'host2:2222'
     env=staging:
       - 'host3:3333'
     region=us-east:
       - 'host1:1111'
       - 'host3:3333'
     region=us-west:
       - 'host2:2222'
-  tenantId: tenant_01
   level: TENANT
   config:
     celeborn.tags.preferClientTagsExpr: false
     celeborn.tags.tagsExpr: 'env=production,region=us-east'
   users:
     - name: Jerry
       config:
         celeborn.tags.preferClientTagsExpr: true
```

#### Database Store Backend

This backend reads [worker tags and configurations](#configuration) settings from a
user-specified database. For more information on using the database store backend,
refer to [database config service](developers/configuration.md#database-config-service).

Here is an example SQL of worker tags assignment and configuration:

```sql
# SYSTEM level configuration
INSERT INTO `celeborn_cluster_system_config` ( `id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'celeborn.tags.preferClientTagsExpr', 'true', 'master', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),
    ( 2, 1, 'celeborn.tags.tagsExpr', 'env=production', 'master', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),

# TENANT/TENANT_USER level configuration
INSERT INTO `celeborn_cluster_tenant_config` ( `id`, `cluster_id`, `tenant_id`, `level`, `name`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'tenant_01', 'TENANT', '', 'celeborn.tags.preferClientTagsExpr', 'true', 'master', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),
    ( 2, 1, 'tenant_01', 'TENANT', '', 'celeborn.tags.tagsExpr', 'env=production,region=us-east', 'master', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),
    ( 3, 1, 'tenant_01', 'TENANT_USER', 'Jerry', 'celeborn.tags.preferClientTagsExpr', 'true', 'master', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),

# Worker Tags Assignment
INSERT INTO `celeborn_cluster_tags` ( `id`, `cluster_id`, `tag`, `worker_id`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'env=production', 'host1:1111', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'env=production', 'host2:2222', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'env=staging',    'host3:3333', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 4, 1, 'region=us-east', 'host1:1111', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 5, 1, 'region=us-east', 'host3:3333', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 6, 1, 'region=us-west', 'host2:2222', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
```

## FAQ

#### - What happens if no worker matches the specified tagsExpr?
If no worker matches the specified tags expression, no workers will be selected
for the shuffle. Depending on application configurations, it can fall back to
Spark Shuffle.

#### - Can a worker have multiple tags, and can tags be updated for a running worker?
Yes, a worker can have multiple tags, and they can be dynamically updated for a
running/non-running worker via dynamic config service.

#### - Are there restrictions on the tag naming format?
Tags should be alphanumeric and can include dashes, underscores, and equal signs.
Avoid any special characters to ensure compatibility.
