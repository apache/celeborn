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

# Configuration
The configuration of Celeborn is divided into static and dynamic categories, with details provided in the [Configuration Guide](../configuration/index.md).

## Static Configuration
Static configuration, referred to as `CelebornConf`, loads configurations from the default file located at `$CELEBORN_HOME/conf/celeborn-defaults.conf`. 

## Dynamic Configuration
Dynamic configuration allows for changes to be applied at runtime, as necessary, and it takes precedence over the corresponding static configuration in the 
Celeborn `Master` and `Worker`. A configuration key's dynamic nature is indicated by the `isDynamic` property, as listed in [All Configurations](../configuration/index.md#all-configurations). 
This means that configurations tagged with the dynamic property can be updated and refreshed while Celeborn is running.

### Config Level
At present dynamic configuration supports various config levels including:

- `SYSTEM`: The system configurations.
- `TENANT`: The dynamic configurations of tenant id.
- `TENANT_USER`: The dynamic configurations of tenant id and username.

When applying dynamic configuration, the following is the order of precedence for configuration levels:

- `SYSTEM` level configuration takes precedence over static configuration and the default `CelebornConf`. 
If the system-level configuration is absent, it will fall back to the static configuration defined in `CelebornConf`.
- `TENANT` level configuration supersedes the `SYSTEM` level, meaning that configurations specific to a tenant id will override those set at the system level. 
If tenant-level configuration is absent, it will fall back to the system-level dynamic configuration.
- `TENANT_USER` level configuration takes precedence over `TENANT` level. Configurations specific to both a tenant id and username will override those set at the tenant level. 
If tenant-user-level configuration is missing, it will fall back to the tenant-level dynamic configuration.

## Config Service
The config service provides a configuration management service with a local cache for both static and dynamic configurations. Moreover, `ConfigService` is 
a pluggable service interface whose implementation can vary based on different storage backends. The storage backend for `ConfigService` is specified by the 
configuration key `celeborn.dynamicConfig.store.backend`, and it currently supports filesystem (`FS`) and database (`DB`) as storage backends by default.
Additionally, users can provide their own implementation by extending the `ConfigService` interface and using the fully qualified class name of the implementation
as storage backend. If no storage backend is specified, this indicates that the config service is disabled.

### FileSystem Config Service
The filesystem config service enables the use of dynamic configuration files, the location of which is set by the configuration key `celeborn.dynamicConfig.store.fs.path`. 
The template for the dynamic configuration is as follows:

```yaml
# SYSTEM level configuration
- level: SYSTEM
  config:
    [config_key]: [config_val]
    ...

# TENANT level configuration
- tenantId: [tenant_id]
  level: TENANT
  config:
    [config_key]: [config_val]
    ...
  users:
    # TENANT_USER level configuration
    - name: [name]
      config:
        [config_key]: [config_val]
        ...
```

For example, a Celeborn worker `celeborn-worker` has 10 storage directories or disks and the buffer size is set to 256 KiB. A tenant `tenantId1` only uses half of the storage 
and sets the buffer size to 128 KiB. Meanwhile, a user `user1` needs to change the buffer size to 96 KiB at runtime. The example configurations are as follows: 

```yaml
# SYSTEM level configuration
- level: SYSTEM
  config:
    celeborn.worker.flusher.buffer.size: 256K # sets buffer size of worker to 256 KiB

# TENANT level configuration
- tenantId: tenantId1
  level: TENANT
  config:
    celeborn.worker.flusher.buffer.size: 128K # sets buffer size of tenantId1 to 128 KiB
  users:
    # TENANT_USER level configuration
    - name: user1
      config:
        celeborn.worker.flusher.buffer.size: 96K # sets buffer size of tenantId1 and user1 to 128 KiB
```

### Database Config Service
The database config service updates dynamic configurations stored in the database using the JDBC approach. Configuration settings for the database storage backend 
are defined by the `celeborn.dynamicConfig.store.db.*` series of configuration keys. To use the database as a config store backend, it is necessary to create tables for 
dynamic configurations at the various configuration levels. The sql script for MySQL configuration tables is located under `$CELEBORN_HOME/db-scripts` directory.
After the creation of configuration tables, dynamic configuration of config levels is specified via inserting a configuration record in corresponding config level table.

Above example dynamic configurations can be supported via the following sql:

```sql
CREATE TABLE IF NOT EXISTS celeborn_cluster_info (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL COMMENT 'celeborn cluster name',
  namespace varchar(255) DEFAULT NULL COMMENT 'celeborn cluster namespace',
  endpoint varchar(255) DEFAULT NULL COMMENT 'celeborn cluster endpoint',
  gmt_create timestamp NOT NULL,
  gmt_modify timestamp NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY `index_cluster_unique_name` (`name`)
);

# SYSTEM level configuration
CREATE TABLE IF NOT EXISTS celeborn_cluster_system_config (
  id int NOT NULL AUTO_INCREMENT,
  cluster_id int NOT NULL,
  config_key varchar(255) NOT NULL,
  config_value varchar(255) NOT NULL,
  type varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
  gmt_create timestamp NOT NULL,
  gmt_modify timestamp NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY `index_unique_system_config_key` (`cluster_id`, `config_key`)
);

# TENANT/TENANT_USER level configuration
CREATE TABLE IF NOT EXISTS celeborn_cluster_tenant_config (
  id int NOT NULL AUTO_INCREMENT,
  cluster_id int NOT NULL,
  tenant_id varchar(255) NOT NULL,
  level varchar(255) NOT NULL COMMENT 'config level, valid level is TENANT,USER',
  name varchar(255) DEFAULT NULL COMMENT 'tenant sub user',
  config_key varchar(255) NOT NULL,
  config_value varchar(255) NOT NULL,
  type varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
  gmt_create timestamp NOT NULL,
  gmt_modify timestamp NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY `index_unique_tenant_config_key` (`cluster_id`, `tenant_id`, `name`, `config_key`)
);

INSERT INTO celeborn_cluster_info ( `id`, `name`, `namespace`, `endpoint`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 'default', 'celeborn-worker', 'celeborn-namespace.endpoint.com', '2024-02-27 22:08:30', '2024-02-27 22:08:30' );

# SYSTEM level configuration
# sets buffer size of celeborn-worker to 256 KiB
INSERT INTO `celeborn_cluster_system_config` ( `id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'celeborn.worker.flusher.buffer.size', '256K', 'QUOTA', '2024-02-27 22:08:30', '2024-02-27 22:08:30' );

# TENANT/TENANT_USER level configuration
# TENANT: sets buffer size of tenantId1 to 128 KiB
# TENANT_USER: sets buffer size of tenantId1 and user1 to 96 KiB
INSERT INTO `celeborn_cluster_tenant_config` ( `id`, `cluster_id`, `tenant_id`, `level`, `name`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'tenantId1', 'TENANT', '', 'celeborn.worker.flusher.buffer.size', '128K', 'worker', '2024-02-27 22:08:30', '2024-02-27 22:08:30' ),
    ( 2, 1, 'tenantId1', 'TENANT_USER', 'user1', 'celeborn.worker.flusher.buffer.size', '96K', 'worker', '2024-02-27 22:08:30', '2024-02-27 22:08:30' );
```

## Rest API

In addition to viewing the configurations, Celeborn support REST API available for both master and worker including:

- `/conf`: List the conf setting of master and worker.
- `/listDynamicConfigs`: List the dynamic configs of master and worker.

The API providers of listing configurations refer to [Available API providers](../monitoring.md#available-api-providers)
