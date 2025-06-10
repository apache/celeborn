/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE IF NOT EXISTS celeborn_cluster_info
(
    id         int          NOT NULL AUTO_INCREMENT,
    name       varchar(255) NOT NULL COMMENT 'celeborn cluster name',
    namespace  varchar(255) DEFAULT NULL COMMENT 'celeborn cluster namespace',
    endpoint   varchar(255) DEFAULT NULL COMMENT 'celeborn cluster endpoint',
    gmt_create timestamp NOT NULL,
    gmt_modify timestamp NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_cluster_unique_name` (`name`)
);

CREATE TABLE IF NOT EXISTS celeborn_cluster_system_config
(
    id           int NOT NULL AUTO_INCREMENT,
    cluster_id   int NOT NULL,
    config_key   varchar(255) NOT NULL,
    config_value varchar(255) NOT NULL,
    type         varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
    gmt_create   timestamp NOT NULL,
    gmt_modify   timestamp NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_unique_system_config_key` (`cluster_id`, `config_key`)
);

CREATE TABLE IF NOT EXISTS celeborn_cluster_tenant_config
(
    id           int          NOT NULL AUTO_INCREMENT,
    cluster_id   int          NOT NULL,
    tenant_id    varchar(255) NOT NULL,
    level        varchar(255) NOT NULL COMMENT 'config level, valid level is TENANT,TENANT_USER',
    name         varchar(255) DEFAULT NULL COMMENT 'tenant sub user',
    config_key   varchar(255) NOT NULL,
    config_value varchar(255) NOT NULL,
    type         varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
    gmt_create   timestamp NOT NULL,
    gmt_modify   timestamp NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_unique_tenant_config_key` (`cluster_id`, `tenant_id`, `name`, `config_key`)
);

CREATE TABLE IF NOT EXISTS celeborn_cluster_tags
(
    id         int          NOT NULL AUTO_INCREMENT,
    cluster_id int          NOT NULL,
    tag        varchar(255) NOT NULL,
    worker_id  varchar(255) NOT NULL,
    gmt_create timestamp    NOT NULL,
    gmt_modify timestamp    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_unique_cluster_tag_key` (`cluster_id`, `tag`, `worker_id`)
);
