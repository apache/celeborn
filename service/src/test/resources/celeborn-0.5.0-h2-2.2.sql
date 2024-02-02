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

CREATE SCHEMA IF NOT EXISTS TEST;

SET SCHEMA TEST;
DROP TABLE IF exists celeborn_cluster_info;
DROP TABLE IF exists celeborn_cluster_system_config;
DROP TABLE IF exists celeborn_cluster_tenant_config;

CREATE TABLE celeborn_cluster_info
(
    id         int          NOT NULL AUTO_INCREMENT,
    name       varchar(255) NOT NULL COMMENT 'celeborn cluster name',
    namespace  varchar(255) DEFAULT NULL COMMENT 'celeborn cluster namespace',
    endpoint   varchar(255) DEFAULT NULL COMMENT 'celeborn cluster endpoint',
    gmt_create timestamp NULL DEFAULT NULL,
    gmt_modify timestamp NULL DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_cluster_unique_name` (`name`)
    );

CREATE TABLE celeborn_cluster_system_config
(
    id           int NOT NULL AUTO_INCREMENT,
    cluster_id   int NOT NULL,
    config_key   varchar(255) DEFAULT NULL,
    config_value varchar(255) DEFAULT NULL,
    type         varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
    gmt_create   timestamp NOT NULL,
    gmt_modify   timestamp NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_unique_system_config_key` (`cluster_id`, `config_key`)
    );

CREATE TABLE celeborn_cluster_tenant_config
(
    id          int          NOT NULL AUTO_INCREMENT,
    cluster_id   int          NOT NULL,
    tenant_id    varchar(255) NOT NULL,
    level        varchar(255) NOT NULL COMMENT 'config level, valid level is TENANT,USER',
    `user`         varchar(255) DEFAULT NULL COMMENT 'tenant sub user',
    config_key   varchar(255) NOT NULL,
    config_value varchar(255) NOT NULL,
    type         varchar(255) DEFAULT NULL COMMENT 'conf categories, such as quota',
    gmt_create   timestamp NOT NULL,
    gmt_modify   timestamp NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY `index_unique_tenant_config_key` (`cluster_id`, `tenant_id`, `user`, `config_key`)
    );


INSERT INTO celeborn_cluster_info (`id`, `name`, `namespace`, `endpoint`, `gmt_create`, `gmt_modify`)
VALUES (1, 'default', 'celeborn-1', 'celeborn-namespace.endpoint.com', '2023-08-26 22:08:30', '2023-08-26 22:08:30');


INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (1, 1, 'celeborn.client.push.buffer.initial.size', '102400', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (2, 1, 'celeborn.client.push.buffer.max.size', '1024000', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (3, 1, 'celeborn.worker.fetch.heartbeat.enabled', 'true', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (5, 1, 'celeborn.client.push.buffer.initial.size.only', '10240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (6, 1, 'celeborn.test.timeoutMs.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (7, 1, 'celeborn.test.enabled.only', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_system_config` (`id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (8, 1, 'celeborn.test.int.only', '10', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');


INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (1, 1, 'tenant_id', 'TENANT', '', 'celeborn.client.push.buffer.initial.size', '10240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');

INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (2, 1, 'tenant_id', 'TENANT', '', 'celeborn.client.push.buffer.initial.size.only', '102400', 'QUOTA', '2023-08-26 22:08:30',
        '2023-08-26 22:08:30');

INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (3, 1, 'tenant_id', 'TENANT', '', 'celeborn.worker.fetch.heartbeat.enabled', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');

INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (4, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.timeoutMs.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (5, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.enabled.only', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');
INSERT INTO `celeborn_cluster_tenant_config` (`id`, `cluster_id`, `tenant_id`, `level`, `user`,
                                              `config_key`, `config_value`, `type`, `gmt_create`,
                                              `gmt_modify`)
VALUES (6, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.int.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30');