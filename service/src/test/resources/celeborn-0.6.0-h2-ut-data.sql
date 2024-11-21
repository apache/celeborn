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

INSERT INTO celeborn_cluster_info ( `id`, `name`, `namespace`, `endpoint`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 'default', 'celeborn-1', 'celeborn-namespace.endpoint.com', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
INSERT INTO `celeborn_cluster_system_config` ( `id`, `cluster_id`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'celeborn.client.push.buffer.initial.size', '102400', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'celeborn.client.push.buffer.max.size', '1024000', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'celeborn.worker.fetch.heartbeat.enabled', 'true', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 5, 1, 'celeborn.client.push.buffer.initial.size.only', '10240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 6, 1, 'celeborn.test.timeoutMs.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 7, 1, 'celeborn.test.enabled.only', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 8, 1, 'celeborn.test.int.only', '10', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
INSERT INTO `celeborn_cluster_tenant_config` ( `id`, `cluster_id`, `tenant_id`, `level`, `name`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'tenant_id', 'TENANT', '', 'celeborn.client.push.buffer.initial.size', '10240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'tenant_id', 'TENANT', '', 'celeborn.client.push.buffer.initial.size.only', '102400', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'tenant_id', 'TENANT', '', 'celeborn.worker.fetch.heartbeat.enabled', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 4, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.timeoutMs.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 5, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.enabled.only', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 6, 1, 'tenant_id', 'TENANT', '', 'celeborn.test.tenant.int.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 7, 1, 'tenant_id', 'TENANT', '', 'celeborn.client.push.queue.capacity', '1024', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 8, 1, 'tenant_id1', 'TENANT', '', 'celeborn.client.push.buffer.initial.size', '10240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 9, 1, 'tenant_id1', 'TENANT', '', 'celeborn.client.push.buffer.initial.size.only', '102400', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 10, 1, 'tenant_id1', 'TENANT', '', 'celeborn.worker.fetch.heartbeat.enabled', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 11, 1, 'tenant_id1', 'TENANT', '', 'celeborn.test.tenant.timeoutMs.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 12, 1, 'tenant_id1', 'TENANT', '', 'celeborn.test.tenant.enabled.only', 'false', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 13, 1, 'tenant_id1', 'TENANT', '', 'celeborn.test.tenant.int.only', '100s', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 14, 1, 'tenant_id1', 'TENANT', '', 'celeborn.client.push.queue.capacity', '1024', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 15, 1, 'tenant_id1', 'TENANT_USER', 'Jerry', 'celeborn.client.push.buffer.initial.size', '1k', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 16, 1, 'tenant_id1', 'TENANT_USER', 'Jerry', 'celeborn.client.push.buffer.initial.size.user.only', '512k', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
INSERT INTO `celeborn_cluster_tags` ( `id`, `cluster_id`, `tag`, `worker_id`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'tag1', 'host1:1111', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'tag1', 'host2:2222', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'tag2', 'host3:3333', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 4, 1, 'tag2', 'host4:4444', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
