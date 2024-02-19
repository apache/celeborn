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
    ( 1, 1, 'celeborn.quota.diskBytesWritten', '1073741824', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'celeborn.quota.diskFileCount', '100', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'celeborn.quota.hdfsBytesWritten', '1073741824', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
INSERT INTO `celeborn_cluster_tenant_config` ( `id`, `cluster_id`, `tenant_id`, `level`, `name`, `config_key`, `config_value`, `type`, `gmt_create`, `gmt_modify` )
VALUES
    ( 1, 1, 'tenant_01', 'TENANT', '', 'celeborn.quota.diskBytesWritten', '10737418240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 2, 1, 'tenant_01', 'TENANT', '', 'celeborn.quota.diskFileCount', '1000', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 3, 1, 'tenant_01', 'TENANT', '', 'celeborn.quota.hdfsBytesWritten', '10737418240', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 4, 1, 'tenant_01', 'TENANT_USER', 'Jerry', 'celeborn.quota.diskBytesWritten', '107374182400', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' ),
    ( 5, 1, 'tenant_01', 'TENANT_USER', 'Jerry', 'celeborn.quota.diskFileCount', '10000', 'QUOTA', '2023-08-26 22:08:30', '2023-08-26 22:08:30' );
