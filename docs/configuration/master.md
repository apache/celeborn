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
| celeborn.cluster.name | default | false | Celeborn cluster name. | 0.5.0 |  | 
| celeborn.container.info.provider | org.apache.celeborn.server.common.container.DefaultContainerInfoProvider | false | ContainerInfoProvider class name. Default class is `org.apache.celeborn.server.common.container.DefaultContainerInfoProvider`.  | 0.6.0 |  | 
| celeborn.dynamicConfig.refresh.interval | 120s | false | Interval for refreshing the corresponding dynamic config periodically. | 0.4.0 |  | 
| celeborn.dynamicConfig.store.backend | &lt;undefined&gt; | false | Store backend for dynamic config service. The store backend can be specified in two ways: - Using the short name of the store backend defined in the implementation of `ConfigStore#getName` whose return value can be mapped to the corresponding backend implementation. Available options: FS, DB. - Using the service class name of the store backend implementation. If not provided, it means that dynamic configuration is disabled. | 0.4.0 |  | 
| celeborn.dynamicConfig.store.db.fetch.pageSize | 1000 | false | The page size for db store to query configurations. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.connectionTimeout | 30s | false | The connection timeout that a client will wait for a connection from the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.driverClassName |  | false | The jdbc driver class name of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.idleTimeout | 600s | false | The idle timeout that a connection is allowed to sit idle in the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.jdbcUrl |  | false | The jdbc url of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.maxLifetime | 1800s | false | The maximum lifetime of a connection in the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.maximumPoolSize | 2 | false | The maximum pool size of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.password |  | false | The password of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.username |  | false | The username of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.fs.path | &lt;undefined&gt; | false | The path of dynamic config file for fs store backend. The file format should be yaml. The default path is `${CELEBORN_CONF_DIR}/dynamicConfig.yaml`. | 0.5.0 |  | 
| celeborn.internal.port.enabled | false | false | Whether to create a internal port on Masters/Workers for inter-Masters/Workers communication. This is beneficial when SASL authentication is enforced for all interactions between clients and Celeborn Services, but the services can exchange messages without being subject to SASL authentication. | 0.5.0 |  | 
| celeborn.logConf.enabled | false | false | When `true`, log the CelebornConf for debugging purposes. | 0.5.0 |  | 
| celeborn.master.allowWorkerHostPattern | &lt;undefined&gt; | false | Pattern of worker host that allowed to register with the master. If not set, all workers are allowed to register. | 0.6.0 |  | 
| celeborn.master.denyWorkerHostPattern | &lt;undefined&gt; | false | Pattern of worker host that denied to register with the master. If not set, no workers are denied to register. | 0.6.0 |  | 
| celeborn.master.dfs.expireDirs.timeout | 1h | false | The timeout for an expired dirs to be deleted on dfs like HDFS, S3, OSS. | 0.6.0 |  | 
| celeborn.master.estimatedPartitionSize.initialSize | 64mb | false | Initial partition size for estimation, it will change according to runtime stats. | 0.3.0 | celeborn.shuffle.initialEstimatedPartitionSize | 
| celeborn.master.estimatedPartitionSize.maxSize | &lt;undefined&gt; | false | Max partition size for estimation. Default value should be celeborn.worker.shuffle.partitionSplit.max * 2. | 0.4.1 |  | 
| celeborn.master.estimatedPartitionSize.minSize | 8mb | false | Ignore partition size smaller than this configuration of partition size for estimation. | 0.3.0 | celeborn.shuffle.minPartitionSizeToEstimate | 
| celeborn.master.estimatedPartitionSize.update.initialDelay | 5min | false | Initial delay time before start updating partition size for estimation. | 0.3.0 | celeborn.shuffle.estimatedPartitionSize.update.initialDelay | 
| celeborn.master.estimatedPartitionSize.update.interval | 10min | false | Interval of updating partition size for estimation. | 0.3.0 | celeborn.shuffle.estimatedPartitionSize.update.interval | 
| celeborn.master.excludeWorker.unhealthyDiskRatioThreshold | 1.0 | false | Max ratio of unhealthy disks for excluding worker, when unhealthy disk is larger than max unhealthy count, master will exclude worker. If this value is set to 1, master will exclude worker of which disks are all unhealthy. | 0.6.0 |  | 
| celeborn.master.heartbeat.application.timeout | 300s | false | Application heartbeat timeout. | 0.3.0 | celeborn.application.heartbeat.timeout | 
| celeborn.master.heartbeat.worker.timeout | 120s | false | Worker heartbeat timeout. | 0.3.0 | celeborn.worker.heartbeat.timeout | 
| celeborn.master.host | &lt;localhost&gt; | false | Hostname for master to bind. | 0.2.0 |  | 
| celeborn.master.http.auth.administers |  | false | A comma-separated list of users who have admin privileges, Note, when celeborn.master.http.auth.supportedSchemes is not set, everyone is treated as administrator. | 0.6.0 |  | 
| celeborn.master.http.auth.basic.provider | org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl | false | User-defined password authentication implementation of org.apache.celeborn.spi.authentication.PasswdAuthenticationProvider | 0.6.0 |  | 
| celeborn.master.http.auth.bearer.provider | org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl | false | User-defined token authentication implementation of org.apache.celeborn.spi.authentication.TokenAuthenticationProvider | 0.6.0 |  | 
| celeborn.master.http.auth.supportedSchemes |  | false | A comma-separated list of master http auth supported schemes.<ul> <li>SPNEGO: Kerberos/GSSAPI authentication.</li> <li>BASIC: User-defined password authentication, the concreted implementation is configurable via `celeborn.master.http.auth.basic.provider`.</li> <li>BEARER: User-defined bearer token authentication, the concreted implementation is configurable via `celeborn.master.http.auth.bearer.provider`.</li></ul> | 0.6.0 |  | 
| celeborn.master.http.host | &lt;localhost&gt; | false | Master's http host. | 0.4.0 | celeborn.metrics.master.prometheus.host,celeborn.master.metrics.prometheus.host | 
| celeborn.master.http.idleTimeout | 30s | false | Master http server idle timeout. | 0.5.0 |  | 
| celeborn.master.http.maxWorkerThreads | 200 | false | Maximum number of threads in the master http worker thread pool. | 0.5.0 |  | 
| celeborn.master.http.port | 9098 | false | Master's http port. | 0.4.0 | celeborn.metrics.master.prometheus.port,celeborn.master.metrics.prometheus.port | 
| celeborn.master.http.proxy.client.ip.header | X-Real-IP | false | The HTTP header to record the real client IP address. If your server is behind a load balancer or other proxy, the server will see this load balancer or proxy IP address as the client IP address, to get around this common issue, most load balancers or proxies offer the ability to record the real remote IP address in an HTTP header that will be added to the request for other devices to use. Note that, because the header value can be specified to any IP address, so it will not be used for authentication. | 0.6.0 |  | 
| celeborn.master.http.spnego.keytab | &lt;undefined&gt; | false | The keytab file for SPNego authentication. | 0.6.0 |  | 
| celeborn.master.http.spnego.principal | &lt;undefined&gt; | false | SPNego service principal, typical value would look like HTTP/_HOST@EXAMPLE.COM. SPNego service principal would be used when celeborn http authentication is enabled. This needs to be set only if SPNEGO is to be used in authentication. | 0.6.0 |  | 
| celeborn.master.http.ssl.disallowed.protocols | SSLv2,SSLv3 | false | SSL versions to disallow. | 0.6.0 |  | 
| celeborn.master.http.ssl.enabled | false | false | Set this to true for using SSL encryption in http server. | 0.6.0 |  | 
| celeborn.master.http.ssl.include.ciphersuites |  | false | A comma-separated list of include SSL cipher suite names. | 0.6.0 |  | 
| celeborn.master.http.ssl.keystore.algorithm | &lt;undefined&gt; | false | SSL certificate keystore algorithm. | 0.6.0 |  | 
| celeborn.master.http.ssl.keystore.password | &lt;undefined&gt; | false | SSL certificate keystore password. | 0.6.0 |  | 
| celeborn.master.http.ssl.keystore.path | &lt;undefined&gt; | false | SSL certificate keystore location. | 0.6.0 |  | 
| celeborn.master.http.ssl.keystore.type | &lt;undefined&gt; | false | SSL certificate keystore type. | 0.6.0 |  | 
| celeborn.master.http.stopTimeout | 5s | false | Master http server stop timeout. | 0.5.0 |  | 
| celeborn.master.internal.port | 8097 | false | Internal port on the master where both workers and other master nodes connect. | 0.5.0 |  | 
| celeborn.master.persist.workerNetworkLocation | false | false |  | 0.6.0 |  | 
| celeborn.master.port | 9097 | false | Port for master to bind. | 0.2.0 |  | 
| celeborn.master.rackResolver.refresh.interval | 30s | false | Interval for refreshing the node rack information periodically. | 0.5.0 |  | 
| celeborn.master.send.applicationMeta.threads | 8 | false | Number of threads used by the Master to send ApplicationMeta to Workers. | 0.5.0 |  | 
| celeborn.master.slot.assign.extraSlots | 2 | false | Extra slots number when master assign slots. Provided enough workers are available. | 0.3.0 | celeborn.slots.assign.extraSlots | 
| celeborn.master.slot.assign.loadAware.diskGroupGradient | 0.1 | false | This value means how many more workload will be placed into a faster disk group than a slower group. | 0.3.0 | celeborn.slots.assign.loadAware.diskGroupGradient | 
| celeborn.master.slot.assign.loadAware.fetchTimeWeight | 1.0 | false | Weight of average fetch time when calculating ordering in load-aware assignment strategy | 0.3.0 | celeborn.slots.assign.loadAware.fetchTimeWeight | 
| celeborn.master.slot.assign.loadAware.flushTimeWeight | 0.0 | false | Weight of average flush time when calculating ordering in load-aware assignment strategy | 0.3.0 | celeborn.slots.assign.loadAware.flushTimeWeight | 
| celeborn.master.slot.assign.loadAware.numDiskGroups | 5 | false | This configuration is a guidance for load-aware slot allocation algorithm. This value is control how many disk groups will be created. | 0.3.0 | celeborn.slots.assign.loadAware.numDiskGroups | 
| celeborn.master.slot.assign.maxWorkers | 10000 | false | Max workers that slots of one shuffle can be allocated on. Will choose the smaller positive one from Master side and Client side, see `celeborn.client.slot.assign.maxWorkers`. | 0.3.1 |  | 
| celeborn.master.slot.assign.minWorkers | 100 | false | Min workers that slots of one shuffle should be allocated on. Provided enough workers are available. | 0.6.0 |  | 
| celeborn.master.slot.assign.policy | ROUNDROBIN | false | Policy for master to assign slots, Celeborn supports two types of policy: roundrobin and loadaware. Loadaware policy will be ignored when `HDFS` is enabled in `celeborn.storage.availableTypes` | 0.3.0 | celeborn.slots.assign.policy | 
| celeborn.master.userResourceConsumption.metrics.enabled | false | false | Whether to enable resource consumption metrics. | 0.6.0 |  | 
| celeborn.master.userResourceConsumption.update.interval | 30s | false | Time length for a window about compute user resource consumption. | 0.3.0 |  | 
| celeborn.master.workerUnavailableInfo.expireTimeout | 1800s | false | Worker unavailable info would be cleared when the retention period is expired. Set -1 to disable the expiration. | 0.3.1 |  | 
| celeborn.quota.cluster.enabled | true | false | Whether to enable cluster-level quota. | 0.6.0 |  | 
| celeborn.quota.enabled | true | false | When Master side sets to true, the master will enable to check the quota via QuotaManager. When Client side sets to true, LifecycleManager will request Master side to check whether the current user has enough quota before registration of shuffle. Fallback to the default shuffle service when Master side checks that there is no enough quota for current user. | 0.2.0 |  | 
| celeborn.quota.tenant.enabled | true | false | Whether to enable tenant-level quota. | 0.6.0 |  | 
| celeborn.quota.user.enabled | true | false | Whether to enable user-level quota. | 0.6.0 |  | 
| celeborn.redaction.regex | (?i)secret|password|token|access[.]key | false | Regex to decide which Celeborn configuration properties and environment variables in master and worker environments contain sensitive information. When this regex matches a property key or value, the value is redacted from the logging. | 0.5.0 |  | 
| celeborn.storage.availableTypes | HDD | false | Enabled storages. Available options: MEMORY,HDD,SSD,HDFS,S3,OSS. Note: HDD and SSD would be treated as identical. | 0.3.0 | celeborn.storage.activeTypes | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | false | HDFS base directory for Celeborn to store shuffle data. | 0.2.0 |  | 
| celeborn.storage.hdfs.kerberos.keytab | &lt;undefined&gt; | false | Kerberos keytab file path for HDFS storage connection. | 0.3.2 |  | 
| celeborn.storage.hdfs.kerberos.principal | &lt;undefined&gt; | false | Kerberos principal for HDFS storage connection. | 0.3.2 |  | 
| celeborn.storage.oss.access.key | &lt;undefined&gt; | false | OSS access key for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.oss.dir | &lt;undefined&gt; | false | OSS base directory for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.oss.endpoint | &lt;undefined&gt; | false | OSS endpoint for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.oss.ignore.credentials | true | false | Whether to skip oss credentials, disable this config to support jindo sdk . | 0.6.0 |  | 
| celeborn.storage.oss.secret.key | &lt;undefined&gt; | false | OSS secret key for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.s3.dir | &lt;undefined&gt; | false | S3 base directory for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.s3.endpoint.region | &lt;undefined&gt; | false | S3 endpoint for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.tags.enabled | true | false | Whether to enable tags for workers. | 0.6.0 |  | 
| celeborn.tags.preferClientTagsExpr | false | true | When `true`, prefer the tags expression provided by the client over the tags expression provided by the master. | 0.6.0 |  | 
| celeborn.tags.tagsExpr |  | true | Expression to filter workers by tags. The expression is a comma-separated list of tags. The expression is evaluated as a logical AND of all tags. For example, `prod,high-io` filters workers that have both the `prod` and `high-io` tags. | 0.6.0 |  | 
<!--end-include-->
