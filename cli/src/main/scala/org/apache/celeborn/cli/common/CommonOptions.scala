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

package org.apache.celeborn.cli.common

import picocli.CommandLine.{Command, Option, Spec}
import picocli.CommandLine.Model.CommandSpec

@Command
class CommonOptions {

  @Spec var spec: CommandSpec = _ // injected by picocli

  @Option(
    names = Array("--hostport"),
    paramLabel = "host:port",
    description = Array("The host and port"))
  private[cli] var hostPort: String = _

  @Option(
    names = Array("--host-list"),
    paramLabel = "h1,h2,h3...",
    description = Array("List of hosts to pass to the command"))
  private[cli] var hostList: String = _

  @Option(
    names = Array("--cluster"),
    paramLabel = "cluster_alias",
    description = Array("The alias of the cluster to use to query masters"))
  private[cli] var cluster: String = _

  // Required for getting dynamic config info
  @Option(
    names = Array("--config-level"),
    paramLabel = "level",
    description = Array("The config level of the dynamic configs"))
  private[cli] var configLevel: String = _

  // Required for getting dynamic config info
  @Option(
    names = Array("--config-tenant"),
    paramLabel = "tenant_id",
    description = Array("The tenant id of TENANT or TENANT_USER level."))
  private[cli] var configTenant: String = _

  // Required for getting dynamic config info
  @Option(
    names = Array("--config-name"),
    paramLabel = "username",
    description = Array("The username of the TENANT_USER level."))
  private[cli] var configName: String = _
}
