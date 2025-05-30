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

# Celeborn CLI

Celeborn CLI is the command line interface of Celeborn including the management of the master and worker service etc.

> **Note**:
> CLI requires version 0.6+ of Celeborn to work since it depends on OpenAPI for API calls.

## Availability

|  Version  | Available in src tarball? | Available in bin tarball? |
|:---------:|:-------------------------:|:-------------------------:|
|  < 0.6.0  |            No             |            No             |
| \>= 0.6.0 |            Yes            |            Yes            |


## Setup

To get the binary package `apache-celeborn-<VERSION>-bin.tgz`, download the pre-built binary tarball from [Download](https://celeborn.apache.org/download) or
the source tarball from [Download](https://celeborn.apache.org/download) for building Celeborn according to [Build](https://github.com/apache/celeborn?tab=readme-ov-file#build).

After getting the binary package `apache-celeborn-<VERSION>-bin.tgz`:

```shell
$ tar -C <DST_DIR> -zxvf apache-celeborn-<VERSION>-bin.tgz
$ ln -s <DST_DIR>/apache-celeborn-<VERSION>-bin <DST_DIR>/celeborn
```

Export the following environment variable and add the bin directory to the `$PATH`:

```shell
$ export CELEBORN_HOME=<DST_DIR>/celeborn
$ export PATH=${CELEBORN_HOME}/sbin:$PATH
```

You can use the following command to verify whether CLI works well:

```shell
$ celeborn-cli -V
Celeborn CLI - Celeborn <VERSION>
```

## Usage

The commands available can be seen via the help option `-h` or `--help`.

```shell
$ celeborn-cli -h
Usage: celeborn-cli [-hV] [COMMAND]
Scala Celeborn CLI
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  master
  worker
```

The basic usage of commands for master and worker service can also get with the help option `-h` or `--help`.
 
- master commands:

```shell
$ celeborn-cli master -h
Usage: celeborn-cli master [-hV] [--apps=appId] [--cluster=cluster_alias]
                           [--config-level=level] [--config-name=username]
                           [--config-tenant=tenant_id] [--host-list=h1,h2,
                           h3...] [--hostport=host:port] [--worker-ids=w1,w2,
                           w3...] (--show-masters-info | --show-cluster-apps |
                           --show-cluster-shuffles | --exclude-worker |
                           --remove-excluded-worker |
                           --send-worker-event=IMMEDIATELY | DECOMMISSION | 
                           DECOMMISSION_THEN_IDLE | GRACEFUL | RECOMMISSION | 
                           NONE | --show-worker-event-info |
                           --show-lost-workers | --show-excluded-workers |
                           --show-manual-excluded-workers |
                           --show-shutdown-workers |
                           --show-decommissioning-workers |
                           --show-lifecycle-managers | --show-workers |
                           --show-workers-topology | --show-conf |
                           --show-dynamic-conf | --show-thread-dump |
                           --show-container-info | --add-cluster-alias=alias |
                           --remove-cluster-alias=alias |
                           --remove-workers-unavailable-info |
                           --revise-lost-shuffles | --delete-apps)
                           [[--shuffleIds=<shuffleIds>]]
      --add-cluster-alias=alias
                             Add alias to use in the cli for the given set of
                               masters
      --auth-header=authHeader
                             The http `Authorization` header for
                               authentication. It should be in the format of
                               `Bearer <token>` or `Basic
                               <base64-encoded-credentials>`.
      --apps=appId           The application Id list seperated by comma.
      --cluster=cluster_alias
                             The alias of the cluster to use to query masters
      --config-level=level   The config level of the dynamic configs
      --config-name=username The username of the TENANT_USER level.
      --config-tenant=tenant_id
                             The tenant id of TENANT or TENANT_USER level.
      --delete-apps          Delete resource of an application.
      --exclude-worker       Exclude workers by ID
  -h, --help                 Show this help message and exit.
      --host-list=h1,h2,h3...
                             List of hosts to pass to the command
      --hostport=host:port   The host and http port
      --remove-cluster-alias=alias
                             Remove alias to use in the cli for the given set
                               of masters
      --remove-excluded-worker
                             Remove excluded workers by ID
      --remove-workers-unavailable-info
                             Remove the workers unavailable info from the
                               master.
      --revise-lost-shuffles Revise lost shuffles or remove shuffles for an
                               application.
      --send-worker-event=IMMEDIATELY | DECOMMISSION | DECOMMISSION_THEN_IDLE | 
        GRACEFUL | RECOMMISSION | NONE
                             Send an event to a worker
      --show-cluster-apps    Show cluster applications
      --show-cluster-shuffles
                             Show cluster shuffles
      --show-conf            Show master conf
      --show-container-info  Show container info
      --show-decommissioning-workers
                             Show decommissioning workers
      --show-dynamic-conf    Show dynamic master conf
      --show-excluded-workers
                             Show excluded workers
      --show-lifecycle-managers
                             Show lifecycle managers
      --show-lost-workers    Show lost workers
      --show-manual-excluded-workers
                             Show manual excluded workers
      --show-masters-info    Show master group info
      --show-shutdown-workers
                             Show shutdown workers
      --show-thread-dump     Show master thread dump
      --show-worker-event-info
                             Show worker event information
      --show-workers         Show registered workers
      --show-workers-topology
                             Show registered workers topology
      --shuffleIds=<shuffleIds>
                             The shuffle ids to manipulate.
  -V, --version              Print version information and exit.
      --worker-ids=w1,w2,w3...
                             List of workerIds to pass to the command. Each
                               worker should be in the format host:rpcPort:
                               pushPort:fetchPort:replicatePort.
```

- worker commands:

```shell
$ celeborn-cli worker -h
Usage: celeborn-cli worker [-hV] [--apps=appId] [--cluster=cluster_alias]
                           [--config-level=level] [--config-name=username]
                           [--config-tenant=tenant_id] [--host-list=h1,h2,
                           h3...] [--hostport=host:port] [--worker-ids=w1,w2,
                           w3...] (--show-worker-info | --show-apps-on-worker |
                           --show-shuffles-on-worker |
                           --show-partition-location-info |
                           --show-unavailable-peers | --is-shutdown |
                           --is-decommissioning | --is-registered |
                           --exit=exit_type | --show-conf |
                           --show-container-info | --show-dynamic-conf |
                           --show-thread-dump)
      --apps=appId           The application Id list seperated by comma.
      --auth-header=authHeader
                             The http `Authorization` header for
                               authentication. It should be in the format of
                               `Bearer <token>` or `Basic
                               <base64-encoded-credentials>`.
      --cluster=cluster_alias
                             The alias of the cluster to use to query masters
      --config-level=level   The config level of the dynamic configs
      --config-name=username The username of the TENANT_USER level.
      --config-tenant=tenant_id
                             The tenant id of TENANT or TENANT_USER level.
      --exit=exit_type       Exit the application with a specified type
  -h, --help                 Show this help message and exit.
      --host-list=h1,h2,h3...
                             List of hosts to pass to the command
      --hostport=host:port   The host and http port
      --is-decommissioning   Check if the system is decommissioning
      --is-registered        Check if the system is registered
      --is-shutdown          Check if the system is shutdown
      --show-apps-on-worker  Show applications running on the worker
      --show-conf            Show worker conf
      --show-container-info  Show container info
      --show-dynamic-conf    Show dynamic worker conf
      --show-partition-location-info
                             Show partition location information
      --show-shuffles-on-worker
                             Show shuffles running on the worker
      --show-thread-dump     Show worker thread dump
      --show-unavailable-peers
                             Show unavailable peers
      --show-worker-info     Show worker info
  -V, --version              Print version information and exit.
      --worker-ids=w1,w2,w3...
                             List of workerIds to pass to the command. Each
                               worker should be in the format host:rpcPort:
                               pushPort:fetchPort:replicatePort.
```

## Ratis Shell

Celeborn CLI supports ratis shell with `celeborn-ratis` command to operate the master ratis service. Details of ratis shell refer to [Celeborn Ratis Shell](celeborn_ratis_shell.md).
