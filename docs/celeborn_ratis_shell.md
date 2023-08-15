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

# Celeborn Ratis-shell

[Ratis-shell](https://github.com/apache/ratis/blob/master/ratis-docs/src/site/markdown/cli.md) is the command line interface of Ratis.
Celeborn uses Ratis to implement the HA function of the master, Celeborn directly introduces ratis-shell package into the project
then it's convenient for Celeborn Admin to operate the master ratis service.

> **Note**:
> Ratis-shell is currently only **experimental**.
> The compatibility story is not considered for the time being.

## Availability
|  Version  | Available in src tarball? | Available in bin tarball? |
|:---------:| :-----------------------: |:-------------------------:|
|  < 0.3.0  | No                        |            No             |
| \>= 0.3.0 | Yes                       |            Yes            |

## Setting up the Celeborn ratis-shell

Celeborn directly introduces the ratis-shell into the project, users don't need to set up ratis-shell env from ratis repo.
User can directly download the Celeborn source tarball from [Download](https://celeborn.apache.org/download) and
build the Celeborn according to [build_and_test](https://celeborn.apache.org/community/contributor_guide/build_and_test/)
or just download the pre-built binary tarball from [Download](https://celeborn.apache.org/download)
to get the binary package `apache-celeborn-<VERSION>-bin.tgz`.

After getting the binary package `apache-celeborn-<VERSION>-bin.tgz`:
```
$ tar -C <DST_DIR> -zxvf apache-celeborn-<VERSION>-bin.tgz
$ ln -s <DST_DIR>/apache-celeborn-<VERSION>-bin <DST_DIR>/celeborn
```

Export the following environment variable and add the bin directory to the `$PATH`.
```
$ export CELEBORN_HOME=<DST_DIR>/celeborn
$ export PATH=${CELEBORN_HOME}/bin:$PATH
```

The following command can be invoked in order to get the basic usage:

```shell
$ celeborn-ratis sh
Usage: celeborn-ratis sh [generic options]
         [election [transfer] [stepDown] [pause] [resume]]
         [group [info] [list]]
         [peer [add] [remove] [setPriority]]
         [snapshot [create]]
```

## generic options
The `generic options` pass values for a given ratis-shell property.
It supports the following content:
`-D*`, `-X*`, `-agentlib*`, `-javaagent*`

```
$ celeborn-ratis sh -D<property=value> ...
```

**Note:**

Celeborn HA uses `NETTY` as the default RPC type, for details please refer to configuration `celeborn.master.ha.ratis.raft.rpc.type`. But Ratis uses `GRPC` as the default RPC type. So if the user wants to use Ratis shell to access Ratis cluster which uses `NETTY` RPC type, the generic option `-Draft.rpc.type=NETTY` should be set to change the RPC type of Ratis shell to Netty.

## election
The `election` command manages leader election.
It has the following subcommands:
`transfer`, `stepDown`, `pause`, `resume`

### election transfer
Transfer a group leader to the specified server.
```
$ celeborn-ratis sh election transfer -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```

### election stepDown
Make a group leader of the given group step down its leadership.
```
$ celeborn-ratis sh election stepDown -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]
```

### election pause
Pause leader election at the specified server.
Then, the specified server would not start a leader election.
```
$ celeborn-ratis sh election pause -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```

### election resume
Resume leader election at the specified server.
```
$ celeborn-ratis sh election resume -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```

## group
The `group` command manages ratis groups.
It has the following subcommands:
`info`, `list`

### group info
Display the information of a specific raft group.
```
$ celeborn-ratis sh group info -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]
```

### group list
Display the group information of a specific raft server
```
$ celeborn-ratis sh group list -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]  <[-serverAddress <P0_HOST:P0_PORT>]|[-peerId <peerId0>]>
```

## peer
The `peer` command manages ratis cluster peers.
It has the following subcommands:
`add`, `remove`, `setPriority`

### peer add
Add peers to a ratis group.
```
$ celeborn-ratis sh peer add -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -address <P4_HOST:P4_PORT,...,PN_HOST:PN_PORT>
```

### peer remove
Remove peers to from a ratis group.
```
$ celeborn-ratis sh peer remove -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -address <P0_HOST:P0_PORT,...>
```

### peer setPriority
Set priority to ratis peers.
The priority of ratis peer can affect the leader election, the server with the highest priority will eventually become the leader of the cluster.
```
$ celeborn-ratis sh peer setPriority -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -addressPriority <P0_HOST:P0_PORT|PRIORITY>
```
## snapshot
The `snapshot` command manages ratis snapshot.
It has the following subcommands:
`create`

### snapshot create
Trigger the specified server take snapshot.
```
$ celeborn-ratis sh snapshot create -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -peerId <peerId0> [-groupid <RAFT_GROUP_ID>]
```
