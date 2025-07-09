---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
# Introduction
Celeborn supports both SASL (Simple Authentication and Security Layer) based authentication and TLS (Transport Layer Security) based over the wire encryption.  Both are disabled by default, and require to be explicitly enabled.

Celeborn can use TLS to encrypt data transmitted over the network, provide privacy and data integrity. It also facilitates validating the identity of the server to mitigate the risk of man-in-the-middle attacks and ensure trusted communication.

SASL is leveraged by Celeborn to authenticate requests from an application - it ensures clients can only mutate or access state and data that belongs to them.

Note: **SSL** and **TLS** are used interchangeably in this document.
## Network encryption with TLS
When enabled, Celeborn leverages TLS to provide over the wire encryption.
Celeborn has different transport namespaces, and each can be independently configured for TLS.
The full list of all configurations which apply for ssl are listed in [network configurations](configuration/network.md) - and namespaced under `celeborn.ssl`.
 
{!
include-markdown "./configuration/network-module.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

When SSL is enabled for `rpc_service`, Raft communication between masters are secured **only when** `celeborn.master.ha.ratis.raft.rpc.type` is set to `grpc`.

Note that `celeborn.ssl`, **without any module**, can be used to set SSL default values which applies to all modules.

Also note that `data` module at application side, maps to `push` and `fetch` at worker - hence, for SSL configuration, worker configuration for `push` and `fetch` should be compatible with each other and with `data` at application side.

### Example configuration

#### Master/Worker configuration for TLS

```properties

# TLS configuration
celeborn.ssl.rpc_service.enabled                true
# Location of the java keystore which contains the private key and certificate for the master/worker.
celeborn.ssl.rpc_service.keyStore               /mnt/disk1/celeborn/conf/ssl/server.jks
celeborn.ssl.rpc_service.keyStorePassword       password
# Location of the java truststore which contains the CA certs which can validate the master/worker certificate 
celeborn.ssl.rpc_service.trustStore             /mnt/disk1/celeborn/conf/ssl/truststore.jks
celeborn.ssl.rpc_service.trustStorePassword     changeit
```

#### Application configuration for TLS

```properties


# TLS

# Configure rpc_app to enable ssl between lifecyclemanager and executors
spark.celeborn.ssl.rpc_app.enabled                              true
# Use auto ssl to generate a self-signed certificate at lifecyclemanager, to secure network communication.
spark.celeborn.ssl.rpc_app.autoSslEnabled                       true

# Secure communication with celeborn servers
spark.celeborn.ssl.rpc_service.enabled                          true
# trust store with CA certs, to verify the celeborn service certificate   
spark.celeborn.ssl.rpc_service.trustStore                       /etc/ssl/certs/truststore.jks
spark.celeborn.ssl.rpc_service.trustStorePassword               changeit
```


## Authentication

Celeborn supports authentication to prevent unauthorized access or modifications to an application's data. 
When enabled, lifecyclemanager registers the application with Celeborn service, and negotiates a shared secret.
All further connections, from the application to Celeborn servers, will first be authenticated - and only an authorized connection can read/modify data for a registered application.

The `shared secret`, which is generated as part of application registration, is used to authenticate all subsequent connections.
Even though Celeborn does not transmit the secret, in the clear, as part of authentication - it still sends it in the clear during registration - and so enabling TLS for `rpc_service` and `rpc_app` transport modules is recommended (see above on how).  

Note: SASL **requires use of internal port**.

| Property Name | Default | Description |
| ------ | ------------- | ----------- |
| celeborn.auth.enabled | false | Enables Authentication |
| celeborn.internal.port.enabled | false | Enable internal port for Celeborn services. This **must be** enabled when authentication is enabled.<br/>Only server components communicate with each other on the internal port, while applications continue to use the regular ports. |
| celeborn.master.internal.endpoints | None | Analogous to `celeborn.master.endpoints`, but with internal ports instead. |
| celeborn.master.ha.node.&lt;id&gt;.internal.port  | None | Analogous to `celeborn.master.ha.node.<id>.port`, but for internal ports instead |

### Example configuration

#### Master/Worker configuration for authentication

```properties

# Enable authentication
celeborn.auth.enabled                   true

# Rest of the changes are to enable internal port

# Enable internal port
celeborn.internal.port.enabled          true

celeborn.master.endpoints               clb-1:9097,clb-2:9097,clb-3:9097
# Configure internal master endpoint, in addition to master endpoint
celeborn.master.internal.endpoints      clb-1:19097,clb-2:19097,clb-3:19097

celeborn.master.host                    clb-master
celeborn.master.port                    9097
# internal port, matches internal endpoint above
celeborn.master.internal.port           19097


celeborn.master.ha.enabled              true
celeborn.master.ha.node.id              1

celeborn.master.ha.node.1.host          clb-1
celeborn.master.ha.node.1.port          9097
# Ensure that internal.port is configured for HA mode as well
celeborn.master.ha.node.1.internal.port 19097
celeborn.master.ha.node.1.ratis.port    9872
celeborn.master.ha.node.2.host          clb-2
celeborn.master.ha.node.2.port          9097
celeborn.master.ha.node.2.internal.port 19097
celeborn.master.ha.node.2.ratis.port    9872
celeborn.master.ha.node.3.host          clb-3
celeborn.master.ha.node.3.port          9097
celeborn.master.ha.node.3.internal.port 19097
celeborn.master.ha.node.3.ratis.port    9872

```

#### Application configuration for Authentication

```properties
spark.celeborn.auth.enabled             true
```

