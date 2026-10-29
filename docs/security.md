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

## Configurable transport bootstraps

Transport bootstraps let a plugin perform an additional handshake when a connection opens.
Implement `org.apache.celeborn.common.network.client.TransportClientBootstrap` on the initiating
side and `org.apache.celeborn.common.network.server.TransportServerBootstrap` on the accepting side.
Celeborn's existing request authorization checks still apply after the handshake.

### Configuration and deployment

Compile the plugin against the matching Celeborn transport API and put its classes and dependencies
on the classpath of every participating process. Celeborn uses the thread context class loader.
Each class must implement the interface for its configured side and expose a public constructor
taking `TransportConf`, or a public no-argument constructor. The `TransportConf` constructor takes
precedence when both exist.

Shaded Spark and Flink clients relocate dependencies such as Netty, including types in bootstrap
method signatures. Build the plugin for the distribution that loads it, using matching relocated
types for a shaded client. Keep Celeborn API classes out of the plugin JAR so the host supplies them.

Configure comma-separated class names with these properties. Both default to an empty list:

| Property | Meaning |
| --- | --- |
| `celeborn.<module>.client.bootstrap.classes` | Client bootstraps, in handshake execution order. |
| `celeborn.<module>.server.bootstrap.classes` | Server bootstraps, in handshake execution order. |

Bootstrap lists apply only to the exact module name and do not inherit parent module settings.
Configure matching lists on both sides of each participating transport:

| Transport | Initiating side | Accepting side |
| --- | --- | --- |
| LifecycleManager RPC to Master/Worker, with `celeborn.auth.enabled=false` | `rpc_app_lifecyclemanager` client | `rpc_service` server |
| LifecycleManager RPC to Master/Worker, with `celeborn.auth.enabled=true` | `rpc_service` client | `rpc_service` server |
| Service RPC between Master/Worker processes | `rpc_service` client | `rpc_service` server |
| Shuffle push | Application `data` client | Worker `push` server |
| Shuffle fetch | Application `data` client | Worker `fetch` server |
| Worker replication | Worker `replicate` client | Worker `replicate` server |
| RPC within an application | The initiating process's `rpc_app_client` or `rpc_app_lifecyclemanager` client | The receiving process's `rpc_app_client` or `rpc_app_lifecyclemanager` server |

Public and internal Master/Worker RPC listeners both use `rpc_service`. Their bootstrap lists
cannot be configured separately, so a plugin must support the peers using both listeners.

For example, a plugin protecting shuffle data connections could use these settings.
Replace the `com.example.security` placeholders with the installed plugin class names:

```properties
# Application-side Celeborn configuration.
celeborn.data.client.bootstrap.classes com.example.security.TokenClientBootstrap

# Worker-side Celeborn configuration.
celeborn.push.server.bootstrap.classes com.example.security.TokenServerBootstrap
celeborn.fetch.server.bootstrap.classes com.example.security.TokenServerBootstrap
```

When supplying Celeborn settings through Spark configuration, prefix the names with `spark.`.
Configure RPC and replication separately if those transports use the plugin.

### Handshake order and handler obligations

For client and server lists `A,B`, a connection completes A's handshake, then B's. Configured client
bootstraps run before caller-supplied bootstraps. Configured server wrappers receive requests in
configuration order, before caller-supplied handlers. Caller-supplied lists retain their existing
ordering behavior.

Configured bootstraps run whether `celeborn.auth.enabled` is enabled or disabled. When native
authentication is enabled, configured handshakes precede Celeborn's registration or SASL handshake
on transports that use them. Each layer must finish its handshake before forwarding requests to
the next layer. TLS remains a separate transport setting.

`TransportClient.clientId` is reserved for the Celeborn application ID used by existing application
authorization checks, including `BaseMessageHandler.checkAuth()`. A configured bootstrap must leave
this field to native registration/SASL or establish the same application identity. Both layers can
set the same application ID; setting it to an independent plugin principal prevents later native
authentication with a different application ID. `setClientId()` rejects changing or clearing an
assigned identity.

Keep independent plugin principals and authentication-completion state in per-connection handler
fields or channel attributes, not in `clientId` or shared bootstrap fields. A non-null `clientId`
does not prove authentication succeeded: native SASL can assign it before credential validation
finishes. Each authentication layer must still gate requests on its own completed handshake.

The client bootstrap must finish authentication before `doBootstrap` returns and throw on failure.
A failed client handshake closes the connection and prevents it from being returned or pooled.
A server bootstrap must reject business requests until authentication completes, including one-way
and data requests. Its handler must preserve the delegate's registration checks and lifecycle
callbacks. If activation starts business traffic, defer `channelActive` until authentication
succeeds; continue forwarding `channelInactive` and `exceptionCaught`, including failed handshakes.
The plugin implements these server-side checks and callback rules.

### Instance lifecycle

Celeborn creates one instance per list entry and transport context, shared across that context's
connections. Configuring the same class on both sides creates separate instances. Implementations
must support concurrent connections and keep connection-specific state in separate handlers.
Clients can read refreshed credentials in `doBootstrap` for each new connection; credential changes
do not automatically reauthenticate existing connections.

The transport context closes configured instances once, in reverse construction order. Owners
close factories and servers before their context. Channel shutdown is asynchronous, so plugins
must coordinate in-flight callbacks with resource cleanup. Caller-supplied bootstrap instances
remain the caller's responsibility.

An invalid class, constructor failure, or class-initialization failure prevents context creation
and closes previously constructed instances. A constructor that fails must release its own
resources before throwing. If an instance's `close` throws an exception, Celeborn logs it and
continues closing the remaining instances.

Validate successful and failed handshakes, reconnection, native SASL composition, and shutdown
against the actual client and service JARs used in the deployment, including shaded clients.

