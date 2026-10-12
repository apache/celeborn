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

## Custom transport authentication

Transport bootstraps let an external JAR authenticate connections and install a policy for
application and service operations. Implement `TransportClientBootstrap` on the initiating side
and `TransportServerBootstrap` on the accepting side. Their full class names are
`org.apache.celeborn.common.network.client.TransportClientBootstrap` and
`org.apache.celeborn.common.network.server.TransportServerBootstrap`.

The accepting bootstrap binds the identity established by its authentication mechanism to a
`org.apache.celeborn.common.network.security.ConnectionSecurityContext`. Celeborn supplies each
operation and its resource to that context before executing the request. An external handshake
can use existing RPC request/response frames without changing Celeborn's transport wire format.

### Configuration and deployment

Compile the plugin against the matching Celeborn transport API and make its classes and dependencies
available on the classpath of every participating service or application process. Celeborn loads
configured classes using the thread context class loader. Each class must implement the interface
for its configured side and expose a public constructor taking `TransportConf`, or a public
no-argument constructor. The `TransportConf` constructor takes precedence when both exist.

Build the plugin for the distribution that loads it. Shaded Spark and Flink clients relocate
transport dependencies such as Netty, including types in bootstrap method signatures. A plugin for
such a client must use the matching relocated types; a plugin compiled for an unshaded service API
is not automatically compatible with a shaded client. Keep Celeborn API classes out of the plugin
JAR so that the host supplies them.

Configure comma-separated class names with these properties. Both default to an empty list:

| Property | Meaning |
| --- | --- |
| `celeborn.<module>.client.bootstrap.classes` | Client bootstraps, in handshake execution order. |
| `celeborn.<module>.server.bootstrap.classes` | Server bootstraps, in handshake execution order. |

These properties apply to the **exact module name**. Unlike transport settings that support parent
fallback, bootstrap lists do not inherit from `rpc`, `rpc_app`, or another module. Configure each
participating transport explicitly:

| Transport | Initiating side | Accepting side |
| --- | --- | --- |
| LifecycleManager RPC to Master/Worker, with `celeborn.auth.enabled=false` | `rpc_app_lifecyclemanager` client | `rpc_service` server |
| LifecycleManager RPC to Master/Worker, with `celeborn.auth.enabled=true` | `rpc_service` client | `rpc_service` server |
| Service RPC between Master/Worker processes | `rpc_service` client | `rpc_service` server |
| Shuffle push | Application `data` client | Worker `push` server |
| Shuffle fetch | Application `data` client | Worker `fetch` server |
| Worker replication | Worker `replicate` client | Worker `replicate` server |
| RPC within an application | `rpc_app_client` or `rpc_app_lifecyclemanager`, according to the initiating process | The receiving process's exact `rpc_app_client` or `rpc_app_lifecyclemanager` module |

Public and internal Master/Worker RPC listeners both use `rpc_service`. The bootstrap properties
cannot select different plugin lists for those listeners. The native SASL distinction between
public and internal listeners does not provide independent plugin configuration. A deployment
using both listeners must use a plugin that supports the peers on both.

For example, a plugin protecting shuffle data connections could use the following configuration.
The `com.example.security` names are placeholders for the installed plugin classes.

```properties
# Application-side Celeborn configuration.
celeborn.data.client.bootstrap.classes com.example.security.TokenClientBootstrap

# Worker-side Celeborn configuration.
celeborn.push.server.bootstrap.classes com.example.security.TokenServerBootstrap
celeborn.fetch.server.bootstrap.classes com.example.security.TokenServerBootstrap
```

When supplying Celeborn settings through Spark configuration, prefix the property names with
`spark.`, as with the built-in authentication settings. RPC and replication require their own
matching lists when those transports participate in the authentication mechanism.

### Ordering and composition

For client and server lists `A,B`, a connection completes A's handshake, then B's. Configured client
bootstraps run before caller-supplied bootstraps. Configured server handlers receive requests before
caller-supplied handlers; their wrappers are installed in reverse configuration order to preserve
the listed handshake order. Caller-supplied lists retain their existing ordering behavior.

Configured bootstraps run whether `celeborn.auth.enabled` is enabled or disabled. When native
authentication is enabled, the configured handshakes precede Celeborn's registration or SASL
handshake on transports that use them. Each configured layer must complete before forwarding
requests to the next layer. A failed client handshake prevents that connection from being returned
to the caller or added to the connection pool. TLS remains a separate transport setting.

### Connection identity and authorization

After authenticating its peer, a bootstrap calls `client.setSecurityContext(context)` before
releasing business traffic. The context captures the mechanism's authenticated identity and policy;
Celeborn does not require a particular principal class or identity hierarchy. Installation succeeds
only once per `TransportClient`, including when a caller tries to reinstall the same object.
`client.getSecurityContext()` returns the installed context, or `null` when none is installed.
The context must support concurrent authorization calls and keep its bound identity unchanged.

Keep the plugin's verified principal, tenant, or session in its `ConnectionSecurityContext`
implementation. `TransportClient.clientId` is the native application binding used by registration
and SASL. A plugin need not populate it unless it deliberately maps its identity to a native
application. Neither the presence nor the absence of `clientId` indicates whether a plugin has
completed authentication.

When several authentication layers share a connection, designate one layer as the context owner.
The layers coordinate their evidence and policy through that owner. Each layer must still finish
its handshake before forwarding business traffic. Installing a context does not complete another
layer's handshake or authorize it to replace the installed context.

`ConnectionSecurityContext.authorize(AuthorizationRequest)` permits a request by returning normally
and rejects it by throwing `SecurityException`. Reject unrecognized operations as well as denied
ones. `AuthorizationRequest` and `SecurityOperation` are in
`org.apache.celeborn.common.network.security`. The request exposes these fields:

| Getter | Meaning |
| --- | --- |
| `getOperation()` | Operation selected by the receiving handler. `SecurityOperation` defines Celeborn's names, such as `REQUEST_SLOTS`, `GET_APPLICATION_META`, and `PUSH_DATA`. |
| `getScope()` | `APPLICATION` for access to application resources, or `SERVICE` for service operations. |
| `getApplicationId()` | Target application when the operation identifies one; otherwise `null`. |
| `getUserIdentifier()` | User used for authorization, ownership, or quota after the context resolves the request's claimed user. May be `null`. |

Scope describes the requested access. It does not prove the peer's identity or role. For example,
replica push uses `SERVICE` scope and carries the target application ID. The policy must verify
that the authenticated peer may replicate that application's data. A port number or a request's
replication mode is not evidence of service identity. A plugin can use different rules for service
and application access to the same application, including metadata lookup.

The policy receives application and operation information, but no stream ID or stream-creation
channel. This interface supports application-level access decisions; it cannot express a rule
that requires every stream control request to come from the stream's original connection.

Operation names are strings so private endpoints can define their own names. Such endpoints must
construct the authorization request on the receiving side and call the policy before performing
the operation. A plugin must explicitly support each additional operation it intends to allow.

#### Resolving the effective user

`TransportClient.authorize(request)` reads the installed context once and calls
`resolveUserIdentifier(applicationId, claimed)` once. It then passes a request containing that
resolved user to `authorize` and returns the same user to the receiving handler. Handlers use this
returned value for ownership and quota decisions. They must not resolve it again or execute with
the original claim after authorizing a different user.

The default `resolveUserIdentifier` returns the claimed `UserIdentifier` unchanged. A mechanism
that establishes an accounting identity should override this method to select or validate the user
from its bound session. Each field is only as trustworthy as the mechanism's checks; installing a
context does not make every user field cryptographically verified. A non-null user claim cannot be
resolved to `null`.

Some existing requests contain no application ID. `CHECK_QUOTA` contains a user, while
`CHECK_WORKERS_AVAILABLE` contains neither an application ID nor a user. Policies must handle
these known operations with the resource information available to them. For example, a custom
quota handler obtains the effective user with this call, then uses `effectiveUser` for its lookup:

```java
UserIdentifier effectiveUser = client.authorize(
    AuthorizationRequest.forApplication(SecurityOperation.CHECK_QUOTA, null, claimedUser));
```

Here `client` is the receiving `TransportClient`, and `claimedUser` comes from the request.
`UserIdentifier` is in `org.apache.celeborn.common.identity`; the authorization types are in
`org.apache.celeborn.common.network.security`.

#### Native policy and explicit composition

Without an installed context, Celeborn applies its native policy. When native SASL has bound an
application ID, application access must target that ID whenever the request has an application
target. Service operations reject such an application connection, except `GET_APPLICATION_META`,
which retains native access to the same application's metadata. Connections without a native
application ID retain the legacy behavior used by internal service channels and deployments
without authentication. The native policy keeps claimed user identifiers unchanged.

Installing a context replaces this native authorization policy. A plugin that also needs the
native restrictions can call `client.checkNativeAuthorization(request)` from its own policy before
applying its additional checks. This composition is explicit: native exact-application rules can
conflict with a plugin that grants access across applications. Native registration and SASL
handshakes remain required wherever configured, independently of the authorization policy.

If a deployment combines a plugin identity with a native application identity, its policy must
define which associations are allowed. For example, a verified tenant may access several
application IDs, but must not gain access to an application in another tenant merely by completing
native SASL. The framework does not infer that relationship from either identity.

#### Request entry points

RPC dispatch calls `RpcEndpoint.authorize(context, message)` before either `receiveAndReply` for
request/reply calls or `receive` for one-way calls. The default hook returns the original message
without checking a policy. Celeborn's Master, Worker, and LifecycleManager endpoints implement the
operation mapping and checks; custom endpoints must implement their own. An endpoint can return a
message with its user claim replaced by the effective user; that returned message reaches the
business handler. Forwarding endpoints must preserve the original `RpcRequestContext` when
delegating authorization.

The receiving RPC framework determines whether a request is local and retains the transport
connection for remote requests. A sender address inside a serialized message cannot make a remote
request local. Internal actions can require this local origin through `context.requireLocal()`.
A remote context without its transport connection cannot authorize a request.

Worker data handlers authorize push, stream-open, and subsequent stream operations before their
business work. For a request that carries only a stream ID, the target application comes from
server-held stream state established when that stream was opened. Credit control requests use the
current connection's policy for that application. An authorized control request from another
connection does not move the stream's data delivery or disconnect cleanup to that connection.

A rejected request/reply RPC receives a failure response. One-way RPCs have no response callback;
authorization failures follow the endpoint's error handling path. Raw one-way credit controls also
have no response callback: authorization failure prevents the state change and is logged by the
transport, without closing the shared connection. Their sender receives no immediate denial
notification.

### Implementation and lifecycle

A configured instance is created once per list entry and transport context, and shared across that
context's connections. Configuring the same class on both sides creates two instances. Implementations
must support concurrent connections and keep connection-specific authentication state in a separate
handler. The client can read refreshed credentials in `doBootstrap` for each new connection;
credential changes do not automatically reauthenticate an existing connection.

The client bootstrap must finish authentication before `doBootstrap` returns and throw on failure.
A server bootstrap must reject business requests until authentication completes, including
one-way and data requests. `AbstractAuthRpcHandler` provides an authentication gate for both
`receive` overloads. A wrapper must preserve the delegate's registration checks and lifecycle
callbacks. If activation starts business traffic, defer `channelActive` until authentication succeeds;
continue forwarding `channelInactive` and `exceptionCaught`, including failed handshakes.

The transport context owns configured instances and closes them once, in reverse construction
order. Owners close factories and servers before closing their context. Shutdown may overlap
connection callbacks because channel shutdown is asynchronous; plugins must coordinate their own
in-flight work with resource cleanup. Caller-supplied bootstrap instances remain the caller's
responsibility.

An invalid class, constructor failure, or class-initialization failure prevents context creation and
closes the configured instances that were already constructed. A plugin whose own constructor fails
must release any resources it acquired before throwing. An exception from closing a configured
instance is logged and does not prevent the remaining instances from being closed.

### Validating a deployment

Validate the complete shuffle path with the plugin enabled: reserve storage, push ordinary and
merged data, replicate to another Worker, finish the mapper, and read the data back. Check the
stored user as well as the returned bytes. Test a failed handshake separately from a successfully
authenticated peer whose application access is denied; a timeout does not establish that the
intended check rejected the request. Test the plugin against the actual packaged client JAR used
by the application, including shaded distributions.

Measure authentication and authorization separately. Bootstraps authenticate new connections;
request handlers authorize operations on reused connections, including subsequent chunk and
credit requests. A cold-connection measurement captures handshake cost. A warmed shuffle
measurement captures request authorization together with I/O and other processing. Compare the
same workload, concurrency, replication, and client configuration with authentication disabled,
with native authentication, and with the configured plugin.

Use end-to-end shuffle time and process CPU per logical GiB to assess workload impact. Worker
handler timers start after authorization on these paths, so those timers alone cannot measure
its overhead. Their request-ID keys can also collide across client JVMs; timer counts are not an
exact RPC count. Record the tested build, plugin, workload, and environment with the results;
passing functional tests does not establish a performance bound.
