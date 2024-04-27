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
# Network encryption with TLS
When enabled, Celeborn leverages TLS to provide over the wire encryption.
Celeborn has different transport namespaces, and each can be independently configured for TLS - an exhaustive list of all configurations which apply to a transport module are listed in [network configurations](configuration/network.md), but the subset which are relevant to TLS are detailed below.
{!
include-markdown "./configuration/network-module.md"
start="<!--begin-include-->"
end="<!--end-include-->"
!}

The namespace `${ns}` to configure SSL configuration is `celeborn.ssl.<module>` from the list above.

Note that `celebord.ssl`, **without any module**, can be used to set SSL default values which apply to all modules.

Also note that at `data` module at application side, maps to `push` and `fetch` at worker side - hence, for SSL configuration, worker configuration for `push` and `fetch` should be compatible with each other and with `data` at application side.

The SSL options which are supported in Celeborn are detailed below. The `${ns}` placeholder should be replaced with one of the transport namespaces, as detailed above.

| Property Name | Default | Description |
| ------ | ------------- | ----------- |
| ${ns}.enabled | false | Enables SSL |
| ${ns}.enabledAlgorithms | Default cipher suite from the JRE | A comma-separated list of ciphers. The specified ciphers must be supported by JVM.<br/>The reference list of protocols can be found in the "JSSE Cipher Suite Names" section of the Java security guide. The list for Java 17 can be found at [this page](https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#jsse-cipher-suite-names)<br/>Note: If not set, the default cipher suite for the JRE will be used |
| ${ns}.keyStore | None | Path to the key store file.<br/> The path can be absolute or relative to the directory in which the process is started. |
| ${ns}.keyStorePassword | None | Password for the key store |
| ${ns}.protocol | TLSv1.2 | TLS protocol to use.<br/> The protocol must be supported by JVM.<br/> The reference list of protocols can be found in the "Additional JSSE Standard Names" section of the Java security guide. For Java 17, the list can be found [here](https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#additional-jsse-standard-names) |
| ${ns}.trustStore | None | Path to the trust store file.<br/> The path can be absolute or relative to the directory in which the process is started.|
| ${ns}.trustStorePassword | None | Password for the trust store |
| ${ns}.trustStoreReloadingEnabled | false | Whether the trust store should be reloaded periodically. <br/> his setting is mostly only useful for Celeborn services (masters, workers), and not applications. |
| ${ns}.trustStoreReloadIntervalMs | 10s | The interval at which the trust store should be reloaded (in milliseconds) - when enabled. |
| ${ns}.autoSslEnabled | false | Enable auto ssl for encrypted communication between lifecyclemanager and executors. <br/><br/> This is applicable only for `rpc_app` module and ignored for others. Additionally if truststore or keystore are present, this config is ignored. <br/> <br/>Lifecyclemanager generates a self-signed certificate, which is used for SSL. Given use of self-signed certificate, auto ssl only provides over the wire encryption |


## Example configuration

### Master/Worker configuration for enabling TLS

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

### Application configuration for enabling TLS

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


# Authentication

Celeborn supports authentication to prevent unauthorized access or modifications to an application's data. 
When enabled, lifecyclemanager registers the application with Celeborn service, and negotiates a shared secret.
All further connections, from the application to Celeborn servers, will first be authenticated - and only an authorized connection can read/modify data for a registered application.

The `shared secret`, which is generated as part of application registration, is used to authenticate all subsequent connections.
Even though Celeborn does not transmit the secret, in the clear, as part of authentication - it still sends it in the clear during registration - and so enabling TLS for `rpc_service` and `rpc_app` transport modules is recommended (see above on how).  

Note: SASL **requires use of internal port**.

An exhaustive list of all network configurations is documented [here](configuration/network.md), but the subset which are relevant to authentication are captured below.

| Property Name | Default | Description |
| ------ | ------------- | ----------- |
| celeborn.auth.enabled | false | Enables Authentication |
| celeborn.internal.port.enabled | false | Enable internal port for Celeborn services. This **must be** enabled when authentication is enabled.<br/>nly server components communicate with each other on the internal port, while applications continue to use the regular ports. |
| celeborn.master.internal.endpoints | None | Analogous to `celeborn.master.endpoints`, but with internal ports instead. |
| celeborn.master.ha.node.&lt;id&gt;.internal.port  | None | Analogous to `celeborn.master.ha.node.<id>.port`, but for internal ports instead |


### Master/Worker configuration for enabling authentication

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

### Application configuration for enabling Authentication

```properties
spark.celeborn.auth.enabled             true
```

