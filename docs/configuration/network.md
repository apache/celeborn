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
| Key | Default | Description | Since |
| --- | ------- | ----------- | ----- |
| celeborn.&lt;module&gt;.decoder.mode | default | Netty TransportFrameDecoder implementation, available options: default, supplier. |  | 
| celeborn.&lt;module&gt;.io.backLog | 0 | Requested maximum length of the queue of incoming connections. Default 0 for no backlog. |  | 
| celeborn.&lt;module&gt;.io.clientThreads | 0 | Number of threads used in the client thread pool. Default to 0, which is 2x#cores. |  | 
| celeborn.&lt;module&gt;.io.connectTimeout | &lt;value of celeborn.network.connect.timeout&gt; | Socket connect timeout. |  | 
| celeborn.&lt;module&gt;.io.connectionTimeout | &lt;value of celeborn.network.timeout&gt; | Connection active timeout. |  | 
| celeborn.&lt;module&gt;.io.enableVerboseMetrics | false | Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked. |  | 
| celeborn.&lt;module&gt;.io.lazyFD | true | Whether to initialize FileDescriptor lazily or not. If true, file descriptors are created only when data is going to be transferred. This can reduce the number of open files. |  | 
| celeborn.&lt;module&gt;.io.maxRetries | 3 | Max number of times we will try IO exceptions (such as connection timeouts) per request. If set to 0, we will not do any retries. |  | 
| celeborn.&lt;module&gt;.io.mode | NIO | Netty EventLoopGroup backend, available options: NIO, EPOLL. |  | 
| celeborn.&lt;module&gt;.io.numConnectionsPerPeer | 2 | Number of concurrent connections between two nodes. |  | 
| celeborn.&lt;module&gt;.io.preferDirectBufs | true | If true, we will prefer allocating off-heap byte buffers within Netty. |  | 
| celeborn.&lt;module&gt;.io.receiveBuffer | 0b | Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps buffer size should be ~ 1.25MB. | 0.2.0 | 
| celeborn.&lt;module&gt;.io.retryWait | 5s | Time that we will wait in order to perform a retry after an IOException. Only relevant if maxIORetries > 0. | 0.2.0 | 
| celeborn.&lt;module&gt;.io.sendBuffer | 0b | Send buffer size (SO_SNDBUF). | 0.2.0 | 
| celeborn.&lt;module&gt;.io.serverThreads | 0 | Number of threads used in the server thread pool. Default to 0, which is 2x#cores. |  | 
| celeborn.network.connect.timeout | 10s | Default socket connect timeout. | 0.2.0 | 
| celeborn.network.timeout | 240s | Default timeout for network operations. | 0.2.0 | 
| celeborn.port.maxRetries | 1 | When port is occupied, we will retry for max retry times. | 0.2.0 | 
| celeborn.rpc.askTimeout | &lt;value of celeborn.network.timeout&gt; | Timeout for RPC ask operations. | 0.2.0 | 
| celeborn.rpc.connect.threads | 64 |  | 0.2.0 | 
| celeborn.rpc.haClient.askTimeout | &lt;value of celeborn.network.timeout&gt; | Timeout for HA client RPC ask operations. | 0.2.0 | 
| celeborn.rpc.lookupTimeout | 30s | Timeout for RPC lookup operations. | 0.2.0 | 
| celeborn.shuffle.maxChunksBeingTransferred | 9223372036854775807 | The max number of chunks allowed to be transferred at the same time on shuffle service. Note that new incoming connections will be closed when the max number is hit. The client will retry according to the shuffle retry configs (see `celeborn.shuffle.io.maxRetries` and `celeborn.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch failure. | 0.2.0 | 
<!--end-include-->
