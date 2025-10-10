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

#include "celeborn/network/NettyRpcEndpointRef.h"

namespace celeborn {
namespace network {

NettyRpcEndpointRef::NettyRpcEndpointRef(
    const std::string& name,
    const std::string& srcHost,
    int srcPort,
    const std::string& dstHost,
    int dstPort,
    const std::shared_ptr<TransportClient>& client,
    const conf::CelebornConf& conf)
    : name_(name),
      srcHost_(srcHost),
      srcPort_(srcPort),
      dstHost_(dstHost),
      dstPort_(dstPort),
      client_(client),
      defaultTimeout_(conf.rpcAskTimeout()) {}
} // namespace network
} // namespace celeborn
