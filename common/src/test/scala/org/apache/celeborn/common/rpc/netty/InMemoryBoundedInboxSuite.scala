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

package org.apache.celeborn.common.rpc.netty

import org.mockito.Mockito.mock

import org.apache.celeborn.common.rpc.TestRpcEndpoint

class InMemoryBoundedInboxSuite extends InboxSuite {

  override def initInbox[T](
     testRpcEndpoint: TestRpcEndpoint,
     onDropOverride: Option[InboxMessage => T]): InboxBase = {
    val rpcEnvRef = mock(classOf[NettyRpcEndpointRef])
    if (onDropOverride.isEmpty) {
      new InMemoryBoundedInbox(rpcEnvRef, testRpcEndpoint, 10000)
    } else {
      new InMemoryBoundedInbox(rpcEnvRef, testRpcEndpoint, 10000) {
        override def onDrop(message: InboxMessage): Unit = {
          onDropOverride.get(message)
        }
      }
    }
  }
}