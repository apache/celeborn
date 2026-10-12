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

import java.net.{InetAddress, ServerSocket}

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.security.StartupCountingBootstrap
import org.apache.celeborn.common.rpc.RpcEnv

class RpcTransportStartupSuite extends CelebornFunSuite {
  private val module = "rpc_service"
  private val bootstrap = classOf[StartupCountingBootstrap].getName

  test("RPC factory initialization failure closes the configured bootstrap") {
    StartupCountingBootstrap.reset()
    val conf = configuration().set("celeborn.rpc_service.io.clientThreads", "-1")
    val failure = intercept[IllegalArgumentException] {
      RpcEnv.create("startup-factory-failure", module, "localhost", 0, conf, Role.CLIENT, None)
    }
    assert(failure.getMessage.contains("-1"))
    assertClosedOnce()
  }

  test("RPC context construction failure still closes earlier bootstrap instances") {
    StartupCountingBootstrap.reset()
    val conf = configuration()
      .set("celeborn.rpc_service.server.bootstrap.classes", classOf[String].getName)
    intercept[IllegalArgumentException] {
      RpcEnv.create("startup-context-failure", module, "localhost", 0, conf, Role.CLIENT, None)
    }
    assertClosedOnce()
  }

  test("RPC server bind failure does not close a configured bootstrap twice") {
    StartupCountingBootstrap.reset()
    val occupied = new ServerSocket(0, 50, InetAddress.getByName("localhost"))
    try {
      intercept[Exception] {
        RpcEnv.create(
          "startup-bind-failure",
          module,
          "localhost",
          occupied.getLocalPort,
          configuration(),
          Role.CLIENT,
          None)
      }
      assertClosedOnce()
    } finally {
      occupied.close()
    }
  }

  private def assertClosedOnce(): Unit = {
    val counts = StartupCountingBootstrap.closeCounts(module)
    assert(counts.size() == 1)
    assert(counts.values().asScala.forall(_ == 1), counts.toString)
  }

  private def configuration(): CelebornConf = new CelebornConf(false)
    .set("celeborn.auth.enabled", "false")
    .set("celeborn.metrics.enabled", "false")
    .set("celeborn.port.maxRetries", "0")
    .set("celeborn.rpc.dispatcher.threads", "2")
    .set("celeborn.rpc_service.io.clientThreads", "1")
    .set("celeborn.rpc_service.io.serverThreads", "1")
    .set("celeborn.rpc_service.client.bootstrap.classes", bootstrap)
}
