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

import java.util.concurrent.ExecutionException

import scala.concurrent.duration._

import org.mockito.Mockito.mock
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.ThreadUtils

class NettyRpcEnvSuite extends RpcEnvSuite with TimeLimits {

  implicit private val signaler: Signaler = ThreadSignaler

  override def createRpcEnv(
      conf: CelebornConf,
      name: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port, 0)
    new NettyRpcEnvFactory().create(config)
  }

  test("non-existent endpoint") {
    val uri = RpcEndpointAddress(env.address, "nonexist-endpoint").toString
    val e = intercept[CelebornException] {
      env.setupEndpointRef(env.address, "nonexist-endpoint")
    }
    assert(e.getCause.isInstanceOf[RpcEndpointNotFoundException])
    assert(e.getCause.getMessage.contains(uri))
  }

  test("advertise address different from bind address") {
    val celebornConf = createCelebornConf()
    val config = RpcEnvConfig(celebornConf, "test", "localhost", "example.com", 0, 0)
    val env = new NettyRpcEnvFactory().create(config)
    try {
      assert(env.address.hostPort.startsWith("example.com:"))
    } finally {
      env.shutdown()
    }
  }

  test("RequestMessage serialization") {
    def assertRequestMessageEquals(expected: RequestMessage, actual: RequestMessage): Unit = {
      assert(expected.senderAddress === actual.senderAddress)
      assert(expected.receiver === actual.receiver)
      assert(expected.content === actual.content)
    }

    val nettyEnv = env.asInstanceOf[NettyRpcEnv]
    val client = mock(classOf[TransportClient])
    val senderAddress = RpcAddress("localhost", 12345)
    val receiverAddress = RpcEndpointAddress("localhost", 54321, "test")
    val receiver = new NettyRpcEndpointRef(nettyEnv.celebornConf, receiverAddress, nettyEnv)

    val msg = new RequestMessage(senderAddress, receiver, "foo")
    assertRequestMessageEquals(
      msg,
      RequestMessage(nettyEnv, client, msg.serialize(nettyEnv)))

    val msg2 = new RequestMessage(null, receiver, "foo")
    assertRequestMessageEquals(
      msg2,
      RequestMessage(nettyEnv, client, msg2.serialize(nettyEnv)))

    val msg3 = new RequestMessage(senderAddress, receiver, null)
    assertRequestMessageEquals(
      msg3,
      RequestMessage(nettyEnv, client, msg3.serialize(nettyEnv)))
  }

  test("StackOverflowError should be sent back and Dispatcher should survive") {
    val numUsableCores = 2
    val conf = createCelebornConf()
    val config = RpcEnvConfig(
      conf,
      "test",
      "localhost",
      "localhost",
      0,
      numUsableCores)
    val anotherEnv = new NettyRpcEnvFactory().create(config)
    anotherEnv.setupEndpoint(
      "StackOverflowError",
      new RpcEndpoint {
        override val rpcEnv = anotherEnv

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          // scalastyle:off throwerror
          case msg: String => throw new StackOverflowError
          // scalastyle:on throwerror
          case num: Int => context.reply(num)
        }
      })

    val rpcEndpointRef = env.setupEndpointRef(anotherEnv.address, "StackOverflowError")
    try {
      // Send `numUsableCores` messages to trigger `numUsableCores` `StackOverflowError`s
      for (_ <- 0 until numUsableCores) {
        val e = intercept[CelebornException] {
          rpcEndpointRef.askSync[String]("hello")
        }
        // The root cause `e.getCause.getCause` because it is boxed by Scala Promise.
        assert(e.getCause.isInstanceOf[ExecutionException])
        assert(e.getCause.getCause.isInstanceOf[StackOverflowError])
      }
      failAfter(10.seconds) {
        assert(rpcEndpointRef.askSync[Int](100) === 100)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }
}
