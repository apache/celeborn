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

package org.apache.celeborn.client

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration._

import org.mockito.Mockito.{mock, when}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.message.{Message, StatusCode}
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, ApplicationLostResponse, CheckQuotaResponse, HeartbeatFromApplication, HeartbeatFromApplicationResponse}
import org.apache.celeborn.common.rpc.RpcEnv

class ApplicationHeartbeaterSuite extends CelebornFunSuite {

  test("stop should not block indefinitely on unregister") {
    val conf = new CelebornConf(false)
      .set("celeborn.master.endpoints", "host1:9097")
      .set("celeborn.client.application.heartbeatInterval", "1h")
      .set("celeborn.client.application.unregister.timeout", "200ms")

    val workerStatusTracker = mock(classOf[WorkerStatusTracker])
    when(workerStatusTracker.getNeedCheckedWorkers()).thenReturn(Set.empty[WorkerInfo])

    val registeredShuffles = ConcurrentHashMap.newKeySet[Int]()
    val masterClient = new BlockingMasterClient(conf)
    val heartbeater = new ApplicationHeartbeater(
      "application-1",
      conf,
      masterClient,
      () =>
        ((0L, 0L), (0L, 0L, Map.empty[String, java.lang.Long], Map.empty[String, java.lang.Long])),
      workerStatusTracker,
      registeredShuffles,
      _ => ())

    heartbeater.start()
    Thread.sleep(50)

    val startNs = System.nanoTime()
    heartbeater.stop()
    val elapsed = (System.nanoTime() - startNs).nanos

    assert(masterClient.unregisterCalls == 1)
    assert(elapsed < 3.seconds, s"stop should return quickly, but took $elapsed")
  }

  private class BlockingMasterClient(conf: CelebornConf)
    extends MasterClient(mock(classOf[RpcEnv]), conf, false) {

    @volatile var unregisterCalls = 0

    override def askSync[T](message: Message, clz: Class[T]): T = {
      message match {
        case _: HeartbeatFromApplication =>
          HeartbeatFromApplicationResponse(
            StatusCode.SUCCESS,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            CheckQuotaResponse(isAvailable = true, ""))
            .asInstanceOf[T]
        case _: ApplicationLost =>
          unregisterCalls += 1
          Thread.sleep(5000)
          ApplicationLostResponse(StatusCode.SUCCESS).asInstanceOf[T]
        case _ =>
          throw new UnsupportedOperationException(s"Unexpected message: $message")
      }
    }
  }
}
