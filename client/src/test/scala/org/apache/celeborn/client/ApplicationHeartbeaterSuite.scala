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
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, ApplicationLostResponse, CheckQuotaResponse, HeartbeatFromApplication, HeartbeatFromApplicationResponse}
import org.apache.celeborn.common.protocol.message.Message
import org.apache.celeborn.common.protocol.message.StatusCode

class ApplicationHeartbeaterSuite extends CelebornFunSuite {

  test("stop sends a final heartbeat with the latest metrics before ApplicationLost") {
    val conf = new CelebornConf()
    // Only the immediate first heartbeat fires; later increments must be flushed by stop().
    conf.set(CelebornConf.APPLICATION_HEARTBEAT_INTERVAL.key, "1h")
    conf.set(CelebornConf.METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.CLIENT_METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.CLIENT_METRICS_APP_LABELS.key, "env=test")

    val sent = new CopyOnWriteArrayList[AnyRef]()
    val masterClient = mock(classOf[MasterClient])
    when(masterClient.askSync[AnyRef](any[Message](), any[Class[AnyRef]]())).thenAnswer(
      (invocation: InvocationOnMock) => {
        val message = invocation.getArgument[AnyRef](0)
        sent.add(message)
        message match {
          case _: HeartbeatFromApplication =>
            HeartbeatFromApplicationResponse(
              StatusCode.SUCCESS,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              CheckQuotaResponse(isAvailable = true, ""))
          case _ => ApplicationLostResponse(StatusCode.SUCCESS)
        }
      })

    @volatile var counter = 0L
    val clientMetrics = () =>
      Collections.singletonMap("counter", ClientMetric(counter, MetricType.Counter))
    val heartbeater = new ApplicationHeartbeater(
      "app-1",
      conf,
      masterClient,
      () =>
        ((0L, 0L), (0L, 0L, Map.empty[String, java.lang.Long], Map.empty[String, java.lang.Long])),
      new WorkerStatusTracker(conf, null),
      ConcurrentHashMap.newKeySet[Int](),
      _ => (),
      clientMetrics)
    heartbeater.start()
    eventually(timeout(Span(10, Seconds))) {
      assert(sent.asScala.exists(_.isInstanceOf[HeartbeatFromApplication]))
    }

    // Increment after the last periodic heartbeat.
    counter = 5L
    heartbeater.stop()

    val messages = sent.asScala.toList
    assert(messages.last.isInstanceOf[ApplicationLost])
    val heartbeats = messages.collect { case h: HeartbeatFromApplication => h }
    assert(heartbeats.size == 2)
    assert(messages.dropRight(1).last eq heartbeats.last)
    assert(heartbeats.last.clientMetrics.get("counter").value == 5L)
  }
}
