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

package org.apache.celeborn.service.deploy

import org.junit.Assert

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.client.{TransportClient, TransportClientFactory}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker

trait HeartbeatFeature extends MiniClusterFeature {

  def testCore(
      workerConf: Map[String, String],
      dataClientFactory: TransportClientFactory,
      assertFunc: (TransportClient, TransportClient) => Unit): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    var master: Master = null
    var workers: collection.Set[Worker] = null
    try {
      val (_master, _workers) =
        setupMiniClusterWithRandomPorts(workerConf = workerConf, workerNum = 1)
      master = _master
      workers = _workers
      workers.foreach { w =>
        val (pushPort, fetchPort) = w.getPushFetchServerPort
        logInfo(s"worker port1:$pushPort $fetchPort")
        val clientPush =
          dataClientFactory.createClient(Utils.localHostName(w.conf), pushPort, 0)
        val clientFetch =
          dataClientFactory.createClient(Utils.localHostName(w.conf), fetchPort, 0)
        logInfo(s"worker port2:$clientPush $clientFetch")
        // At beginning, the client is active
        Assert.assertTrue(clientPush.isActive)
        Assert.assertTrue(clientFetch.isActive)
        assertFunc(clientPush, clientFetch)
      }
    } finally {
      if (master != null && workers != null)
        shutdownMiniCluster()
    }
  }

  def getTestHeartbeatFromWorker2ClientConf: (Map[String, String], CelebornConf) = {
    val workerConf = Map(
      "celeborn.push.heartbeat.interval" -> "4s",
      "celeborn.worker.push.heartbeat.enabled" -> "true",
      "celeborn.worker.fetch.heartbeat.enabled" -> "true",
      "celeborn.fetch.heartbeat.interval" -> "4s")
    val clientConf = new CelebornConf()
    clientConf.set("celeborn.data.io.connectionTimeout", "6s")
    (workerConf, clientConf)
  }

  def testHeartbeatFromWorker2Client(dataClientFactory: TransportClientFactory): Unit = {
    val (workerConf, _) = getTestHeartbeatFromWorker2ClientConf
    // client <- worker: Worker sends heartbeat to client to avoid client read idle, and client sends heartbeat after receiving heartbeat to avoid worker read idle.
    // client active: After client connection timeout, the channel remains active because worker send heartbeat to client.
    testCore(
      workerConf,
      dataClientFactory,
      (pushClient, fetchClient) => {
        Thread.sleep(7 * 1000)
        Assert.assertTrue(fetchClient.isActive)
        Assert.assertTrue(pushClient.isActive)
      })
  }

  def getTestHeartbeatFromWorker2ClientWithNoHeartbeatConf: (Map[String, String], CelebornConf) = {
    val workerConf = Map(
      "celeborn.push.heartbeat.interval" -> "4s",
      "celeborn.fetch.heartbeat.interval" -> "4s",
      "celeborn.worker.push.heartbeat.enabled" -> "false",
      "celeborn.worker.fetch.heartbeat.enabled" -> "false")
    val clientConf = new CelebornConf()
    clientConf.set("celeborn.data.io.connectionTimeout", "6s")
    (workerConf, clientConf)
  }

  def testHeartbeatFromWorker2ClientWithNoHeartbeat(dataClientFactory: TransportClientFactory)
      : Unit = {
    val (workerConf, _) = getTestHeartbeatFromWorker2ClientWithNoHeartbeatConf

    // client <- worker: Worker sends heartbeat to client to avoid client read idle, and client sends heartbeat after receiving heartbeat to avoid worker read idle.
    // client inactive: After client connection timeout, the channel is inactive.
    testCore(
      workerConf,
      dataClientFactory,
      (pushClient, fetchClient) => {
        Thread.sleep(7 * 1000)
        Assert.assertFalse(fetchClient.isActive)
        Assert.assertFalse(pushClient.isActive)
      })
  }

  def getTestHeartbeatFromWorker2ClientWithWorkerTimeoutConf
      : (Map[String, String], CelebornConf) = {
    val workerConf = Map(
      "celeborn.fetch.io.connectionTimeout" -> "3s",
      "celeborn.push.io.connectionTimeout" -> "3s",
      "celeborn.push.heartbeat.interval" -> "4s",
      "celeborn.fetch.heartbeat.interval" -> "4s",
      "celeborn.worker.push.heartbeat.enabled" -> "true",
      "celeborn.worker.fetch.heartbeat.enabled" -> "true",
      CelebornConf.WORKER_CLOSE_IDLE_CONNECTIONS.key -> "true")
    val clientConf = new CelebornConf()
    clientConf.set("celeborn.data.io.connectionTimeout", "6s")
    (workerConf, clientConf)
  }

  def testHeartbeatFromWorker2ClientWithWorkerTimeout(dataClientFactory: TransportClientFactory)
      : Unit = {
    val (workerConf, _) = getTestHeartbeatFromWorker2ClientWithWorkerTimeoutConf

    // client <- worker: Worker sends heartbeat to client to avoid client read idle, and client sends heartbeat after receiving heartbeat to avoid worker read idle.
    // client inactive: Before client connection timeout, the channel is inactive because worker closes heartbeat.
    testCore(
      workerConf,
      dataClientFactory,
      (pushClient, fetchClient) => {
        Thread.sleep(5 * 1000)
        Assert.assertFalse(fetchClient.isActive)
        Assert.assertFalse(pushClient.isActive)
      })
  }
}
