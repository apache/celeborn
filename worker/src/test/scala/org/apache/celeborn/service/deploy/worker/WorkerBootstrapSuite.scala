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

package org.apache.celeborn.service.deploy.worker

import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.netty.channel.Channel
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.client.{TransportClient, TransportClientBootstrap}
import org.apache.celeborn.common.network.server.{BaseMessageHandler, TransportServerBootstrap}
import org.apache.celeborn.common.network.util.TransportConf
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class WorkerBootstrapSuite extends AnyFunSuite {
  import WorkerBootstrapSuite._

  test("stop closes Worker bootstraps after the replication client factory") {
    serverBootstraps.clear()
    clientBootstraps.clear()
    val storageDir = Files.createTempDirectory("worker-bootstrap-").toFile
    val conf = new CelebornConf(false)
      .set(CelebornConf.WORKER_STORAGE_DIRS.key, storageDir.getPath)
      .set(CelebornConf.WORKER_DISK_MONITOR_CHECKLIST.key, "readwrite")
      .set("celeborn.rpc_service.io.serverThreads", "1")
      .set("celeborn.rpc_service.io.clientThreads", "1")
      .set("celeborn.rpc_service.dispatcher.threads", "1")
      .set("celeborn.replicate.client.bootstrap.classes", classOf[CountingClientBootstrap].getName)
    Seq("push", "fetch", "replicate").foreach { module =>
      conf.set(
        s"celeborn.$module.server.bootstrap.classes",
        classOf[CountingServerBootstrap].getName)
      conf.set(s"celeborn.worker.$module.io.threads", "1")
    }
    val peerConf = new CelebornConf(false).set("celeborn.shuffle.io.serverThreads", "1")
    val peerContext = new TransportContext(
      new TransportConf("shuffle", peerConf),
      new BaseMessageHandler())
    val peerServer = peerContext.createServer("localhost", 0)
    var worker: Worker = null
    try {
      worker = new Worker(conf, new WorkerArguments(Array("--host", "localhost"), conf))
      assert(serverBootstraps.size() == 3)
      assert(serverBootstraps.asScala.map(_.module).toSet == Set("push", "fetch", "replicate"))
      assert(clientBootstraps.size() == 1)
      val clientBootstrap = clientBootstraps.get(0)
      assert(clientBootstrap.module == "replicate")
      val client = worker.replicateClientFactory.createClient("localhost", peerServer.getPort)
      assert(client.isActive)

      // The peer stays open so shutting down Worker's own servers cannot close this connection.
      worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      assert(serverBootstraps.asScala.forall(_.closed.get() == 1))
      assert(clientBootstrap.closed.get() == 1)
      assert(
        clientBootstrap.clientClosedBeforeBootstrap,
        "The replication client must close before its configured bootstrap")
    } finally {
      if (worker != null) {
        worker.rpcEnv.shutdown()
        worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        worker.replicateClientFactory.close()
      }
      peerServer.close()
      peerContext.close()
      MemoryManager.reset()
      JavaUtils.deleteRecursively(storageDir)
    }
  }
}

object WorkerBootstrapSuite {
  val serverBootstraps = new CopyOnWriteArrayList[CountingServerBootstrap]()
  val clientBootstraps = new CopyOnWriteArrayList[CountingClientBootstrap]()

  class CountingServerBootstrap(conf: TransportConf) extends TransportServerBootstrap {
    val module: String = conf.getModuleName
    val closed = new AtomicInteger()
    serverBootstraps.add(this)

    override def doBootstrap(channel: Channel, handler: BaseMessageHandler): BaseMessageHandler =
      handler

    override def close(): Unit = {
      closed.incrementAndGet()
    }
  }

  class CountingClientBootstrap(conf: TransportConf) extends TransportClientBootstrap {
    val module: String = conf.getModuleName
    val closed = new AtomicInteger()
    private var client: TransportClient = _
    var clientClosedBeforeBootstrap = false
    clientBootstraps.add(this)

    override def doBootstrap(transportClient: TransportClient): Unit = {
      client = transportClient
    }

    override def close(): Unit = {
      clientClosedBeforeBootstrap = client != null && !client.isActive
      closed.incrementAndGet()
    }
  }
}
