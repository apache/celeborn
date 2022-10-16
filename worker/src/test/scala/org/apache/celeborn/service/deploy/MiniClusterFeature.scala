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

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments}

trait MiniClusterFeature extends Logging {
  val workerPrometheusPort = new AtomicInteger(12378)
  val masterPrometheusPort = new AtomicInteger(22378)

  protected def runnerWrap[T](code: => T): Thread = new Thread(new Runnable {
    override def run(): Unit = {
      Utils.tryLogNonFatalError(code)
    }
  })

  protected def createTmpDir(): String = {
    val tmpDir = Files.createTempDirectory("rss-")
    logInfo(s"created temp dir: $tmpDir")
    tmpDir.toFile.deleteOnExit()
    tmpDir.toAbsolutePath.toString
  }

  protected def createMaster(map: Map[String, String] = null): (Master, RpcEnv) = {
    val conf = new RssConf()
    conf.set("celeborn.metrics.enabled", "false")
    val prometheusPort = masterPrometheusPort.getAndIncrement()
    conf.set("celeborn.master.metrics.prometheus.port", s"$prometheusPort")
    logInfo(s"set celeborn.master.metrics.prometheus.port to $prometheusPort")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }

    val masterArguments = new MasterArguments(Array(), conf)
    val master = new Master(conf, masterArguments)
    master.startHttpServer()

    Thread.sleep(5000L)
    (master, master.rpcEnv)
  }

  protected def createWorker(map: Map[String, String] = null): (Worker, RpcEnv) = {
    logInfo("start create worker for mini cluster")
    val conf = new RssConf()
    conf.set("celeborn.worker.storage.dirs", createTmpDir())
    conf.set("celeborn.worker.deviceMonitor.enabled", "false")
    conf.set("celeborn.push.buffer.size", "256K")
    conf.set(
      "celeborn.master.metrics.prometheus.port",
      s"${workerPrometheusPort.incrementAndGet()}")
    conf.set("rss.fetch.io.threads", "4")
    conf.set("rss.push.io.threads", "4")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }
    logInfo("rss conf created")

    val workerArguments = new WorkerArguments(Array(), conf)
    logInfo("worker argument created")
    try {
      val worker = new Worker(conf, workerArguments)
      logInfo("worker created for mini cluster")
      (worker, worker.rpcEnv)
    } catch {
      case e: Exception =>
        logError("create worker failed, detail:", e)
        System.exit(-1)
        (null, null)
    }
  }

  def setUpMiniCluster(
      masterConfs: Map[String, String] = null,
      workerConfs: Map[String, String] = null)
      : (Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv) = {
    val (master, masterRpcEnv) = createMaster(masterConfs)
    val masterThread = runnerWrap(masterRpcEnv.awaitTermination())
    masterThread.start()

    Thread.sleep(5000L)

    val (worker1, workerRpcEnv1) = createWorker(workerConfs)
    val workerThread1 = runnerWrap(worker1.initialize())
    workerThread1.start()

    val (worker2, workerRpcEnv2) = createWorker(workerConfs)
    val workerThread2 = runnerWrap(worker2.initialize())
    workerThread2.start()

    val (worker3, workerRpcEnv3) = createWorker(workerConfs)
    val workerThread3 = runnerWrap(worker3.initialize())
    workerThread3.start()

    val (worker4, workerRpcEnv4) = createWorker(workerConfs)
    val workerThread4 = runnerWrap(worker4.initialize())
    workerThread4.start()

    val (worker5, workerRpcEnv5) = createWorker(workerConfs)
    val workerThread5 = runnerWrap(worker5.initialize())
    workerThread5.start()

    Thread.sleep(5000L)

    assert(worker1.isRegistered())
    assert(worker2.isRegistered())
    assert(worker3.isRegistered())
    assert(worker4.isRegistered())
    assert(worker5.isRegistered())

    (
      worker1,
      workerRpcEnv1,
      worker2,
      workerRpcEnv2,
      worker3,
      workerRpcEnv3,
      worker4,
      workerRpcEnv4,
      worker5,
      workerRpcEnv5)
  }
}
