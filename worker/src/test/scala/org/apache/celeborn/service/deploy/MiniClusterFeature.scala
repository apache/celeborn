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

import java.io.IOException
import java.net.BindException
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.common.util.Utils.selectRandomPort
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

trait MiniClusterFeature extends Logging {

  var masterInfo: (Master, Thread) = _
  val workerInfos = new mutable.HashMap[Worker, Thread]()

  class RunnerWrap[T](code: => T) extends Thread {

    override def run(): Unit = {
      Utils.tryLogNonFatalError(code)
    }
  }

  def setupMiniClusterWithRandomPorts(
      masterConf: Map[String, String] = Map(),
      workerConf: Map[String, String] = Map(),
      workerNum: Int = 3): (Master, collection.Set[Worker]) = {
    var retryCount = 0
    var created = false
    var master: Master = null
    var workers: collection.Set[Worker] = null
    while (!created) {
      try {
        val randomPort = selectRandomPort(1024, 65535)
        val randomInternalPort = selectRandomPort(1024, 65535)
        val finalMasterConf = Map(
          s"${CelebornConf.MASTER_HOST.key}" -> "localhost",
          s"${CelebornConf.PORT_MAX_RETRY.key}" -> "0",
          s"${CelebornConf.MASTER_PORT.key}" -> s"$randomPort",
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort",
          s"${CelebornConf.MASTER_INTERNAL_PORT.key}" -> s"$randomInternalPort",
          s"${CelebornConf.MASTER_INTERNAL_ENDPOINTS.key}" -> s"localhost:$randomInternalPort") ++
          masterConf
        val finalWorkerConf = Map(
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort",
          s"${CelebornConf.MASTER_INTERNAL_ENDPOINTS.key}" -> s"localhost:$randomInternalPort") ++
          workerConf
        logInfo(
          s"generated configuration. Master conf = $finalMasterConf, worker conf = $finalWorkerConf")
        val (m, w) =
          setUpMiniCluster(masterConf = finalMasterConf, workerConf = finalWorkerConf, workerNum)
        master = m
        workers = w
        created = true
      } catch {
        case e: IOException
            if e.isInstanceOf[BindException] || Option(e.getCause).exists(
              _.isInstanceOf[BindException]) =>
          logError(s"failed to setup mini cluster, retrying (retry count: $retryCount)", e)
          retryCount += 1
          if (retryCount == 3) {
            logError("failed to setup mini cluster, reached the max retry count", e)
            throw e
          }
      }
    }
    (master, workers)
  }

  def createTmpDir(): String = {
    val tmpDir = Files.createTempDirectory("celeborn-")
    logInfo(s"created temp dir: $tmpDir")
    tmpDir.toFile.deleteOnExit()
    tmpDir.toAbsolutePath.toString
  }

  private def createMaster(map: Map[String, String] = null): Master = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.METRICS_ENABLED.key, "false")
    val httpPort = selectRandomPort(1024, 65535)
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, s"$httpPort")
    logInfo(s"set ${CelebornConf.MASTER_HTTP_PORT.key} to $httpPort")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }

    val masterArguments = new MasterArguments(Array(), conf)
    val master = new Master(conf, masterArguments)
    master.startHttpServer()

    Thread.sleep(5000L)
    master
  }

  def createWorker(map: Map[String, String] = null): Worker = {
    createWorker(map, createTmpDir())
  }

  def createWorker(map: Map[String, String], storageDir: String): Worker = {
    logInfo("start create worker for mini cluster")
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, storageDir)
    conf.set(CelebornConf.WORKER_DISK_MONITOR_ENABLED.key, "false")
    conf.set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
    conf.set(CelebornConf.WORKER_HTTP_PORT.key, s"${selectRandomPort(1024, 65535)}")
    conf.set("celeborn.fetch.io.threads", "4")
    conf.set("celeborn.push.io.threads", "4")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }
    logInfo("celeborn conf created")

    val workerArguments = new WorkerArguments(Array(), conf)
    logInfo("worker argument created")
    try {
      val worker = new Worker(conf, workerArguments)
      logInfo("worker created for mini cluster")
      worker
    } catch {
      case e: Exception =>
        logError("create worker failed, detail:", e)
        System.exit(-1)
        null
    }
  }

  private def setUpMiniCluster(
      masterConf: Map[String, String] = null,
      workerConf: Map[String, String] = null,
      workerNum: Int = 3): (Master, collection.Set[Worker]) = {
    val timeout = 30000
    val master = createMaster(masterConf)
    val masterStartedSignal = Array(false)
    val masterThread = new RunnerWrap({
      try {
        masterStartedSignal(0) = true
        master.rpcEnv.awaitTermination()
      } catch {
        case ex: Exception =>
          masterStartedSignal(0) = false
          throw ex
      }
    })
    masterThread.start()
    masterInfo = (master, masterThread)
    var masterStartWaitingTime = 0
    Thread.sleep(5000)
    while (!masterStartedSignal.head) {
      logInfo("waiting for master node starting")
      masterStartWaitingTime += 5000
      if (masterStartWaitingTime >= timeout) {
        throw new BindException("cannot start master rpc endpoint")
      }
    }

    val workers = new Array[Worker](workerNum)
    val flagUpdateLock = new ReentrantLock()
    val threads = (1 to workerNum).map { i =>
      val workerThread = new RunnerWrap({
        var workerStartRetry = 0
        var workerStarted = false
        while (!workerStarted) {
          try {
            val worker = createWorker(workerConf)
            flagUpdateLock.lock()
            workers(i - 1) = worker
            flagUpdateLock.unlock()
            workerStarted = true
            worker.initialize()
          } catch {
            case ex: Exception =>
              if (workers(i - 1) != null) {
                workers(i - 1).shutdownGracefully()
              }
              workerStarted = false
              workerStartRetry += 1
              logError(s"cannot start worker $i, retrying: ", ex)
              if (workerStartRetry == 3) {
                logError(s"cannot start worker $i, reached to max retrying", ex)
                throw ex
              } else {
                TimeUnit.SECONDS.sleep(Math.pow(2, workerStartRetry).toLong)
              }
          }
        }
      })
      workerThread.setName(s"worker $i starter thread")
      workerThread
    }
    threads.foreach(_.start())
    Thread.sleep(5000)
    var allWorkersStarted = false
    var workersWaitingTime = 0
    while (!allWorkersStarted) {
      try {
        (0 until workerNum).foreach { i =>
          {
            if (workers(i) == null) {
              throw new IllegalStateException(s"worker $i hasn't been initialized")
            } else {
              workerInfos.put(workers(i), threads(i))
            }
          }
        }
        workerInfos.foreach { case (worker, _) => assert(worker.registered.get()) }
        allWorkersStarted = true
      } catch {
        case ex: Throwable =>
          logError("all workers haven't been started retrying", ex)
          Thread.sleep(5000)
          workersWaitingTime += 5000
          if (workersWaitingTime >= timeout) {
            logError(s"cannot start all workers after $timeout ms", ex)
            throw ex
          }
      }
    }
    (master, workerInfos.keySet)
  }

  def shutdownMiniCluster(): Unit = {
    // shutdown workers
    workerInfos.foreach {
      case (worker, _) =>
        worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        worker.rpcEnv.shutdown()
    }

    // shutdown masters
    masterInfo._1.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    masterInfo._1.rpcEnv.shutdown()

    // interrupt threads
    Thread.sleep(5000)
    workerInfos.foreach {
      case (worker, thread) =>
        worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        thread.interrupt()
    }
    workerInfos.clear()
    masterInfo._2.interrupt()
    MemoryManager.reset()
  }
}
