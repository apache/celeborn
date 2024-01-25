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

import java.net.BindException
import java.nio.file.Files

import scala.collection.mutable
import scala.util.Random

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
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

  private def chooseRandomPort(from: Int, to: Int): Int = {
    Random.nextInt(to - from) + from
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
        val randomPort = chooseRandomPort(1024, 65535)
        val finalMasterConf = Map(
          s"${CelebornConf.MASTER_HOST.key}" -> "localhost",
          s"${CelebornConf.PORT_MAX_RETRY.key}" -> "0",
          s"${CelebornConf.MASTER_PORT.key}" -> s"$randomPort",
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort") ++
          masterConf
        val finalWorkerConf = Map(
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort",
          s"${CelebornConf.PORT_MAX_RETRY.key}" -> "0") ++
          workerConf
        logInfo(s"generated configuration $finalMasterConf")
        val (m, w) =
          setUpMiniCluster(masterConf = finalMasterConf, workerConf = finalWorkerConf, workerNum)
        master = m
        workers = w
        created = true
      } catch {
        case e: BindException =>
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
    val httpPort = chooseRandomPort(1024, 65535)
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
    conf.set(CelebornConf.WORKER_HTTP_PORT.key, s"${chooseRandomPort(1024, 65535)}")
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
    Thread.sleep(20000L)

    if (!masterStartedSignal.head) {
      throw new BindException("cannot start master rpc endpoint")
    }

    val workers = new Array[Worker](workerNum)
    val threads = (1 to workerNum).map { i =>
      val workerThread = new RunnerWrap({
        var workerStarted = false
        var workerStartRetry = 0
        while (!workerStarted) {
          try {
            val worker = createWorker(workerConf)
            this.synchronized {
              workers(i - 1) = worker
            }
            worker.initialize()
            workerStarted = true
          } catch {
            case ex: Exception =>
              if (workers(i - 1) != null) {
                workers(i - 1).shutdownGracefully()
              }
              workerStartRetry += 1
              logError(s"cannot start worker $i, retrying: ", ex)
              if (workerStartRetry == 3) {
                logError(s"cannot start worker $i, reached to max retrying", ex)
                throw ex
              } else {
                Thread.sleep(math.pow(5000, workerStartRetry).toInt)
              }
          }
        }
      })
      workerThread.setName(s"worker ${i} starter thread")
      workerThread
    }
    threads.foreach(_.start())
    Thread.sleep(20000)
    (0 until workerNum).foreach { i => workerInfos.put(workers(i), threads(i)) }

    workerInfos.foreach { case (worker, _) => assert(worker.registered.get()) }
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
