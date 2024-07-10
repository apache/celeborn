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
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.common.util.Utils.selectRandomPort
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}

trait MiniClusterFeature extends Logging {

  var masterInfos = new mutable.HashMap[Master, Thread]()

  class RunnerWrap[T](code: => T) extends Thread {

    override def run(): Unit = {
      Utils.tryLogNonFatalError(code)
    }
  }

  def setupMiniClusterWithRandomPorts(
      masterConf: Map[String, String] = Map()): collection.Set[Master] = {
    var retryCount = 0
    var created = false
    var masters: collection.Set[Master] = null
    while (!created) {
      try {
        val randomPort = selectRandomPort(1024, 65535)
        val finalMasterConf = Map(
          s"${CelebornConf.HA_ENABLED.key}" -> "true",
          s"celeborn.master.ha.node.1.port" -> s"$randomPort",
          s"celeborn.master.ha.node.1.ratis.port" -> s"${randomPort + 1}",
          s"celeborn.master.ha.node.2.port" -> s"${randomPort + 2}",
          s"celeborn.master.ha.node.2.ratis.port" -> s"${randomPort + 3}",
          s"celeborn.master.ha.node.3.port" -> s"${randomPort + 4}",
          s"celeborn.master.ha.node.3.ratis.port" -> s"${randomPort + 5}",
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort,localhost:${randomPort + 2},localhost:${randomPort + 4}",
          s"${CelebornConf.MASTER_HOST.key}" -> "localhost",
          s"${CelebornConf.PORT_MAX_RETRY.key}" -> "0") ++
          masterConf

        logInfo(s"Generated configuration $finalMasterConf.")
        val m = setUpMiniCluster(finalMasterConf)
        masters = m
        created = true
      } catch {
        case e: IOException
            if e.isInstanceOf[BindException] || Option(e.getCause).exists(
              _.isInstanceOf[BindException]) =>
          logError(s"Failed to setup mini cluster, retrying (retry count: $retryCount)", e)
          retryCount += 1
          if (retryCount == 3) {
            logError("Failed to setup mini cluster, reached the max retry count", e)
            throw e
          }
      }
    }
    masters
  }

  def createTmpDir(): String = {
    val tmpDir = Files.createTempDirectory("celeborn-")
    logInfo(s"Created temp dir: $tmpDir.")
    tmpDir.toFile.deleteOnExit()
    tmpDir.toAbsolutePath.toString
  }

  def createMaster(nodeId: Int, map: Map[String, String] = null): Master = {
    createMaster(nodeId, map, createTmpDir())
  }

  def createMaster(
      nodeId: Int,
      map: Map[String, String],
      storageDir: String): Master = {
    logInfo("Start create master for mini cluster.")
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_MASTER_NODE_ID, s"$nodeId")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR, storageDir)
    conf.set(CelebornConf.METRICS_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, s"${selectRandomPort(1024, 65535)}")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }
    logInfo("Celeborn master conf created.")

    val masterArguments = new MasterArguments(Array(), conf)
    logInfo("Master argument created.")
    try {
      val master = new Master(conf, masterArguments)
      logInfo("Master created for mini cluster.")
      master
    } catch {
      case e: Exception =>
        logError("Create master failed, detail:", e)
        System.exit(-1)
        null
    }
  }

  private def setUpMiniCluster(
      masterConf: Map[String, String] = null): collection.Set[Master] = {
    val timeout = 30000
    val masters = new Array[Master](3)
    val flagUpdateLock = new ReentrantLock()
    val threads = (1 to 3).map { nodeId =>
      val masterThread = new RunnerWrap({
        var masterStartRetry = 0
        var masterStarted = false
        while (!masterStarted) {
          try {
            val master = createMaster(nodeId, masterConf)
            flagUpdateLock.lock()
            masters(nodeId - 1) = master
            flagUpdateLock.unlock()
            masterStarted = true
            master.initialize()
          } catch {
            case e: Exception =>
              if (masters(nodeId - 1) != null) {
                masters(nodeId - 1).stop(CelebornExitKind.EXIT_IMMEDIATELY)
              }
              masterStarted = false
              masterStartRetry += 1
              logError(s"Cannot start master $nodeId, retrying: ", e)
              if (masterStartRetry == 3) {
                logError(s"Cannot start master $nodeId, reached to max retrying", e)
                throw e
              } else {
                Thread.sleep(math.pow(2000, masterStartRetry).toInt)
              }
          }
        }
      })
      masterThread.setName(s"master-$nodeId-starter")
      masterThread
    }
    threads.foreach(_.start())
    Thread.sleep(5000)
    var allMastersStarted = false
    var mastersWaitingTime = 0
    while (!allMastersStarted) {
      try {
        (0 until 3).foreach { i =>
          {
            if (masters(i) == null) {
              throw new IllegalStateException(s"Master $i hasn't been initialized")
            } else {
              masterInfos.put(masters(i), threads(i))
            }
          }
        }
        allMastersStarted = true
      } catch {
        case e: Exception =>
          logError("All masters haven't been started retrying", e)
          Thread.sleep(5000)
          mastersWaitingTime += 5000
          if (mastersWaitingTime >= timeout) {
            logError(s"Cannot start all masters after $timeout ms", e)
            throw e
          }
      }
    }
    masterInfos.keySet
  }

  def shutdownMiniCluster(): Unit = {
    // shutdown masters
    masterInfos.foreach {
      case (master, _) =>
        master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        master.rpcEnv.shutdown()
    }

    // interrupt threads
    Thread.sleep(5000)
    masterInfos.foreach {
      case (_, thread) =>
        thread.interrupt()
    }
    masterInfos.clear()
  }
}
