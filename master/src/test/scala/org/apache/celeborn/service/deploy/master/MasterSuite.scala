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

package org.apache.celeborn.service.deploy.master

import java.nio.file.Files
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.message.ControlMessages.RequestSlots
import org.apache.celeborn.common.protocol.{PbCheckForWorkerTimeout, PbRegisterWorker, TransportModuleConstants}
import org.apache.celeborn.common.rpc.netty.NettyRpcEnvFactory
import org.apache.celeborn.common.rpc.{RpcEndpoint, RpcEnv, RpcEnvConfig}
import org.apache.celeborn.common.util.{CelebornExitKind, ThreadUtils}

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

class MasterSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MasterClusterFeature {

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDirectory(null).toFile
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  def createRpcEnv(
                    conf: CelebornConf,
                    name: String,
                    port: Int,
                    clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(
      conf,
      "test",
      TransportModuleConstants.RPC_MODULE,
      "localhost",
      "localhost",
      port,
      0,
      Role.CLIENT,
      None,
      None)
    new NettyRpcEnvFactory().create(config)
  }


  test("test single node startup functionality") {
    withRetryOnPortBindException { () =>
      val conf = new CelebornConf()
      val randomMasterPort = selectRandomPort()
      val randomHttpPort = selectRandomPort()
      conf.set(CelebornConf.HA_ENABLED.key, "false")
      conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
      conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
      conf.set(CelebornConf.METRICS_ENABLED.key, "true")
      conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
      conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

      val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

      val masterArgs = new MasterArguments(args, conf)
      val master = new Master(conf, masterArgs)
      ThreadUtils.newThread(
        new Runnable {
          override def run(): Unit = {
            master.initialize()
          }
        },
        "master-init-thread").start()
      Thread.sleep(5000L)
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
    }
  }

  test("test dedicated internal port receives") {
    withRetryOnPortBindException { () =>
      val conf = new CelebornConf()
      val randomMasterPort = selectRandomPort()
      val randomHttpPort = selectRandomPort()
      val randomInternalPort = selectRandomPort()
      conf.set(CelebornConf.HA_ENABLED.key, "false")
      conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
      conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
      conf.set(CelebornConf.METRICS_ENABLED.key, "true")
      conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
      conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

      val args = Array(
        "-h",
        "localhost",
        "-p",
        randomMasterPort.toString,
        "--internal-port",
        randomInternalPort.toString)

      val masterArgs = new MasterArguments(args, conf)
      val master = new Master(conf, masterArgs)
      ThreadUtils.newThread(
        new Runnable {
          override def run(): Unit = {
            master.initialize()
          }
        },
        "master-init-thread").start()
      Thread.sleep(5000L)
      master.receive.applyOrElse(
        PbCheckForWorkerTimeout.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))
      master.internalRpcEndpoint.receive.applyOrElse(
        PbCheckForWorkerTimeout.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))

      master.internalRpcEndpoint.receiveAndReply(
        mock(classOf[org.apache.celeborn.common.rpc.RpcCallContext])).applyOrElse(
        PbRegisterWorker.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
      master.internalRpcEnvInUse.shutdown()
    }
  }

  test("test master worker host allow and deny pattern") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    conf.set(CelebornConf.METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)
    conf.set(CelebornConf.ALLOW_WORKER_HOST_PATTERN.key, ".*\\.k8s\\.io$")
    conf.set(CelebornConf.DENY_WORKER_HOST_PATTERN.key, "^deny\\.k8s\\.io$")

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, conf)
    var master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(!master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(!master.workerHostAllowedToRegister("test.example.com"))
    assert(!master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()

    conf.unset(CelebornConf.ALLOW_WORKER_HOST_PATTERN)
    master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(master.workerHostAllowedToRegister("test.example.com"))
    assert(!master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()

    conf.unset(CelebornConf.DENY_WORKER_HOST_PATTERN)
    master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(master.workerHostAllowedToRegister("test.example.com"))
    assert(master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()
  }

  test("test min assign slots") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)

    val requestSlots = RequestSlots("app_id", 0, new util.ArrayList(List[Integer](0, 1).asJava), "localhost", false, false, new UserIdentifier("tenant", "user"), 100, conf.availableStorageTypes)

    val anotherEnv = createRpcEnv(new CelebornConf(), "remote", 0)

    anotherEnv.

//    master.receiveAndReply(
//      mock(classOf[org.apache.celeborn.common.rpc.RpcCallContext]), requestSlots).applyOrElse(
//      requestSlots,
//      (_: Any) => fail("Unexpected message"))
//    master.handleRequestSlots()
//    assert(master.workerHostAllowedToRegister("test.k8s.io"))
//    assert(!master.workerHostAllowedToRegister("test.k8s.io.com"))
//    assert(!master.workerHostAllowedToRegister("test.example.com"))
//    assert(!master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()
  }

}
