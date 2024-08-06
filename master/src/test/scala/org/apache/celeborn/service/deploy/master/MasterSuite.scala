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

import com.google.common.io.Files
import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.{PbCheckForWorkerTimeout, PbRegisterWorker}
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, HeartbeatFromApplication}
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}

class MasterSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  test("test single node startup functionality") {
    val conf = new CelebornConf()
    val randomMasterPort = Utils.selectRandomPort(1024, 65535)
    val randomHttpPort = randomMasterPort + 1
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    conf.set(CelebornConf.METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)
    new Thread() {
      override def run(): Unit = {
        master.initialize()
      }
    }.start()
    Thread.sleep(5000L)
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    master.rpcEnv.shutdown()
  }

  test("test dedicated internal port receives") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    conf.set(CelebornConf.METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")

    val args = Array("-h", "localhost", "-p", "9097", "--internal-port", "8097")

    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)
    new Thread() {
      override def run(): Unit = {
        master.initialize()
      }
    }.start()
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

  test("test master worker host pattern") {
    val conf = new CelebornConf()
    val randomMasterPort = Utils.selectRandomPort(1024, 65535)
    val randomHttpPort = randomMasterPort + 1
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

    assert(master.workerHostAllowToRegister("test.k8s.io"))
    assert(!master.workerHostAllowToRegister("test.k8s.io.com"))
    assert(!master.workerHostAllowToRegister("test.example.com"))
    assert(!master.workerHostAllowToRegister("deny.k8s.io"))

    conf.unset(CelebornConf.ALLOW_WORKER_HOST_PATTERN)
    conf.unset(CelebornConf.DENY_WORKER_HOST_PATTERN)
    master = new Master(conf, masterArgs)

    assert(master.workerHostAllowToRegister("test.k8s.io"))
    assert(master.workerHostAllowToRegister("test.k8s.io.com"))
    assert(master.workerHostAllowToRegister("test.example.com"))
    assert(master.workerHostAllowToRegister("deny.k8s.io"))
  }
}
