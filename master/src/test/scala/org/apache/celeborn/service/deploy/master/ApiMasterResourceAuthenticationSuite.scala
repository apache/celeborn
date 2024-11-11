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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, ThreadUtils, Utils}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceAuthenticationSuite

class ApiMasterResourceAuthenticationSuite extends ApiBaseResourceAuthenticationSuite {
  private var master: Master = _

  override protected def httpService: HttpService = master

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDirectory(null).toFile
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    val randomMasterPort = Utils.selectRandomInt(1024, 65535)
    val randomHttpPort = randomMasterPort + 1
    celebornConf.set(CelebornConf.HA_ENABLED.key, "false")
    celebornConf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    celebornConf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    celebornConf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    celebornConf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, celebornConf)
    master = new Master(celebornConf, masterArgs)
    ThreadUtils.newThread(
      new Runnable {
        override def run(): Unit = {
          master.initialize()
        }
      },
      "api-master-thread").start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    master.rpcEnv.shutdown()
  }
}
