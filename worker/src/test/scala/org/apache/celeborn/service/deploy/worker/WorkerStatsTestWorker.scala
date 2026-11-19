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

import org.apache.celeborn.common.CelebornConf

private[deploy] class WorkerStatsTestWorker(conf: CelebornConf)
  extends Worker(conf, new WorkerArguments(Array.empty, conf)) {

  @volatile private var testClosed = false

  override def exitImmediately(): Unit = {
    if (!testClosed) super.exitImmediately()
  }

  def shutdownRpcEnvs(): Unit = {
    // This fixture owns teardown; the JVM hook must not send RPCs after the cluster is closed.
    testClosed = true
    internalRpcEnvInUse.shutdown()
    if (internalRpcEnvInUse ne rpcEnv) rpcEnv.shutdown()
  }
}
