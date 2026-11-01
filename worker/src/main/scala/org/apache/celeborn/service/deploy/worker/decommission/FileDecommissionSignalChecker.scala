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

package org.apache.celeborn.service.deploy.worker.decommission

import java.nio.file.{Files, Path, Paths}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_FILE_PATH

/**
 * Signals decommission once the file at `celeborn.worker.decommission.signal.file.path` exists,
 * for example after a Kubernetes preStop hook or a spot-notice agent touches it.
 */
class FileDecommissionSignalChecker extends DecommissionSignalChecker {
  @volatile private var path: Path = _

  override def init(conf: CelebornConf): Unit = {
    val configured = conf.workerDecommissionSignalFilePath.map(_.trim).filter(_.nonEmpty)
    if (configured.isEmpty) {
      throw new IllegalArgumentException(
        s"${WORKER_DECOMMISSION_SIGNAL_FILE_PATH.key} must be set when using ${getClass.getName}")
    }
    path = Paths.get(configured.get)
  }

  override def shouldDecommission(): Boolean = Files.exists(path)

  override def getReason: String = s"signal file $path exists"
}
