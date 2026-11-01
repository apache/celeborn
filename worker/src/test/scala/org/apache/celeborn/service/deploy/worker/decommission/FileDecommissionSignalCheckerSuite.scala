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

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_FILE_PATH
import org.apache.celeborn.common.util.JavaUtils

class FileDecommissionSignalCheckerSuite extends AnyFunSuite {

  test("signals once the configured file exists") {
    val dir = Files.createTempDirectory("celeborn-decommission-signal")
    try {
      val signalFile = dir.resolve("decommission")
      val conf =
        new CelebornConf().set(WORKER_DECOMMISSION_SIGNAL_FILE_PATH.key, signalFile.toString)
      val checker = new FileDecommissionSignalChecker
      checker.init(conf)

      assert(!checker.shouldDecommission())
      Files.createFile(signalFile)
      assert(checker.shouldDecommission())
      assert(checker.getReason.contains(signalFile.toString))
    } finally {
      JavaUtils.deleteRecursively(dir.toFile)
    }
  }

  test("init fails when the file path is unset or blank") {
    Seq(new CelebornConf(), new CelebornConf().set(WORKER_DECOMMISSION_SIGNAL_FILE_PATH.key, "  "))
      .foreach { conf =>
        val e = intercept[IllegalArgumentException] {
          new FileDecommissionSignalChecker().init(conf)
        }
        assert(e.getMessage.contains(WORKER_DECOMMISSION_SIGNAL_FILE_PATH.key))
      }
  }
}
