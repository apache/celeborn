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

package org.apache.celeborn.cli

import java.io.File
import java.util.concurrent.TimeUnit

import org.scalatest.funsuite.AnyFunSuite

class CelebornCliExitCodeSuite extends AnyFunSuite {
  private def runCli(args: String*): Int = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val command = Seq(
      java,
      "-cp",
      System.getProperty("java.class.path"),
      "org.apache.celeborn.cli.CelebornCli") ++ args
    val process = new ProcessBuilder(command: _*)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .start()
    try {
      assert(process.waitFor(30, TimeUnit.SECONDS), "CLI process timed out")
      process.exitValue()
    } finally {
      process.destroyForcibly()
    }
  }

  test("CLI exits successfully for help") {
    assert(runCli("--help") == 0)
  }

  test("CLI exits with picocli's usage error status for an unknown option") {
    assert(runCli("--unknown-option") == 2)
  }
}
