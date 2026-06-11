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

package org.apache.celeborn.server.lifecyclemanager

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

class LifecycleManagerDaemonArgumentsSuite extends AnyFunSuite with Logging {

  test("parse all required arguments") {
    val args = Array(
      "--app-id",
      "test-app-1",
      "--master-endpoints",
      "host1:9097,host2:9097",
      "--port",
      "39099")
    val parsed = LifecycleManagerDaemonArguments.parse(args)
    assert(parsed.appId === "test-app-1")
    assert(parsed.masterEndpoints === "host1:9097,host2:9097")
    assert(parsed.port === 39099)
    assert(parsed.host.isEmpty)
    assert(parsed.propertiesFile.isEmpty)
  }

  test("parse all arguments including optional ones") {
    val args = Array(
      "--app-id",
      "my-app",
      "--master-endpoints",
      "localhost:9097",
      "--port",
      "40000",
      "--host",
      "my-host",
      "--properties-file",
      "/tmp/celeborn.conf")
    val parsed = LifecycleManagerDaemonArguments.parse(args)
    assert(parsed.appId === "my-app")
    assert(parsed.masterEndpoints === "localhost:9097")
    assert(parsed.port === 40000)
    assert(parsed.host === Some("my-host"))
    assert(parsed.propertiesFile === Some("/tmp/celeborn.conf"))
  }

  test("parse with short port flag -p") {
    val args = Array(
      "--app-id",
      "short-app",
      "--master-endpoints",
      "host:9097",
      "-p",
      "2048")
    val parsed = LifecycleManagerDaemonArguments.parse(args)
    assert(parsed.appId === "short-app")
    assert(parsed.port === 2048)
    assert(parsed.host.isEmpty)
  }

  test("-h is treated as help, not host") {
    val args = Array("-h")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 0)
    assert(ex.getMessage.contains("Usage"))
  }

  test("--help requests help with exit code 0") {
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(Array("--help"))
    }
    assert(ex.exitCode === 0)
    assert(ex.getMessage.contains("Usage"))
  }

  test("unknown argument fails with exit code 1") {
    val args = Array(
      "--app-id",
      "app",
      "--master-endpoints",
      "host:9097",
      "--port",
      "39099",
      "--bogus")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 1)
    assert(ex.getMessage.contains("Unknown argument: --bogus"))
  }

  test("missing --app-id fails with exit code 1") {
    val args = Array("--master-endpoints", "host:9097", "--port", "39099")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 1)
    assert(ex.getMessage.contains("--app-id is required"))
  }

  test("missing --master-endpoints fails with exit code 1") {
    val args = Array("--app-id", "app", "--port", "39099")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 1)
    assert(ex.getMessage.contains("--master-endpoints is required"))
  }

  test("missing --port fails with exit code 1") {
    val args = Array("--app-id", "app", "--master-endpoints", "host:9097")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 1)
    assert(ex.getMessage.contains("--port is required"))
  }

  test("port below 1024 fails with exit code 1") {
    val args = Array(
      "--app-id",
      "app",
      "--master-endpoints",
      "host:9097",
      "--port",
      "1023")
    val ex = intercept[ArgumentParseException] {
      LifecycleManagerDaemonArguments.parse(args)
    }
    assert(ex.exitCode === 1)
    assert(ex.getMessage.contains("must be >= 1024"))
  }

  test("applyArgsToConf sets master endpoints and shuffle manager port") {
    val parsed = LifecycleManagerDaemonArguments(
      appId = "app",
      masterEndpoints = "host1:9097,host2:9097",
      port = 39099,
      host = None,
      propertiesFile = None)
    val conf = new CelebornConf()
    LifecycleManagerDaemon.applyArgsToConf(parsed, conf)
    assert(conf.get(CelebornConf.MASTER_ENDPOINTS.key) === "host1:9097,host2:9097")
    assert(conf.get(CelebornConf.CLIENT_SHUFFLE_MANAGER_PORT.key) === "39099")
  }

  test("parse minimum valid port 1024") {
    val args = Array(
      "--app-id",
      "app",
      "--master-endpoints",
      "host:9097",
      "--port",
      "1024")
    val parsed = LifecycleManagerDaemonArguments.parse(args)
    assert(parsed.port === 1024)
  }

  test("usage string contains all options") {
    val usageText = LifecycleManagerDaemonArguments.usage
    assert(usageText.contains("--app-id"))
    assert(usageText.contains("--master-endpoints"))
    assert(usageText.contains("--port"))
    assert(usageText.contains("--host"))
    assert(usageText.contains("--properties-file"))
  }
}
