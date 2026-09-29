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

import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.security.StartupCountingBootstrap
import org.apache.celeborn.common.util.JavaUtils

class WorkerTransportStartupSuite extends CelebornFunSuite {
  Seq("push-bind", "replicate-bind", "replicate-factory", "fetch-bind").foreach { scenario =>
    test(s"failed Worker transport acquisition releases its own resources: $scenario") {
      // Earlier Worker constructor resources remain outside this local rollback guarantee.
      // Isolate the failed object in a child so those existing resources cannot leak into tests.
      val output = Files.createTempFile("celeborn-transport-startup-", ".out")
      val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
      val process = new ProcessBuilder(
        java,
        "-Xmx1g",
        "-XX:MaxDirectMemorySize=256m",
        "-cp",
        System.getProperty("java.class.path"),
        "org.apache.celeborn.service.deploy.worker.WorkerTransportStartupProbe",
        scenario)
        .redirectErrorStream(true).redirectOutput(output.toFile).start()
      try {
        val finished = process.waitFor(45, TimeUnit.SECONDS)
        if (!finished) {
          process.destroyForcibly()
          process.waitFor(5, TimeUnit.SECONDS)
        }
        val details = new String(Files.readAllBytes(output), StandardCharsets.UTF_8)
        assert(finished, s"Startup probe timed out: $details")
        assert(process.exitValue() == 0, details)
      } finally {
        if (process.isAlive) process.destroyForcibly()
        Files.deleteIfExists(output)
      }
    }
  }
}

object WorkerTransportStartupProbe {
  def main(args: Array[String]): Unit = {
    try {
      run(args(0))
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
        System.exit(1)
    }
    System.exit(0)
  }

  private def run(scenario: String): Unit = {
    val module = scenario.takeWhile(_ != '-')
    val factoryFailure = scenario.endsWith("factory")
    val directory = Files.createTempDirectory("celeborn-worker-startup-")
    val properties = Files.createFile(directory.resolve("empty.conf"))
    val occupied = new ServerSocket(0)
    val port = occupied.getLocalPort
    if (factoryFailure) occupied.close()
    StartupCountingBootstrap.reset()
    val bootstrap = classOf[StartupCountingBootstrap].getName
    val conf = new CelebornConf(false)
      .set("celeborn.auth.enabled", "false")
      .set("celeborn.internal.port.enabled", "false")
      .set("celeborn.metrics.enabled", "false")
      .set("celeborn.rpc.io.threads", "1")
      .set("celeborn.rpc.dispatcher.threads", "2")
      .set("celeborn.worker.push.io.threads", "1")
      .set("celeborn.worker.replicate.io.threads", "1")
      .set("celeborn.worker.fetch.io.threads", "1")
      .set("celeborn.worker.storage.dirs", directory.resolve("storage").toString)
      .set(CelebornConf.WORKER_DISK_MONITOR_ENABLED.key, "false")
      .set(s"celeborn.$module.io.mode", "NIO")
      .set(s"celeborn.worker.$module.port", port.toString)
      .set(s"celeborn.$module.server.bootstrap.classes", bootstrap)
    if (factoryFailure) {
      conf.set("celeborn.replicate.io.clientThreads", "-1")
        .set("celeborn.replicate.client.bootstrap.classes", bootstrap)
    }
    try {
      var failure: Throwable = null
      try {
        new Worker(
          conf,
          new WorkerArguments(
            Array("--host", "localhost", "--port", "0", "--properties-file", properties.toString),
            conf))
      } catch {
        case NonFatal(e) => failure = e
      }
      assert(failure != null, s"Expected $scenario startup failure")
      val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toSeq
      if (factoryFailure) {
        assert(
          causes.exists(e =>
            e.isInstanceOf[IllegalArgumentException] &&
              Option(e.getMessage).exists(_.contains("-1"))),
          failure.toString)
      } else {
        assert(causes.exists(_.isInstanceOf[java.net.BindException]), failure.toString)
      }
      val counts = StartupCountingBootstrap.closeCounts(module)
      assert(counts.size() == (if (factoryFailure) 2 else 1), counts.toString)
      assert(counts.values().asScala.forall(_ == 1), s"$module close counts: $counts")
      if (factoryFailure) {
        val rebound = new ServerSocket(port)
        rebound.close()
      }
      println(s"$scenario: configured instances closed once; original startup failure retained")
    } finally {
      occupied.close()
      JavaUtils.deleteRecursively(directory.toFile)
    }
  }
}
