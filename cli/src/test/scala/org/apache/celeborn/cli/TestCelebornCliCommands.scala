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

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Base64

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.cli.config.CliConfigManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.HttpAuthSchemes
import org.apache.celeborn.server.common.http.authentication.{UserDefinePasswordAuthenticationProviderImpl, UserDefineTokenAuthenticationProviderImpl}
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker

class TestCelebornCliCommands extends CelebornFunSuite with MiniClusterFeature {

  private val CELEBORN_ADMINISTER = "celeborn"
  private val celebornConf = new CelebornConf()
    .set(CelebornConf.MASTER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC"))
    .set(
      CelebornConf.MASTER_HTTP_AUTH_BASIC_PROVIDER,
      classOf[UserDefinePasswordAuthenticationProviderImpl].getName)
    .set(CelebornConf.WORKER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC"))
    .set(
      CelebornConf.WORKER_HTTP_AUTH_BASIC_PROVIDER,
      classOf[UserDefinePasswordAuthenticationProviderImpl].getName)
    .set(CelebornConf.MASTER_HTTP_AUTH_ADMINISTERS, Seq(CELEBORN_ADMINISTER))
    .set(CelebornConf.WORKER_HTTP_AUTH_ADMINISTERS, Seq(CELEBORN_ADMINISTER))

  private val BASIC_AUTH_HEADER = HttpAuthSchemes.BASIC + " " + new String(
    Base64.getEncoder.encode(
      s"$CELEBORN_ADMINISTER:${UserDefinePasswordAuthenticationProviderImpl.VALID_PASSWORD}".getBytes()),
    StandardCharsets.UTF_8)

  protected var master: Master = _
  protected var worker: Worker = _

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup celeborn mini cluster")
    val (m, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    master = m
    worker = w.head
    super.beforeAll()
    val aliasCommand = Array(
      "master",
      "--add-cluster-alias",
      "unit-test",
      "--host-list",
      master.connectionUrl)
    captureOutputAndValidateResponse(
      aliasCommand,
      s"Cluster alias unit-test added to ${CliConfigManager.cliConfigFilePath}")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    logInfo("all test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
    val removeAliasCommand = Array(
      "master",
      "--remove-cluster-alias",
      "unit-test")
    captureOutputAndValidateResponse(removeAliasCommand, s"Cluster alias unit-test removed.")
    val cliConfigManager = new CliConfigManager
    val aliasExists = cliConfigManager.loadConfig().exists(_.cliConfigData.contains("unit-test"))
    assert(!aliasExists)
    if (new File(CliConfigManager.cliConfigFilePath).exists()) {
      Files.delete(Paths.get(CliConfigManager.cliConfigFilePath))
    }

  }

  test("worker --show-worker-info") {
    val args = prepareWorkerArgs() :+ "--show-worker-info"
    captureOutputAndValidateResponse(args, "WorkerInfoResponse")
  }

  test("worker --show-apps-on-worker") {
    val args = prepareWorkerArgs() :+ "--show-apps-on-worker"
    captureOutputAndValidateResponse(args, "ApplicationsResponse")
  }

  test("worker --show-shuffles-on-worker") {
    val args = prepareWorkerArgs() :+ "--show-shuffles-on-worker"
    captureOutputAndValidateResponse(args, "ShufflesResponse")
  }

  test("worker --show-partition-location-info") {
    val args = prepareWorkerArgs() :+ "--show-partition-location-info"
    captureOutputAndValidateResponse(args, "ShufflePartitionsResponse")
  }

  test("worker --show-unavailable-peers") {
    val args = prepareWorkerArgs() :+ "--show-unavailable-peers"
    captureOutputAndValidateResponse(args, "UnAvailablePeersResponse")
  }

  test("worker --is-shutdown") {
    val args = prepareWorkerArgs() :+ "--is-shutdown"
    captureOutputAndValidateResponse(args, "false")
  }

  test("worker --is-decommissioning") {
    val args = prepareWorkerArgs() :+ "--is-decommissioning"
    captureOutputAndValidateResponse(args, "false")
  }

  test("worker --is-registered") {
    val args = prepareWorkerArgs() :+ "--is-registered"
    captureOutputAndValidateResponse(args, "true")
  }

  test("worker --show-conf") {
    val args = prepareWorkerArgs() :+ "--show-conf"
    captureOutputAndValidateResponse(args, "ConfResponse")
  }

  test("worker --show-container-info") {
    val args = prepareWorkerArgs() :+ "--show-container-info"
    captureOutputAndValidateResponse(args, "ContainerInfo")
  }

  test("worker --show-dynamic-conf") {
    cancel("This test is temporarily disabled since dynamic conf is not enabled in unit tests.")
    val args = prepareWorkerArgs() :+ "--show-dynamic-conf"
    captureOutputAndValidateResponse(args, "")
  }

  test("worker --show-thread-dump") {
    val args = prepareWorkerArgs() :+ "--show-thread-dump"
    captureOutputAndValidateResponse(args, "ThreadStackResponse")
  }

  test("master --show-masters-info") {
    cancel("This test is temporarily disabled since HA is not enabled in the unit tests.")
    val args = prepareMasterArgs() :+ "--show-masters-info"
    captureOutputAndValidateResponse(args, "")
  }

  test("master --show-cluster-apps") {
    val args = prepareMasterArgs() :+ "--show-cluster-apps"
    captureOutputAndValidateResponse(args, "ApplicationsHeartbeatResponse")
  }

  test("master --show-cluster-shuffles") {
    val args = prepareMasterArgs() :+ "--show-cluster-shuffles"
    captureOutputAndValidateResponse(args, "ShufflesResponse")
  }

  test("master --show-worker-event-info") {
    val args = prepareMasterArgs() :+ "--show-worker-event-info"
    captureOutputAndValidateResponse(args, "WorkerEventsResponse")
  }

  test("master --show-lost-workers") {
    val args = prepareMasterArgs() :+ "--show-lost-workers"
    captureOutputAndValidateResponse(args, "No lost workers found.")
  }

  test("master --show-excluded-workers") {
    val args = prepareMasterArgs() :+ "--show-excluded-workers"
    captureOutputAndValidateResponse(args, "No excluded workers found.")
  }

  test("master --show-manual-excluded-workers") {
    val args = prepareMasterArgs() :+ "--show-manual-excluded-workers"
    captureOutputAndValidateResponse(args, "No manual excluded workers found.")
  }

  test("master --show-shutdown-workers") {
    val args = prepareMasterArgs() :+ "--show-shutdown-workers"
    captureOutputAndValidateResponse(args, "No shutdown workers found.")
  }

  test("master --show-decommissioning-workers") {
    val args = prepareMasterArgs() :+ "--show-decommissioning-workers"
    captureOutputAndValidateResponse(args, "No decommissioning workers found.")
  }

  test("master --show-lifecycle-managers") {
    val args = prepareMasterArgs() :+ "--show-lifecycle-managers"
    captureOutputAndValidateResponse(args, "HostnamesResponse")
  }

  test("master --show-workers") {
    val args = prepareMasterArgs() :+ "--show-workers"
    captureOutputAndValidateResponse(args, "WorkersResponse")
  }

  test("master --show-workers-topology") {
    val args = prepareMasterArgs() :+ "--show-workers-topology"
    captureOutputAndValidateResponse(args, "TopologyResponse")
  }

  test("master --show-conf") {
    val args = prepareMasterArgs() :+ "--show-conf"
    captureOutputAndValidateResponse(args, "ConfResponse")
  }

  test("master --show-container-info") {
    val args = prepareMasterArgs() :+ "--show-container-info"
    captureOutputAndValidateResponse(args, "ContainerInfo")
  }

  test("master --show-dynamic-conf") {
    cancel("This test is temporarily disabled since dynamic conf is not enabled in unit tests.")
    val args = prepareMasterArgs() :+ "--show-dynamic-conf"
    captureOutputAndValidateResponse(args, "")
  }

  test("master --show-thread-dump") {
    val args = prepareMasterArgs() :+ "--show-thread-dump"
    captureOutputAndValidateResponse(args, "ThreadStackResponse")
  }

  test("master --exclude-worker and --remove-excluded-worker") {
    val excludeArgs = prepareMasterArgs() ++ Array(
      "--exclude-worker",
      "--worker-ids",
      getWorkerId())
    captureOutputAndValidateResponse(excludeArgs, "success: true")
    val removeExcludedArgs = prepareMasterArgs() ++ Array(
      "--remove-excluded-worker",
      "--worker-ids",
      getWorkerId())
    captureOutputAndValidateResponse(removeExcludedArgs, "success: true")
  }

  test("master --send-worker-event") {
    val args = prepareMasterArgs() ++ Array(
      "--send-worker-event",
      "RECOMMISSION",
      "--worker-ids",
      getWorkerId())
    captureOutputAndValidateResponse(args, "success: true")
  }

  test("master --remove-workers-unavailable-info") {
    val args = prepareMasterArgs() ++ Array(
      "--remove-workers-unavailable-info",
      "--worker-ids",
      getWorkerId())
    captureOutputAndValidateResponse(args, "success: true")
  }

  test("master --delete-apps case1") {
    val args = prepareMasterArgs() ++ Array(
      "--delete-apps",
      "--apps",
      "app1")
    captureOutputAndValidateResponse(args, "success: true")
  }

  test("master --delete-apps case2") {
    val args = prepareMasterArgs() ++ Array(
      "--delete-apps",
      "--apps",
      "app1,app2")
    captureOutputAndValidateResponse(args, "success: true")
  }

  test("master --revise-lost-shuffles case1") {
    val args = prepareMasterArgs() ++ Array(
      "--revise-lost-shuffles",
      "--apps",
      "app1",
      "--shuffleIds",
      "1,2,3,4,5,6")
    captureOutputAndValidateResponse(args, "success: true")
  }

  private def prepareMasterArgs(): Array[String] = {
    Array(
      "master",
      "--cluster",
      "unit-test",
      "--auth-header",
      BASIC_AUTH_HEADER)
  }

  private def prepareWorkerArgs(): Array[String] = {
    Array(
      "worker",
      "--hostport",
      worker.connectionUrl,
      "--auth-header",
      BASIC_AUTH_HEADER)
  }

  private def captureOutputAndValidateResponse(
      args: Array[String],
      stdoutValidationString: String): Unit = {
    val stdoutStream = new ByteArrayOutputStream()
    val stdoutPrintStream = new PrintStream(stdoutStream)
    Console.withOut(stdoutPrintStream) {
      CelebornCli.main(args)
    }
    val stdout = stdoutStream.toString
    assert(stdout.nonEmpty && stdout.contains(stdoutValidationString))
  }

  private def getWorkerId(): String = {
    s"${worker.workerArgs.host}:${worker.rpcEnv.address.port}:${worker.getPushFetchServerPort._1}" +
      s":${worker.getPushFetchServerPort._2}:${worker.replicateServer.getPort}"
  }
}
