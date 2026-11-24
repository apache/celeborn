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

package org.apache.celeborn.service.deploy.master.quota

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.source.{ResourceConsumptionSource, Role}
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckQuotaResponse
import org.apache.celeborn.common.quota.{ResourceConsumption, StorageQuota}
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.service.config.{ConfigService, FsConfigServiceImpl}
import org.apache.celeborn.service.deploy.master.MasterSource
import org.apache.celeborn.service.deploy.master.clustermeta.{AbstractMetaManager, SingleMasterMetaManager}

class QuotaManagerAppQuotaSuite extends CelebornFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private val jerry = UserIdentifier("tenant_01", "Jerry")
  private val tom = UserIdentifier("tenant_01", "Tom")
  private val userWithZeroAppQuota = UserIdentifier("tenant_02", "Tom")
  private val userWithoutAppQuota = UserIdentifier("tenant_03", "Spike")
  private val userWithDiskOnlyAppQuota = UserIdentifier("tenant_04", "Mike")
  private val interruptEnabled =
    Map(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED.key -> "true").asJava
  private val interruptDisabled =
    Map(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED.key -> "false").asJava

  private var rpcEnv: RpcEnv = _
  private var conf: CelebornConf = _
  private var configService: ConfigService = _
  private var statusSystem: AbstractMetaManager = _
  private var worker: WorkerInfo = _
  private var masterSource: MasterSource = _
  private var quotaManager: QuotaManager = _

  override def beforeAll(): Unit = {
    rpcEnv = RpcEnv.create(
      "test-rpc",
      "rpc",
      "localhost",
      9004,
      new CelebornConf(),
      "master",
      None)
  }

  override def afterAll(): Unit = {
    rpcEnv.shutdown()
  }

  override def beforeEach(): Unit = {
    createQuotaManager("dynamicConfig-app-quota.yaml")
  }

  override def afterEach(): Unit = {
    configService.shutdown()
  }

  private def createQuotaManager(dynamicConfigFile: String): Unit = {
    // A test may replace the manager that beforeEach created, so close its config service first.
    if (configService != null) {
      configService.shutdown()
    }
    conf = new CelebornConf()
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile(dynamicConfigFile).getPath)
    conf.set(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED, true)
    configService = new FsConfigServiceImpl(conf)
    statusSystem = new SingleMasterMetaManager(rpcEnv, conf)
    worker = new WorkerInfo("localhost", 10001, 10002, 10003, 10004)
    statusSystem.availableWorkers.add(worker)
    masterSource = new MasterSource(conf)
    quotaManager = new QuotaManager(
      statusSystem,
      masterSource,
      new ResourceConsumptionSource(conf, Role.MASTER),
      conf,
      configService)
  }

  test("test app storage quota falls back from user to tenant level") {
    configService.refreshCache()

    assert(quotaManager.getAppStorageQuota(jerry) ==
      StorageQuota(Utils.byteStringAsBytes("10G"), 100, Utils.byteStringAsBytes("2G"), 10))
    assert(quotaManager.getAppStorageQuota(tom) ==
      StorageQuota(
        Utils.byteStringAsBytes("1G"),
        100,
        Utils.byteStringAsBytes("2G"),
        10))
  }

  test("test app over the quota is not marked when the system dynamic config disables it") {
    createQuotaManager("dynamicConfig-app-quota-disabled.yaml")
    conf.set(CelebornConf.APP_QUOTA_ENABLED, true)
    registerApps(jerry, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("app1" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))

    assert(!quotaManager.appQuotaEnabled)
    assert(
      quotaManager.getAppStorageQuota(jerry).diskBytesWritten == Utils.byteStringAsBytes("1G"))
    assert(markedAppIds.isEmpty)
    assertMarkedAppCount(masterSource, 0)
  }

  Seq(
    (
      "disk bytes written",
      usage(diskBytes = Utils.byteStringAsBytes("10G")),
      usage(diskBytes = Utils.byteStringAsBytes("10G") - 1)),
    ("disk file count", usage(diskFiles = 100), usage(diskFiles = 99)),
    (
      "hdfs bytes written",
      usage(hdfsBytes = Utils.byteStringAsBytes("2G")),
      usage(hdfsBytes = Utils.byteStringAsBytes("2G") - 1)),
    ("hdfs file count", usage(hdfsFiles = 10), usage(hdfsFiles = 9))).foreach {
    case (quotaName, atQuota, belowQuota) =>
      test(s"test app is marked at exactly the $quotaName quota but not below it") {
        registerApps(jerry, "atQuotaApp", "belowQuotaApp")
        configService.refreshCache()

        quotaManager.checkAppResourceConsumption(
          jerry,
          Seq(usageOfApps("atQuotaApp" -> atQuota, "belowQuotaApp" -> belowQuota)))

        assert(markedAppIds == Set("atQuotaApp"))
      }
  }

  test("test app exceeding every quota reports each exceeded quota") {
    registerApps(jerry, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("app1" -> usage(
        Utils.byteStringAsBytes("11G"),
        101,
        Utils.byteStringAsBytes("3G"),
        11))))

    assert(checkApplicationQuota("app1") ==
      CheckQuotaResponse(
        false,
        appExhaustedReason(
          jerry,
          "DISK_BYTES_WRITTEN(11.0 GiB) exceeds quota(10.0 GiB). " +
            "DISK_FILE_COUNT(101) exceeds quota(100). " +
            "HDFS_BYTES_WRITTEN(3.0 GiB) exceeds quota(2.0 GiB). " +
            "HDFS_FILE_COUNT(11) exceeds quota(10). ")))
  }

  test("test each app is checked against the app quota of its owner") {
    registerApps(jerry, "jerrySmallApp", "jerryLargeApp")
    registerApps(tom, "tomSmallApp", "tomLargeApp")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps(
        "jerrySmallApp" -> usage(diskBytes = Utils.byteStringAsBytes("1G")),
        "jerryLargeApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))
    quotaManager.checkAppResourceConsumption(
      tom,
      Seq(usageOfApps(
        "tomSmallApp" -> usage(diskBytes = Utils.byteStringAsBytes("512M")),
        "tomLargeApp" -> usage(diskBytes = Utils.byteStringAsBytes("3G")))))

    assert(markedAppIds == Set("jerryLargeApp", "tomLargeApp"))
    assert(checkApplicationQuota("jerryLargeApp").reason ==
      appExhaustedReason(jerry, "DISK_BYTES_WRITTEN(11.0 GiB) exceeds quota(10.0 GiB). "))
    val tomLimit = Utils.bytesToString(Utils.byteStringAsBytes("1G"))
    assert(checkApplicationQuota("tomLargeApp").reason ==
      appExhaustedReason(tom, s"DISK_BYTES_WRITTEN(3.0 GiB) exceeds quota($tomLimit). "))
  }

  test("test app is not marked when the app quota is unlimited") {
    registerApps(userWithoutAppQuota, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      userWithoutAppQuota,
      Seq(usageOfApps("app1" -> usage(
        Utils.byteStringAsBytes("100G"),
        10000,
        Utils.byteStringAsBytes("100G"),
        10000))))

    assert(markedAppIds.isEmpty)
    assertMarkedAppCount(masterSource, 0)
  }

  test("test app is not marked when the app quota limits are zero") {
    registerApps(userWithZeroAppQuota, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      userWithZeroAppQuota,
      Seq(usageOfApps("app1" -> usage(
        Utils.byteStringAsBytes("100G"),
        10000,
        Utils.byteStringAsBytes("100G"),
        10000))))

    assert(markedAppIds.isEmpty)
    assertMarkedAppCount(masterSource, 0)
  }

  test("test app quota with only the disk bytes limit set marks at that limit") {
    registerApps(userWithDiskOnlyAppQuota, "overDiskApp")
    configService.refreshCache()

    assert(quotaManager.getAppStorageQuota(userWithDiskOnlyAppQuota) ==
      StorageQuota(Utils.byteStringAsBytes("5G"), Long.MaxValue, Long.MaxValue, Long.MaxValue))

    quotaManager.checkAppResourceConsumption(
      userWithDiskOnlyAppQuota,
      Seq(usageOfApps("overDiskApp" -> usage(diskBytes = Utils.byteStringAsBytes("5G")))))

    assert(markedAppIds == Set("overDiskApp"))
    assertMarkedAppCount(masterSource, 1)
  }

  test("test app quota switched off at runtime stops marking apps") {
    registerApps(jerry, "markedApp", "laterApp")
    configService.refreshCache()
    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("markedApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))

    conf.set(CelebornConf.APP_QUOTA_ENABLED, false)
    configService.refreshCache()
    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps(
        "markedApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")),
        "laterApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))

    assert(markedAppIds == Set("markedApp"))
  }

  test("test app is not marked when interrupt shuffle is disabled in the cluster") {
    conf.set(CelebornConf.QUOTA_INTERRUPT_SHUFFLE_ENABLED, false)
    registerApps(jerry, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("app1" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))

    assert(markedAppIds.isEmpty)
    assertMarkedAppCount(masterSource, 0)
  }

  test("test app that did not enable interrupt shuffle is not marked") {
    registerApps(jerry, "interruptibleApp")
    statusSystem.updateApplicationInfo("optedOutApp", jerry, interruptDisabled)
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps(
        "interruptibleApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")),
        "optedOutApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")),
        "unregisteredApp" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))

    assert(markedAppIds == Set("interruptibleApp"))
  }

  test("test app that is already marked keeps its first reason") {
    registerApps(jerry, "app1")
    configService.refreshCache()

    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("app1" -> usage(diskBytes = Utils.byteStringAsBytes("11G")))))
    quotaManager.checkAppResourceConsumption(
      jerry,
      Seq(usageOfApps("app1" -> usage(
        diskBytes = Utils.byteStringAsBytes("12G"),
        hdfsBytes = Utils.byteStringAsBytes("2G")))))

    assert(checkApplicationQuota("app1").reason ==
      appExhaustedReason(jerry, "DISK_BYTES_WRITTEN(11.0 GiB) exceeds quota(10.0 GiB). "))
  }

  test("test app usage from several workers is summed by the resource consumption update") {
    val secondWorker = new WorkerInfo("localhost", 10011, 10012, 10013, 10014)
    statusSystem.availableWorkers.add(secondWorker)
    registerApps(jerry, "spreadApp")
    addUserConsumption(
      jerry,
      usageOfApps("spreadApp" -> usage(diskBytes = Utils.byteStringAsBytes("6G"))))
    secondWorker.userResourceConsumption.put(
      jerry,
      usageOfApps("spreadApp" -> usage(diskBytes = Utils.byteStringAsBytes("5G"))))
    configService.refreshCache()

    quotaManager.updateResourceConsumption()

    assert(markedAppIds == Set("spreadApp"))
  }

  test("test user quota does not mark more apps after the app quota marked the excess") {
    registerApps(jerry, "appOverQuota", "appUnderQuota")
    addUserConsumption(
      jerry,
      usageOfApps(
        "appOverQuota" -> usage(diskBytes = Utils.byteStringAsBytes("15G")),
        "appUnderQuota" -> usage(diskBytes = Utils.byteStringAsBytes("9G"))))
    configService.refreshCache()

    quotaManager.updateResourceConsumption()

    assert(markedAppIds == Set("appOverQuota"))
    assert(checkApplicationQuota("appOverQuota").reason.startsWith(
      QuotaStatus.APPLICATION_EXHAUSTED))
    assert(!quotaManager.checkUserQuotaStatus(jerry).isAvailable)
  }

  test(
    "test user quota still marks apps after the app quota marks, and each mark is counted once") {
    registerApps(
      jerry,
      "appOverQuota",
      "largestUnderQuotaApp",
      "secondUnderQuotaApp",
      "thirdUnderQuotaApp")
    addUserConsumption(
      jerry,
      usageOfApps(
        "appOverQuota" -> usage(diskBytes = Utils.byteStringAsBytes("15G")),
        "largestUnderQuotaApp" -> usage(diskBytes = Utils.byteStringAsBytes("8G")),
        "secondUnderQuotaApp" -> usage(diskBytes = Utils.byteStringAsBytes("7G")),
        "thirdUnderQuotaApp" -> usage(diskBytes = Utils.byteStringAsBytes("6G"))))
    configService.refreshCache()

    quotaManager.updateResourceConsumption()

    assert(markedAppIds == Set("appOverQuota", "largestUnderQuotaApp"))
    assert(checkApplicationQuota("appOverQuota").reason.startsWith(
      QuotaStatus.APPLICATION_EXHAUSTED))
    assert(checkApplicationQuota("largestUnderQuotaApp").reason.startsWith(
      QuotaStatus.USER_EXHAUSTED))
    assertMarkedAppCount(masterSource, 2)

    quotaManager.updateResourceConsumption()
    assertMarkedAppCount(masterSource, 2)
  }

  Seq(
    ("tenant", "celeborn.quota.tenant.diskBytesWritten"),
    ("cluster", "celeborn.quota.cluster.diskBytesWritten")).foreach {
    case (scope, quotaKey) =>
      test(
        s"test $scope quota does not mark more apps after the app quota marked the excess") {
        conf.set(quotaKey, "6gb")
        registerApps(jerry, "appOverQuota", "appUnderQuota")
        addUserConsumption(
          jerry,
          usageOfApps(
            "appOverQuota" -> usage(diskBytes = Utils.byteStringAsBytes("11G")),
            "appUnderQuota" -> usage(diskBytes = Utils.byteStringAsBytes("2G"))))
        configService.refreshCache()

        quotaManager.updateResourceConsumption()

        assert(markedAppIds == Set("appOverQuota"))
        assert(checkApplicationQuota("appOverQuota").reason.startsWith(
          QuotaStatus.APPLICATION_EXHAUSTED))
        // The wider scope stays exceeded, so new applications are still rejected.
        assert(!quotaManager.checkUserQuotaStatus(jerry).isAvailable)
      }
  }

  test("test marked app counter counts apps marked by the app quota once") {
    registerApps(jerry, "app1", "app2", "app3")
    addUserConsumption(
      jerry,
      usageOfApps(
        "app1" -> usage(diskBytes = Utils.byteStringAsBytes("11G")),
        "app2" -> usage(diskBytes = Utils.byteStringAsBytes("12G")),
        "app3" -> usage(diskBytes = Utils.byteStringAsBytes("1G"))))
    configService.refreshCache()

    quotaManager.updateResourceConsumption()
    assertMarkedAppCount(masterSource, 2)

    quotaManager.updateResourceConsumption()
    assertMarkedAppCount(masterSource, 2)
  }

  private def registerApps(user: UserIdentifier, appIds: String*): Unit = {
    appIds.foreach(statusSystem.updateApplicationInfo(_, user, interruptEnabled))
  }

  private def usage(
      diskBytes: Long = 0,
      diskFiles: Long = 0,
      hdfsBytes: Long = 0,
      hdfsFiles: Long = 0): ResourceConsumption = {
    ResourceConsumption(diskBytes, diskFiles, hdfsBytes, hdfsFiles)
  }

  private def usageOfApps(apps: (String, ResourceConsumption)*): ResourceConsumption = {
    apps.map(_._2).foldLeft(usage())(_ add _).withSubResourceConsumptions(apps.toMap.asJava)
  }

  private def addUserConsumption(user: UserIdentifier, consumption: ResourceConsumption): Unit = {
    worker.userResourceConsumption.put(user, consumption)
  }

  private def checkApplicationQuota(applicationId: String): CheckQuotaResponse = {
    quotaManager.checkApplicationQuotaStatus(applicationId)
  }

  private def markedAppIds: Set[String] = quotaManager.appQuotaStatus.asScala.keySet.toSet

  private def metricsLabels: String =
    s"""instance="${Utils.localHostName(conf)}:${conf.masterHttpPort}",role="Master""""

  private def assertMarkedAppCount(source: MasterSource, expected: Int): Unit = {
    assert(source.getMetrics.contains(
      s"""metrics_QuotaInterruptMarkedAppCount_Count{$metricsLabels} $expected """))
  }

  private def appExhaustedReason(user: UserIdentifier, exceededQuotas: String): String = {
    s"Application storage usage reach threshold. User: $user.  $exceededQuotas"
  }
}
