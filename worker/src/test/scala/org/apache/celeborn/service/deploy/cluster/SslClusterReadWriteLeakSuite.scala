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

package org.apache.celeborn.service.deploy.cluster

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.util.{ResourceLeakDetector, ResourceLeakDetectorFactory}
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.ssl.SslSampleConfigs
import org.apache.celeborn.common.protocol.{CompressionCodec, TransportModuleConstants}
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * Integration test verifying that the SslMessageEncoder memory-leak fix holds under a realistic
 * mini-cluster workload with TLS enabled on every transport channel.
 *
 * <p>The test installs a custom Netty ResourceLeakDetector in PARANOID mode before the cluster
 * starts, runs a full push+replicate+read shuffle cycle with payloads large enough to require
 * multi-record TLS framing (> 16 KB per push), then forces GC and asserts that the detector
 * reported zero leaks.
 */
class SslClusterReadWriteLeakSuite
  extends AnyFunSuite
  with MiniClusterFeature
  with BeforeAndAfterAll
  with Logging {

  private val reportedLeaks = new AtomicInteger(0)

  private var previousLeakLevel: ResourceLeakDetector.Level = _
  private var previousLeakFactory: ResourceLeakDetectorFactory = _
  private var testMasterPort: Int = _

  private lazy val serverSslConf: Map[String, String] = {
    val modules = Seq(
      TransportModuleConstants.PUSH_MODULE,
      TransportModuleConstants.REPLICATE_MODULE,
      TransportModuleConstants.FETCH_MODULE)
    modules
      .flatMap(m => SslSampleConfigs.createDefaultConfigMapForModule(m).asScala.toSeq)
      .toMap
  }

  override def beforeAll(): Unit = {

    // Capture the original leak detection settings so we can restore them in afterAll().
    previousLeakLevel = ResourceLeakDetector.getLevel
    previousLeakFactory = ResourceLeakDetectorFactory.instance()

    // Install the leak-counting detector BEFORE any Netty buffers are allocated so that
    // AbstractByteBuf.leakDetector (a static final field) is initialized with our instance
    // rather than the default one.
    installLeakCountingDetector()

    testMasterPort = selectRandomPort()
    val masterInternalPort = selectRandomPort()

    val masterConf = Map(
      CelebornConf.MASTER_HOST.key -> "localhost",
      CelebornConf.PORT_MAX_RETRY.key -> "0",
      CelebornConf.MASTER_PORT.key -> testMasterPort.toString,
      CelebornConf.MASTER_ENDPOINTS.key -> s"localhost:$testMasterPort",
      CelebornConf.MASTER_INTERNAL_PORT.key -> masterInternalPort.toString,
      CelebornConf.MASTER_INTERNAL_ENDPOINTS.key -> s"localhost:$masterInternalPort") ++ serverSslConf

    val workerConf = Map(
      CelebornConf.MASTER_ENDPOINTS.key -> s"localhost:$testMasterPort",
      CelebornConf.MASTER_INTERNAL_ENDPOINTS.key -> s"localhost:$masterInternalPort") ++ serverSslConf

    setupMiniClusterWithRandomPorts(masterConf, workerConf)
  }

  override def afterAll(): Unit = {
    shutdownMiniCluster()
    // Restore the original leak detection settings so that other suites running
    // in the same JVM (forkMode=once) are not affected.
    ResourceLeakDetector.setLevel(previousLeakLevel)
    ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(previousLeakFactory)
  }

  // ---------------------------------------------------------------------------

  test("SSL mini-cluster: push+replicate+fetch large data produces no ByteBuf memory leaks") {
    // Verify that our custom leak-counting detector is the one actually installed in
    // AbstractByteBuf.leakDetector (a static final field). In a shared JVM (scalatest
    // forkMode=once), an earlier suite may have triggered class loading of AbstractByteBuf,
    // causing the default detector to be installed instead of ours. In that case, skip
    // the test rather than silently producing false negatives.
    val field = classOf[io.netty.buffer.AbstractByteBuf].getDeclaredField("leakDetector")
    field.setAccessible(true)
    val detector = field.get(null)
    assume(
      detector.getClass.getEnclosingClass == classOf[SslClusterReadWriteLeakSuite],
      "Leak-counting detector is not active — AbstractByteBuf.leakDetector was " +
        "initialised by an earlier test in this JVM. Skipping leak assertions.")

    val app = "app-ssl-leak-test"
    val clientConf = buildSslClientConf(app)
    val lifecycleManager = new LifecycleManager(app, clientConf)
    val shuffleClient = new ShuffleClientImpl(app, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    try {
      // Payloads > 16 KB force TransportFrameDecoder.decodeNext() to assemble a
      // CompositeByteBuf from multiple TLS records – this is to prevent a leaked direct ByteBuf
      // in SslMessageEncoder.encode().
      val payload32k = RandomStringUtils.random(32 * 1024).getBytes(StandardCharsets.UTF_8)
      val payload64k = RandomStringUtils.random(64 * 1024).getBytes(StandardCharsets.UTF_8)
      val payloadSmall = RandomStringUtils.random(512).getBytes(StandardCharsets.UTF_8)

      // Push via the primary push path (exercises push + replicate channels).
      shuffleClient.pushData(1, 0, 0, 0, payload32k, 0, payload32k.length, 1, 1)
      shuffleClient.pushData(1, 0, 0, 0, payload64k, 0, payload64k.length, 1, 1)

      // Also exercise mergeData + pushMergedData.
      shuffleClient.mergeData(1, 0, 0, 0, payload32k, 0, payload32k.length, 1, 1)
      shuffleClient.mergeData(1, 0, 0, 0, payloadSmall, 0, payloadSmall.length, 1, 1)
      shuffleClient.pushMergedData(1, 0, 0)
      Thread.sleep(500)

      shuffleClient.mapperEnd(1, 0, 0, 1, 1)

      // Read back via the fetch channel and verify total byte count.
      val expectedBytes =
        payload32k.length + payload64k.length + payload32k.length + payloadSmall.length

      val metricsCallback = new MetricsCallback {
        override def incBytesRead(bytesWritten: Long): Unit = {}
        override def incReadTime(time: Long): Unit = {}
      }
      val inputStream =
        shuffleClient.readPartition(1, 0, 0, 0L, 0, Integer.MAX_VALUE, metricsCallback)
      try {
        val output = new ByteArrayOutputStream()
        var b = inputStream.read()
        while (b != -1) {
          output.write(b)
          b = inputStream.read()
        }

        Assert.assertEquals(expectedBytes, output.size())
      } finally {
        inputStream.close()
      }
    } finally {
      Thread.sleep(2000) // let in-flight replication finish before shutdown
      shuffleClient.shutdown()
      lifecycleManager.rpcEnv.shutdown()
    }

    // Trigger GC and make Netty poll its queue.
    triggerLeakDetection()

    Assert.assertEquals(0, reportedLeaks.get())
  }

  /**
   * Installs a custom ResourceLeakDetectorFactory whose detectors override
   * reportTracedLeak/reportUntracedLeak to count every leak report in reportedLeaks.
   * Must be called before any ByteBuf is allocated so that AbstractByteBuf.leakDetector
   * (static final) is initialised with our instance.
   */
  private def installLeakCountingDetector(): Unit = {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    val counter = reportedLeaks
    ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(
      new ResourceLeakDetectorFactory() {
        override def newResourceLeakDetector[T](
            resource: Class[T],
            samplingInterval: Int,
            maxActive: Long): ResourceLeakDetector[T] = {
          new ResourceLeakDetector[T](resource, samplingInterval) {
            override protected def reportTracedLeak(
                resourceType: String,
                records: String): Unit = {
              super.reportTracedLeak(resourceType, records)
              counter.incrementAndGet()
            }
            override protected def reportUntracedLeak(resourceType: String): Unit = {
              super.reportUntracedLeak(resourceType)
              counter.incrementAndGet()
            }
          }
        }
      })
  }

  /**
   * Builds client CelebornConf with SSL enabled on the "data" module, matching the production
   * client-side configuration (spark.celeborn.ssl.data.enabled=true).  ShuffleClientImpl uses the
   * DATA_MODULE ("data") for all its data-plane connections (push + fetch) to workers.
   */
  private def buildSslClientConf(app: String): CelebornConf = {
    val clientSslConf =
      SslSampleConfigs.createDefaultConfigMapForModule(
        TransportModuleConstants.DATA_MODULE).asScala.toMap

    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$testMasterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
    clientSslConf.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * Forces several rounds of GC and allocates direct buffers in between so that Netty's
   * ResourceLeakDetector (PARANOID mode) polls its PhantomReference queue and reports any
   * buffers that were GC'd without being released.
   */
  private def triggerLeakDetection(): Unit = {
    for (_ <- 1 to 5) {
      System.gc()
      System.runFinalization()
      Thread.sleep(500)
      // Each directBuffer() allocation causes the detector to drain its ref queue.
      val bufs = (1 to 200).map(_ => UnpooledByteBufAllocator.DEFAULT.directBuffer(1))
      bufs.foreach(_.release())
      Thread.sleep(200)
    }
  }
}
