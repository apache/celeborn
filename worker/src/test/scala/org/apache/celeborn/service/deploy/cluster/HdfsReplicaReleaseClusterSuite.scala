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
import java.nio.file.{Files, Path => JPath}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Progressable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar._

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.{PartitionLocation, StorageInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * Local filesystem standing in for HDFS. For [[SlowPrimaryFileSystem.App]] it stalls the
 * primary's index past the worker commit timeout, and holds the replica's final flush until the
 * primary has stalled, so the replica always finishes closing second.
 */
class SlowPrimaryFileSystem extends RawLocalFileSystem {
  import SlowPrimaryFileSystem._

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (inApp(f) && f.getName.endsWith(s"-0${Utils.INDEX_SUFFIX}")) {
      primaryStalled.countDown()
      Thread.sleep(8000)
    }
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    if (inApp(f) && f.getName.endsWith("-1")) primaryStalled.await(30, TimeUnit.SECONDS)
    super.append(f, bufferSize, progress)
  }
}

object SlowPrimaryFileSystem {
  val App = "app-commit-timeout"
  private val primaryStalled = new CountDownLatch(1)

  private def inApp(f: Path): Boolean = f.toString.contains(s"/$App/")
}

class HdfsReplicaReleaseClusterSuite extends AnyFunSuite with MiniClusterFeature
  with BeforeAndAfterAll {

  private val hdfsRoot: JPath = Files.createTempDirectory("celeborn-hdfs")
  private val storageConf = Map(
    CelebornConf.ACTIVE_STORAGE_TYPES.key -> "HDFS",
    CelebornConf.HDFS_DIR.key -> hdfsRoot.toUri.toString,
    // The checksummed local filesystem does not support the appends the HDFS flusher uses.
    "celeborn.hadoop.fs.file.impl" -> classOf[SlowPrimaryFileSystem].getName,
    "celeborn.hadoop.fs.file.impl.disable.cache" -> "true")
  private var masterPort = 0

  override def beforeAll(): Unit = {
    val workerConf = storageConf + (CelebornConf.WORKER_SHUFFLE_COMMIT_TIMEOUT.key -> "3s")
    val (master, _) = setupMiniClusterWithRandomPorts(storageConf, workerConf)
    masterPort = master.conf.masterPort
  }

  override def afterAll(): Unit = {
    shutdownMiniCluster()
    FileUtils.deleteDirectory(hdfsRoot.toFile)
  }

  // Shuffle file names end with the location mode: 0 for primary, 1 for replica.
  private def shuffleFiles(app: String, mode: Int): Seq[String] = {
    val files = Files.walk(hdfsRoot)
    try {
      files.iterator().asScala
        .filter(p => Files.isRegularFile(p) && p.toString.contains(s"/$app/"))
        .map(_.getFileName.toString)
        .filter(_.matches(s"\\d+-\\d+-$mode(\\..*)?"))
        .toList
    } finally {
      files.close()
    }
  }

  /**
   * Pushes one payload per partition, ends the map stage, and passes each partition's published
   * locations, expected payload and read payload to `check` while the application is still alive.
   */
  private def runShuffle(
      app: String,
      numPartitions: Int,
      extraConf: Map[String, String] = Map.empty)(
      check: (LifecycleManager, Seq[(Set[PartitionLocation], String, String)]) => Unit): Unit = {
    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
    (storageConf ++ extraConf).foreach { case (k, v) => clientConf.set(k, v) }
    val lifecycleManager = new LifecycleManager(app, clientConf)
    val shuffleClient = new ShuffleClientImpl(app, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesRead: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }
    try {
      val payloads = (0 until numPartitions).map(p => s"partition-$p-" * 1024)
      payloads.zipWithIndex.foreach { case (payload, partitionId) =>
        val bytes = payload.getBytes(StandardCharsets.UTF_8)
        shuffleClient.pushData(1, 0, 0, partitionId, bytes, 0, bytes.length, 1, numPartitions)
      }
      assert(shuffleFiles(app, 1).nonEmpty, "replicas should be written before commit")
      shuffleClient.mapperEnd(1, 0, 0, 1, numPartitions)
      check(
        lifecycleManager,
        payloads.zipWithIndex.map { case (payload, partitionId) =>
          val group = shuffleClient.updateFileGroup(1, partitionId).partitionGroups.get(partitionId)
          val in =
            shuffleClient.readPartition(1, partitionId, 0, 0, 0, Int.MaxValue, metricsCallback)
          val out = new ByteArrayOutputStream()
          try IOUtils.copy(in, out)
          finally in.close()
          val locations = Option(group).map(_.asScala.toSet).getOrElse(Set.empty)
          (locations, payload, new String(out.toByteArray, StandardCharsets.UTF_8))
        })
    } finally {
      shuffleClient.shutdown()
      lifecycleManager.stop()
    }
  }

  test("HDFS replica is left out of the file group and deleted once both copies commit") {
    val app = "app-hdfs-replica-release"
    runShuffle(app, numPartitions = 2) { (_, results) =>
      results.foreach { case (locations, expected, read) =>
        assert(locations.size === 1)
        val location = locations.head
        assert(location.getMode == PartitionLocation.Mode.PRIMARY && location.getPeer == null)
        assert(location.getStorageInfo.getType == StorageInfo.Type.HDFS)
        assert(read === expected)
      }
      eventually(timeout(30.seconds), interval(500.millis)) {
        assert(shuffleFiles(app, 1).isEmpty)
      }
      assert(shuffleFiles(app, 0).count(!_.contains(".")) === 2)
    }
  }

  test("a primary whose commit times out leaves its partition readable from the replica") {
    val app = SlowPrimaryFileSystem.App
    // The timed-out worker never answers the first CommitFiles; the retry gets its cached failure.
    val retryQuickly = Map(
      CelebornConf.CLIENT_RPC_COMMIT_FILES_ASK_TIMEOUT.key -> "5s",
      CelebornConf.CLIENT_COMMIT_FILE_REQUEST_RETRY_WAIT.key -> "0s")
    runShuffle(app, numPartitions = 1, retryQuickly) { (lifecycleManager, results) =>
      val commitFailures =
        lifecycleManager.workerStatusTracker.excludedWorkers.values().asScala.map(_._1)
      assert(commitFailures.toSet === Set(StatusCode.COMMIT_FILE_EXCEPTION))
      val (locations, expected, read) = results.head
      assert(locations.map(_.getMode) === Set(PartitionLocation.Mode.REPLICA))
      assert(read === expected)
      assert(shuffleFiles(app, 1).count(!_.contains(".")) === 1)
    }
  }
}
