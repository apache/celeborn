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

import scala.collection.mutable

import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * End-to-end read/write tests with chunk-level compression enabled
 * (celeborn.chunk.compression.enabled = true).
 *
 * Each test runs against a live mini-cluster, pushes several batches of data,
 * commits the shuffle, then reads back and verifies byte-for-byte correctness.
 * Scenarios cover:
 *   - Different batch-level codecs (NONE, LZ4, ZSTD) layered under chunk ZSTD
 *   - Small chunk size to exercise multi-chunk boundary handling
 *   - Local-read path (LocalPartitionReader) with chunk compression
 */
class ChunkCompressedReadWriteTest extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("ChunkCompressedReadWriteTest: starting mini-cluster")
    val (m, _) = setupMiniClusterWithRandomPorts()
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("ChunkCompressedReadWriteTest: stopping mini-cluster")
    shutdownMiniCluster()
  }

  // ── Core helper ─────────────────────────────────────────────────────────────

  /**
   * Pushes four variable-length data blobs to the cluster (two via pushData,
   * two via mergeData), commits, then reads back all bytes from partition 0 of
   * shuffle 1 and asserts that both the total length and per-blob content match.
   *
   * @param codec          batch-level compression codec (may be NONE)
   * @param readLocal      whether to use the local-read short-circuit path
   * @param shuffleChunkSz chunk size for the chunk-compressed writer (e.g. "8k", "1m")
   */
  private def doReadWriteWithChunkCompression(
      codec: CompressionCodec,
      readLocal: Boolean = false,
      shuffleChunkSz: String = "8m"): Unit = {

    val APP = s"app-chunk-${codec.name}-local$readLocal"

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      // Enable chunk-level ZSTD compression on the worker writer side and
      // the ZSTD decompression in CelebornInputStream on the reader side.
      .set(CelebornConf.CHUNK_COMPRESSION_ENABLED.key, "true")
      // Batch-level codec is independent — NONE means raw batches inside the
      // ZSTD chunk; LZ4/ZSTD means batch-compressed payloads inside the chunk.
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, codec.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set(CelebornConf.READ_LOCAL_SHUFFLE_FILE, readLocal)
      // Controls the accumulation buffer in ChunkCompressedFileChannelWriter.
      .set(CelebornConf.SHUFFLE_CHUNK_SIZE.key, shuffleChunkSz)
      .set("celeborn.data.io.numConnectionsPerPeer", "1")

    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    try {
      // ── Write phase ──────────────────────────────────────────────────────
      // Each string is prefixed with a 6-char sentinel so we can identify
      // and verify individual blobs in the combined read output.
      val dataPrefix = Array("000000", "111111", "222222", "333333")
      val dataPrefixMap = new mutable.HashMap[String, String]

      val STR1 = dataPrefix(0) + RandomStringUtils.random(1024)
      dataPrefixMap.put(dataPrefix(0), STR1)
      val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
      val dataSize1 = shuffleClient.pushData(1, 0, 0, 0, DATA1, 0, DATA1.length, 1, 1)
      logInfo(s"pushData #1 size=$dataSize1")

      val STR2 = dataPrefix(1) + RandomStringUtils.random(32 * 1024)
      dataPrefixMap.put(dataPrefix(1), STR2)
      val DATA2 = STR2.getBytes(StandardCharsets.UTF_8)
      val dataSize2 = shuffleClient.pushData(1, 0, 0, 0, DATA2, 0, DATA2.length, 1, 1)
      logInfo(s"pushData #2 size=$dataSize2")

      val STR3 = dataPrefix(2) + RandomStringUtils.random(32 * 1024)
      dataPrefixMap.put(dataPrefix(2), STR3)
      val DATA3 = STR3.getBytes(StandardCharsets.UTF_8)
      shuffleClient.mergeData(1, 0, 0, 0, DATA3, 0, DATA3.length, 1, 1)

      val STR4 = dataPrefix(3) + RandomStringUtils.random(16 * 1024)
      dataPrefixMap.put(dataPrefix(3), STR4)
      val DATA4 = STR4.getBytes(StandardCharsets.UTF_8)
      shuffleClient.mergeData(1, 0, 0, 0, DATA4, 0, DATA4.length, 1, 1)

      shuffleClient.pushMergedData(1, 0, 0)
      Thread.sleep(1000)
      shuffleClient.mapperEnd(1, 0, 0, 1, 1)

      // ── Read phase ──────────────────────────────────────────────────────
      val metricsCallback = new MetricsCallback {
        override def incBytesRead(bytesWritten: Long): Unit = {}
        override def incReadTime(time: Long): Unit = {}
      }

      val inputStream = shuffleClient.readPartition(
        1,
        1,
        0,
        0,
        0,
        0,
        Integer.MAX_VALUE,
        null,
        null,
        null,
        null,
        null,
        null,
        metricsCallback,
        true)

      val outputStream = new ByteArrayOutputStream()
      var b = inputStream.read()
      while (b != -1) {
        outputStream.write(b)
        b = inputStream.read()
      }

      val readBytes = outputStream.toByteArray
      val expectedTotal = DATA1.length + DATA2.length + DATA3.length + DATA4.length

      // ── Assertions ───────────────────────────────────────────────────────
      Assert.assertEquals(
        s"Total byte count mismatch (codec=$codec, readLocal=$readLocal, chunkSz=$shuffleChunkSz)",
        expectedTotal,
        readBytes.length)

      val readStringMap = extractBlobs(readBytes, dataPrefix, dataPrefixMap)
      for ((prefix, actual) <- readStringMap) {
        Assert.assertEquals(
          s"Content mismatch for blob '$prefix'",
          dataPrefixMap(prefix),
          actual)
      }

    } finally {
      Thread.sleep(3000L)
      shuffleClient.shutdown()
      lifecycleManager.rpcEnv.shutdown()
    }
  }

  /**
   * Rebuilds the per-blob strings from the flat read output by scanning for
   * known 6-char prefixes and extracting the expected number of characters.
   */
  private def extractBlobs(
      readBytes: Array[Byte],
      prefixes: Array[String],
      prefixMap: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    var remaining = new String(readBytes, StandardCharsets.UTF_8)
    val result = new mutable.HashMap[String, String]
    while (remaining.nonEmpty) {
      prefixes.find(remaining.startsWith) match {
        case Some(prefix) =>
          val len = prefixMap(prefix).length
          result.put(prefix, remaining.substring(0, len))
          remaining = remaining.substring(len)
        case None =>
          remaining = ""
      }
    }
    result
  }

  /**
   * Pushes data in three phases with a 2 KB chunk size:
   *   Phase 1 — 3 small batches (500 B each; 516 B on disk with header → 1548 B total)
   *             These accumulate in the chunk buffer (< 2048 B) without flushing.
   *   Phase 2 — 1 large batch (3000 B; 3016 B on disk > 2048 B chunk size)
   *             Arrival flushes phase-1 data as chunk 1 via compressAndFlush(), then
   *             writes the large batch as its own chunk 2 via flushLargeRecord().
   *   Phase 3 — 3 more small batches (same size; 1548 B total)
   *             Accumulate and are flushed as chunk 3 on close().
   *
   * This exercises:
   *  - Multiple batches compressed together in a single ZSTD chunk (chunks 1 and 3),
   *    which requires ZstdInputStream to be kept alive across fillBuffer() calls.
   *  - The large-record path where one batch is larger than the chunk size.
   */
  private def doSmallLargeSmallReadWrite(): Unit = {
    val APP = "app-chunk-small-large-small"

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CHUNK_COMPRESSION_ENABLED.key, "true")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      // 2 KB chunk size: small batches (516 B each) accumulate; large batch (3016 B) overflows.
      .set(CelebornConf.SHUFFLE_CHUNK_SIZE.key, "2k")
      .set("celeborn.data.io.numConnectionsPerPeer", "1")

    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    try {
      // 6-char alphanumeric prefixes — unique, non-overlapping.
      // RandomStringUtils.random(N, true, true) → exactly N ASCII bytes.
      val dataPrefix = Array("SMLL1-", "SMLL2-", "SMLL3-", "LARGE-", "SMLL4-", "SMLL5-", "SMLL6-")
      val dataPrefixMap = new mutable.HashMap[String, String]

      // Phase 1: three small batches (500 B each → 516 B on disk with 16-B header).
      // Combined 1548 B < 2048 B chunk size — all sit in the chunk buffer together.
      val STR1 = dataPrefix(0) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(0), STR1)
      val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA1, 0, DATA1.length, 1, 1)

      val STR2 = dataPrefix(1) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(1), STR2)
      val DATA2 = STR2.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA2, 0, DATA2.length, 1, 1)

      val STR3 = dataPrefix(2) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(2), STR3)
      val DATA3 = STR3.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA3, 0, DATA3.length, 1, 1)

      // Phase 2: one large batch (3000 B → 3016 B on disk > 2048 B chunk size).
      // Triggers compressAndFlush() of the phase-1 smalls as chunk 1,
      // then flushLargeRecord() writes this batch alone as chunk 2.
      val STR4 = dataPrefix(3) + RandomStringUtils.random(2994, true, true)
      dataPrefixMap.put(dataPrefix(3), STR4)
      val DATA4 = STR4.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA4, 0, DATA4.length, 1, 1)

      // Phase 3: three more small batches that accumulate as chunk 3 and are flushed on close().
      val STR5 = dataPrefix(4) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(4), STR5)
      val DATA5 = STR5.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA5, 0, DATA5.length, 1, 1)

      val STR6 = dataPrefix(5) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(5), STR6)
      val DATA6 = STR6.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA6, 0, DATA6.length, 1, 1)

      val STR7 = dataPrefix(6) + RandomStringUtils.random(494, true, true)
      dataPrefixMap.put(dataPrefix(6), STR7)
      val DATA7 = STR7.getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, DATA7, 0, DATA7.length, 1, 1)

      Thread.sleep(1000)
      shuffleClient.mapperEnd(1, 0, 0, 1, 1)

      val metricsCallback = new MetricsCallback {
        override def incBytesRead(bytesWritten: Long): Unit = {}
        override def incReadTime(time: Long): Unit = {}
      }

      val inputStream = shuffleClient.readPartition(
        1,
        1,
        0,
        0,
        0,
        0,
        Integer.MAX_VALUE,
        null,
        null,
        null,
        null,
        null,
        null,
        metricsCallback,
        true)

      val outputStream = new ByteArrayOutputStream()
      var b = inputStream.read()
      while (b != -1) {
        outputStream.write(b)
        b = inputStream.read()
      }

      val readBytes = outputStream.toByteArray
      val expectedTotal =
        DATA1.length + DATA2.length + DATA3.length + DATA4.length +
          DATA5.length + DATA6.length + DATA7.length

      Assert.assertEquals(
        "Total byte count mismatch (small-large-small interleave)",
        expectedTotal,
        readBytes.length)

      val readStringMap = extractBlobs(readBytes, dataPrefix, dataPrefixMap)
      for ((prefix, actual) <- readStringMap) {
        Assert.assertEquals(
          s"Content mismatch for blob '$prefix'",
          dataPrefixMap(prefix),
          actual)
      }

    } finally {
      Thread.sleep(3000L)
      shuffleClient.shutdown()
      lifecycleManager.rpcEnv.shutdown()
    }
  }

  // ── Test cases ───────────────────────────────────────────────────────────────

  // 1. Pure chunk ZSTD — no batch-level compression.
  //    Simplest configuration: the chunk writer compresses raw batches.
  test("chunk compression with NONE batch codec") {
    doReadWriteWithChunkCompression(CompressionCodec.NONE)
  }

  // 2. Chunk ZSTD wrapping LZ4-compressed batches.
  //    Verifies that CelebornInputStream correctly decompresses the chunk first
  //    then hands each batch to the LZ4 Decompressor.
  test("chunk compression with LZ4 batch codec") {
    doReadWriteWithChunkCompression(CompressionCodec.LZ4)
  }

  // 3. Chunk ZSTD wrapping ZSTD-compressed batches (two layers of ZSTD).
  //    Both chunkCompressed and shouldDecompress paths are active simultaneously.
  test("chunk compression with ZSTD batch codec") {
    doReadWriteWithChunkCompression(CompressionCodec.ZSTD)
  }

  // 4. Small chunk size (8 KB) forces many chunk flushes across the data set,
  //    exercising the multi-chunk offset tracking and boundary handling.
  test("chunk compression with small chunk size produces multiple chunks") {
    doReadWriteWithChunkCompression(CompressionCodec.NONE, shuffleChunkSz = "8k")
  }

  // 5. Same small-chunk scenario with LZ4 batches.
  test("chunk compression + LZ4 batch codec with small chunk size") {
    doReadWriteWithChunkCompression(CompressionCodec.LZ4, shuffleChunkSz = "8k")
  }

  // 6. Local-read path (LocalPartitionReader) with chunk compression.
  //    Verifies that the chunk-compressed file is correctly decompressed when
  //    read directly from disk rather than through the network fetch path.
  test("chunk compression with local shuffle read") {
    doReadWriteWithChunkCompression(CompressionCodec.NONE, readLocal = true)
  }

  // 7. Local read + LZ4 batch codec.
  test("chunk compression with local shuffle read and LZ4 batch codec") {
    doReadWriteWithChunkCompression(CompressionCodec.LZ4, readLocal = true)
  }

  // 8. Small batches → large record → more small batches.
  //    Validates that ZstdInputStream is kept alive across multiple fillBuffer() calls
  //    within a single chunk (chunks 1 and 3 each hold 3 batches), and that the
  //    large-record ZSTD frame in chunk 2 round-trips without corruption.
  test("chunk compression: multiple small batches, one large record, then more small batches") {
    doSmallLargeSmallReadWrite()
  }
}
