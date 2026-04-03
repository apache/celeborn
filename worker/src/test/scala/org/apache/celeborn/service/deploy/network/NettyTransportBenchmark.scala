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

package org.apache.celeborn.service.deploy.network

import java.io.{File, RandomAccessFile}
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}

import org.apache.celeborn.benchmark.{Benchmark, BenchmarkBase}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.DiskFileInfo
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.buffer.{FileChunkBuffers, FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.celeborn.common.network.client.{ChunkReceivedCallback, TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.TransportConf
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.worker.{FetchHandler, WorkerSource}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.storage.ChunkStreamManager

/**
 * Benchmark for Celeborn's Netty transport layer.
 *
 * All suites measure performance through the actual Celeborn transport pipeline
 * (TransportServer + TransportClientFactory + TransportContext).
 *
 * Suite overview:
 *   1. File-Backed Shuffle    - ChunkFetch from disk, NIO vs native transport (EPOLL sendfile bypass detection)
 *
 * {{{
 *   To run this benchmark:
 *   1. build/sbt "celeborn-worker/test:runMain <this class>"
 *   2. generate result:
 *      CELEBORN_GENERATE_BENCHMARK_FILES=1 build/sbt "celeborn-worker/test:runMain <this class>"
 *      Results will be written to "benchmarks/NettyTransportBenchmark-results.txt".
 * }}}
 */
object NettyTransportBenchmark extends BenchmarkBase {

  private def createConf(
      ioMode: String,
      serverThreads: Int = 4,
      clientThreads: Int = 4,
      extraConf: Map[String, String] = Map.empty): TransportConf = {
    val celebornConf = new CelebornConf()
      .set("celeborn.shuffle.io.mode", ioMode)
      .set("celeborn.shuffle.io.serverThreads", serverThreads.toString)
      .set("celeborn.shuffle.io.clientThreads", clientThreads.toString)
    celebornConf.loadFromMap(extraConf, silent = false)
    new TransportConf("shuffle", celebornConf)
  }

  /**
   * Creates N independent (non-pooled) connections to the server.
   * Uses createUnmanagedClient to bypass TransportClientFactory's connection pool,
   * which defaults to numConnectionsPerPeer=1 and would return the same client.
   */
  private def createClients(
      factory: TransportClientFactory,
      port: Int,
      count: Int): Seq[TransportClient] = {
    (0 until count).map(_ => factory.createUnmanagedClient("localhost", port))
  }

  // ==================== Benchmark Suites ====================

  /**
   * Suite 1: File-Backed Shuffle Block Fetch via ChunkFetch.
   *
   * Writes temp files to disk, serves them as FileSegmentManagedBuffer via StreamManager,
   * and fetches them using client.fetchChunk(). This exercises the DefaultFileRegion
   * zero-copy sendfile/splice path.
   *
   * Compares NIO vs EPOLL/KQUEUE to verify that native transports (EPOLL/KQUEUE) use sendfile()
   * for file-backed transfers. EPOLL/KQUEUE should be equal to or faster than NIO.
   */
  private def fileBackedShuffleBenchmark(): Unit = {
    val numFiles = 100
    val fileSize = 16 * 1024 * 1024 // 16 MB per file

    // Create temp shuffle files
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), "celeborn-bench-shuffle")
    tmpDir.mkdirs()
    val files = (0 until numFiles).map { idx =>
      val f = new File(tmpDir, s"shuffle-$idx.data")
      f.deleteOnExit()
      val raf = new RandomAccessFile(f, "rw")
      try {
        val chunk = new Array[Byte](1024 * 1024)
        var written = 0
        while (written < fileSize) {
          val toWrite = math.min(chunk.length, fileSize - written)
          raf.write(chunk, 0, toWrite)
          written += toWrite
        }
      } finally {
        raf.close()
      }
      f
    }

    try {
      runBenchmark(s"File-Backed Shuffle Block Fetch (NIO vs EPOLL/KQUEUE, ${numFiles}x16MB)") {
        val benchmark = new Benchmark(
          "File-Backed Shuffle Fetch",
          numFiles.toLong,
          minNumIters = 3,
          output = output)

        Seq("NIO", detectIOMode()).foreach { mode =>
          benchmark.addTimerCase(s"$mode, sequential fetch", numIters = 3) { timer =>
            val conf = createConf(mode)
            val streamId = 0
            val streamManager = createFileStreamManager(conf, streamId, files)
            val rpcHandler = createStreamRpcHandler(conf, streamManager)
            val context = new TransportContext(conf, rpcHandler)
            val server = context.createServer()
            val clientFactory = context.createClientFactory()
            try {
              val client = clientFactory.createUnmanagedClient("localhost", server.getPort)
              fetchChunksSync(conf, client, streamId, Seq(0))

              timer.startTiming()
              fetchChunksSync(conf, client, streamId, 0 until numFiles)
              timer.stopTiming()
              client.close()
            } finally {
              clientFactory.close()
              server.close()
              context.close()
            }
          }

          benchmark.addTimerCase(s"$mode, parallel fetch (4 clients)", numIters = 3) { timer =>
            val conf = createConf(mode)
            val streamId = 0
            val streamManager = createFileStreamManager(conf, streamId, files)
            val rpcHandler = createStreamRpcHandler(conf, streamManager)
            val context = new TransportContext(conf, rpcHandler)
            val server = context.createServer()
            val clientFactory = context.createClientFactory()
            try {
              val port = server.getPort
              val numThreads = 4
              val chunksPerThread = numFiles / numThreads
              val clients = createClients(clientFactory, port, numThreads)
              clients.foreach(c =>
                fetchChunksSync(conf, c, streamId, Seq(0)))

              timer.startTiming()
              val latch = new CountDownLatch(numThreads)
              (0 until numThreads).foreach { threadIdx =>
                val client = clients(threadIdx)
                val startChunk = threadIdx * chunksPerThread
                val endChunk = startChunk + chunksPerThread
                val t = new Thread(new Runnable {
                  override def run(): Unit = {
                    try {
                      fetchChunksSync(
                        conf,
                        client,
                        0,
                        startChunk until endChunk)
                    } finally {
                      latch.countDown()
                    }
                  }
                })
                t.setDaemon(true)
                t.start()
              }
              latch.await(120, TimeUnit.SECONDS)
              timer.stopTiming()
              clients.foreach(_.close())
            } finally {
              clientFactory.close()
              server.close()
              context.close()
            }
          }
        }

        benchmark.run()
      }
    } finally {
      files.foreach(_.delete())
      tmpDir.delete()
    }
  }

  private def createFileStreamManager(
      conf: TransportConf,
      streamId: Long,
      files: Seq[File]): ChunkStreamManager = {
    val streamManager = new ChunkStreamManager() {
      override def getChunk(
          streamId: Long,
          chunkIndex: Int,
          offset: Int,
          len: Int): ManagedBuffer = {
        new FileSegmentManagedBuffer(conf, files(chunkIndex), 0, files(chunkIndex).length())
      }
    }
    val shuffleFile = files.head
    streamManager.registerStream(
      streamId,
      "bench-0",
      new FileChunkBuffers(
        new DiskFileInfo(
          shuffleFile,
          new UserIdentifier("bench-tenantId", "bench-name"),
          conf.getCelebornConf),
        conf),
      shuffleFile.getName,
      null)
    streamManager
  }

  private def createStreamRpcHandler(
      conf: TransportConf,
      streamManager: ChunkStreamManager): BaseMessageHandler = {
    val celebornConf = conf.getCelebornConf
    MemoryManager.initialize(celebornConf)
    new FetchHandler(
      celebornConf,
      Utils.fromCelebornConf(
        celebornConf,
        TransportModuleConstants.FETCH_MODULE),
      new WorkerSource(celebornConf)) {
      override def getChunkStreamManager: ChunkStreamManager = streamManager

      override def checkRegistered: Boolean = true
    }
  }

  // ==================== Helpers ====================

  /** Automatically selects EPOLL/KQUEUE, which safely falls back to NIO if native .so is unavailable. */
  private def detectIOMode(): String = CelebornConf.networkIoMode()

  /**
   * Fetches chunks synchronously via ChunkFetchRequest and waits for all to complete.
   * The buffer is NOT retained or released here -- TransportResponseHandler releases
   * it after the callback returns.
   */
  private def fetchChunksSync(
      conf: TransportConf,
      client: TransportClient,
      streamId: Long,
      chunkIndices: Seq[Int]): Unit = {
    val sem = new Semaphore(0)
    @volatile var error: Throwable = null
    val callback = new ChunkReceivedCallback {
      override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
        sem.release()
      }
      override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
        error = e
        sem.release()
      }
    }
    chunkIndices.foreach { idx =>
      client.fetchChunk(streamId, idx, conf.getCelebornConf.clientFetchTimeoutMs, callback)
    }
    if (!sem.tryAcquire(chunkIndices.size, 120, TimeUnit.SECONDS)) {
      throw new RuntimeException("ChunkFetch timed out")
    }
    if (error != null) {
      throw new RuntimeException("ChunkFetch failed", error)
    }
  }

  // ==================== Main ====================

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    fileBackedShuffleBenchmark()
  }
}
