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

package org.apache.celeborn.client.compress

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.benchmark.Benchmark
import org.apache.celeborn.common.CelebornConf

/**
 * Benchmark for LZ4 codec performance.
 * To run this benchmark
 * {{{
 *   1. build/sbt "celeborn-client/test:runMain <this class>"
 *   2. generate result:
 *      CELEBORN_GENERATE_BENCHMARK_FILES=1 build/sbt "celeborn-client/test:runMain <this class>"
 *      Results will be written to "benchmarks/LZ4TPCDSDataBenchmark-results.txt".
 * }}}
 */
object LZ4TPCDSDataBenchmark extends TPCDSDataBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    prepareData()
    runBenchmark("Benchmark LZ4CompressionCodec") {
      Seq(
        64 * 1024 /* 64k */,
        256 * 1024 /* 256k */,
        1024 * 1024 /* 1m */,
        4 * 1024 * 1024 /* 4m */ ).foreach { chunkSize: Int =>
        compressionBenchmark(chunkSize)
        decompressionBenchmark(chunkSize)
      }
    }
  }

  private def compressionBenchmark(chunkSize: Int): Unit = {
    val benchmark = new Benchmark("Compression", N, output = output)
    val blockSize = new CelebornConf().clientPushBufferMaxSize
    val lz4Compressor = new Lz4Compressor(blockSize)
    benchmark.addCase(s"Compression with chunk size $chunkSize $N times") { _ =>
      (1 until N).foreach { _ =>
        var offset = 0
        while (offset < data.length) {
          val length = math.min(chunkSize, data.length - offset)
          lz4Compressor.compress(data, offset, length)
          offset += length
        }
      }
    }
    benchmark.run()
  }

  private def decompressionBenchmark(chunkSize: Int): Unit = {
    val benchmark = new Benchmark("Decompression", N, output = output)
    val blockSize = new CelebornConf().clientPushBufferMaxSize
    val lz4Compressor = new Lz4Compressor(blockSize)
    val compressedDataChunks = ArrayBuffer.empty[Array[Byte]]
    var offset = 0
    while (offset < data.length) {
      val length = math.min(chunkSize, data.length - offset)
      lz4Compressor.compress(data, offset, length)
      val compressed = lz4Compressor.getCompressedBuffer
      compressedDataChunks += util.Arrays.copyOf(compressed, compressed.length)
      offset += length
    }

    val lz4Decompressor = new Lz4Decompressor(Option.empty)
    val dst = new Array[Byte](chunkSize)
    benchmark.addCase(s"Decompression with chunk size $chunkSize $N times") { _ =>
      (1 until N).foreach { _ =>
        compressedDataChunks.foreach { compressedChunk =>
          lz4Decompressor.decompress(compressedChunk, dst, 0)
        }
      }
    }
    benchmark.run()
  }
}
