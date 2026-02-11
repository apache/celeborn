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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.{ByteArrayInputStream, Closeable, IOException}
import java.nio.channels.FileChannel

import io.netty.buffer.{ByteBufUtil, CompositeByteBuf}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler
import org.apache.celeborn.service.deploy.worker.WorkerSource

abstract private[worker] class FlushTask(
    val buffer: CompositeByteBuf,
    val notifier: FlushNotifier,
    val keepBuffer: Boolean,
    val source: AbstractSource) {
  def flush(copyBytes: Array[Byte]): Unit

  def convertBufferToBytes(
      buffer: CompositeByteBuf,
      copyBytes: Array[Byte],
      length: Int): Array[Byte] = {
    if (copyBytes != null && copyBytes.length >= length) {
      buffer.getBytes(buffer.readerIndex(), copyBytes, 0, length)
      copyBytes
    } else {
      ByteBufUtil.getBytes(buffer)
    }
  }
}

private[worker] class LocalFlushTask(
    buffer: CompositeByteBuf,
    fileChannel: FileChannel,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    source: AbstractSource,
    gatherApiEnabled: Boolean) extends FlushTask(buffer, notifier, keepBuffer, source) {
  override def flush(copyBytes: Array[Byte]): Unit = {
    val readableBytes = buffer.readableBytes()
    val buffers = buffer.nioBuffers()
    if (gatherApiEnabled) {
      val readableBytes = buffer.readableBytes()
      var written = 0L
      do {
        written = fileChannel.write(buffers) + written
      } while (written != readableBytes)
    } else {
      for (buffer <- buffers) {
        while (buffer.hasRemaining) {
          fileChannel.write(buffer)
        }
      }
    }
    source.incCounter(WorkerSource.LOCAL_FLUSH_COUNT)
    source.incCounter(WorkerSource.LOCAL_FLUSH_SIZE, readableBytes)
    // TODO: force flush file channel in scenarios where the upstream task writes and the downstream task reads simultaneously, such as flink hybrid shuffle.
  }
}

abstract private[worker] class DfsFlushTask(
    buffer: CompositeByteBuf,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    source: AbstractSource) extends FlushTask(buffer, notifier, keepBuffer, source) with Logging {
  def flush(stream: Closeable)(block: => Unit): Unit = {
    try {
      block
    } finally {
      try {
        stream.close()
      } catch {
        case e: IOException => logWarning("Close flush dfs stream failed.", e)
      }
    }
  }
}

private[worker] class HdfsFlushTask(
    buffer: CompositeByteBuf,
    hdfsStream: FSDataOutputStream,
    val path: Path,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    source: AbstractSource) extends DfsFlushTask(buffer, notifier, keepBuffer, source) {
  override def flush(copyBytes: Array[Byte]): Unit = {
    val readableBytes = buffer.readableBytes()
    val bytes = convertBufferToBytes(buffer, copyBytes, readableBytes)

    if (hdfsStream != null) {
      // TODO : If the FSDataOutputStream supports concurrent writes, the lock can be removed.
      hdfsStream.synchronized {
        writeAndRecordMetrics(hdfsStream, bytes, readableBytes)
      }
    } else {
      val hadoopFs = StorageManager.hadoopFs.get(Type.HDFS)
      val appendStream = hadoopFs.append(path, 256 * 1024)
      flush(appendStream) {
        writeAndRecordMetrics(appendStream, bytes, readableBytes)
      }
    }
  }

  private def writeAndRecordMetrics(
      hdfsStream: FSDataOutputStream,
      bytes: Array[Byte],
      size: Int): Unit = {
    hdfsStream.write(bytes)
    source.incCounter(WorkerSource.HDFS_FLUSH_COUNT)
    source.incCounter(WorkerSource.HDFS_FLUSH_SIZE, size)
  }
}

private[worker] class S3FlushTask(
    buffer: CompositeByteBuf,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    source: AbstractSource,
    s3MultipartUploader: MultipartUploadHandler,
    partNumber: Int,
    finalFlush: Boolean = false)
  extends DfsFlushTask(buffer, notifier, keepBuffer, source) {

  override def flush(copyBytes: Array[Byte]): Unit = {
    val readableBytes = buffer.readableBytes()
    val bytes = convertBufferToBytes(buffer, copyBytes, readableBytes)
    val inputStream = new ByteArrayInputStream(bytes, 0, readableBytes)
    flush(inputStream) {
      s3MultipartUploader.putPart(inputStream, partNumber, finalFlush)
      source.incCounter(WorkerSource.S3_FLUSH_COUNT)
      source.incCounter(WorkerSource.S3_FLUSH_SIZE, readableBytes)
    }
  }
}

private[worker] class OssFlushTask(
    buffer: CompositeByteBuf,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    source: AbstractSource,
    ossMultipartUploader: MultipartUploadHandler,
    partNumber: Int,
    finalFlush: Boolean = false)
  extends DfsFlushTask(buffer, notifier, keepBuffer, source) {

  override def flush(copyBytes: Array[Byte]): Unit = {
    val readableBytes = buffer.readableBytes()
    val bytes = convertBufferToBytes(buffer, copyBytes, readableBytes)
    val inputStream = new ByteArrayInputStream(bytes, 0, readableBytes)
    flush(inputStream) {
      ossMultipartUploader.putPart(inputStream, partNumber, finalFlush)
      source.incCounter(WorkerSource.OSS_FLUSH_COUNT)
      source.incCounter(WorkerSource.OSS_FLUSH_SIZE, readableBytes)
    }
  }
}
