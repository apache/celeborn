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

import java.io.ByteArrayInputStream
import java.nio.channels.FileChannel

import io.netty.buffer.{ByteBufUtil, CompositeByteBuf}
import org.apache.hadoop.fs.Path

import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler

abstract private[worker] class FlushTask(
    val buffer: CompositeByteBuf,
    val notifier: FlushNotifier,
    val keepBuffer: Boolean) {
  def flush(): Unit
}

private[worker] class LocalFlushTask(
    buffer: CompositeByteBuf,
    fileChannel: FileChannel,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    gatherApiEnabled: Boolean) extends FlushTask(buffer, notifier, keepBuffer) {
  override def flush(): Unit = {
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
    // TODO: force flush file channel in scenarios where the upstream task writes and the downstream task reads simultaneously, such as flink hybrid shuffle.
  }
}

private[worker] class HdfsFlushTask(
    buffer: CompositeByteBuf,
    val path: Path,
    notifier: FlushNotifier,
    keepBuffer: Boolean) extends FlushTask(buffer, notifier, keepBuffer) {
  override def flush(): Unit = {
    val hadoopFs = StorageManager.hadoopFs.get(Type.HDFS)
    val hdfsStream = hadoopFs.append(path, 256 * 1024)
    hdfsStream.write(ByteBufUtil.getBytes(buffer))
    hdfsStream.close()
  }
}

private[worker] class S3FlushTask(
    buffer: CompositeByteBuf,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    s3MultipartUploader: MultipartUploadHandler,
    partNumber: Int,
    finalFlush: Boolean = false)
  extends FlushTask(buffer, notifier, keepBuffer) {

  override def flush(): Unit = {
    val bytes = ByteBufUtil.getBytes(buffer)
    val inputStream = new ByteArrayInputStream(bytes)
    s3MultipartUploader.putPart(inputStream, partNumber, finalFlush)
  }
}

private[worker] class OssFlushTask(
    buffer: CompositeByteBuf,
    notifier: FlushNotifier,
    keepBuffer: Boolean,
    ossMultipartUploader: MultipartUploadHandler,
    partNumber: Int,
    finalFlush: Boolean = false)
  extends FlushTask(buffer, notifier, keepBuffer) {

  override def flush(): Unit = {
    val bytes = ByteBufUtil.getBytes(buffer)
    val inputStream = new ByteArrayInputStream(bytes)
    ossMultipartUploader.putPart(inputStream, partNumber, finalFlush)
  }
}
