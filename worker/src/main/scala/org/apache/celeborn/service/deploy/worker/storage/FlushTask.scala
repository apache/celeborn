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

import java.nio.channels.FileChannel

import io.netty.buffer.{ByteBufUtil, CompositeByteBuf}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

import org.apache.celeborn.common.protocol.StorageInfo.Type

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
    keepBuffer: Boolean) extends FlushTask(buffer, notifier, keepBuffer) {
  override def flush(): Unit = {
    val buffers = buffer.nioBuffers()
    for (buffer <- buffers) {
      while (buffer.hasRemaining) {
        fileChannel.write(buffer)
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
    val path: Path,
    notifier: FlushNotifier,
    keepBuffer: Boolean) extends FlushTask(buffer, notifier, keepBuffer) {
  override def flush(): Unit = {
    val hadoopFs = StorageManager.hadoopFs.get(Type.S3)
    if (hadoopFs.exists(path)) {
      val conf = hadoopFs.getConf
      val tempPath = new Path(path.getParent, path.getName + ".tmp")
      val outputStream = hadoopFs.create(tempPath, true, 256 * 1024)
      val inputStream = hadoopFs.open(path)
      try {
        IOUtils.copyBytes(inputStream, outputStream, conf, false)
      } finally {
        inputStream.close()
      }
      outputStream.write(ByteBufUtil.getBytes(buffer))
      outputStream.close()
      hadoopFs.delete(path, false)
      hadoopFs.rename(tempPath, path)
    } else {
      val s3Stream = hadoopFs.create(path, true, 256 * 1024)
      s3Stream.write(ByteBufUtil.getBytes(buffer))
      s3Stream.close()
    }
  }
}
