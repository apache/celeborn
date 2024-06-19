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
  }
}

private[worker] class HdfsFlushTask(
    buffer: CompositeByteBuf,
    val path: Path,
    notifier: FlushNotifier,
    keepBuffer: Boolean) extends FlushTask(buffer, notifier, keepBuffer) {
  override def flush(): Unit = {
    val hdfsStream = StorageManager.hadoopFs.append(path, 256 * 1024)
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
    if (StorageManager.hadoopFs.exists(path)) {
      val conf = StorageManager.hadoopFs.getConf
      val tempPath = new Path(path.getParent, path.getName + ".tmp")
      val outputStream = StorageManager.hadoopFs.create(tempPath, true, 256 * 1024)
      val inputStream = StorageManager.hadoopFs.open(path)
      try {
        IOUtils.copyBytes(inputStream, outputStream, conf, false)
      } finally {
        inputStream.close()
      }
      outputStream.write(ByteBufUtil.getBytes(buffer))
      outputStream.close()
      StorageManager.hadoopFs.delete(path, false)
      StorageManager.hadoopFs.rename(tempPath, path)
    } else {
      val s3Stream = StorageManager.hadoopFs.create(path, true, 256 * 1024)
      s3Stream.write(ByteBufUtil.getBytes(buffer))
      s3Stream.close()
    }
  }
}
